from fastapi import FastAPI, File, UploadFile, HTTPException, Request, BackgroundTasks, Query
from fastapi.responses import PlainTextResponse, JSONResponse, HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pathlib import Path
from typing import List
import httpx
import os
import json
import uuid
import sys
from datetime import datetime

TIKA_URL = "http://localhost:9998/"
DATA_DIR = Path(os.getenv("DATA_DIR", "data"))
DATA_DIR.mkdir(parents=True, exist_ok=True)

INDEX_JOBS_DIR = DATA_DIR / "index_jobs"
INDEX_JOBS_DIR.mkdir(parents=True, exist_ok=True)

app = FastAPI(title="Bare-bones SARs Sifter", version="0.1.0")
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")


def _write_json(path: Path, obj: dict):
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(obj, indent=2), encoding="utf-8")
    tmp.replace(path)


def run_index_job(job_id: str):
    job_path = INDEX_JOBS_DIR / f"{job_id}.json"
    runtime = {
        "python_executable": sys.executable,
        "venv": "venv",
        "sys_path_0": sys.path[0],
    }

    # If you want to test nameparser *specifically*:
    try:
        import nameparser
        runtime["nameparser"] = nameparser.__file__
    except Exception as e:
        runtime["nameparser_error"] = str(e)

    try:
        # Load what we have so far
        if job_path.exists():
            job = json.loads(job_path.read_text(encoding="utf-8"))
        else:
            job = {"status": "running", "started": datetime.utcnow().isoformat() + "Z"}
        job["status"] = "running"
        job["started"] = job.get("started") or (datetime.utcnow().isoformat() + "Z")
        job["progress"] = {"processed": 0, "total": 0}
        _write_json(job_path, job)

        # Gather .mbox files from sidecars
        items = []
        for meta_path in DATA_DIR.glob("*.meta.json"):
            try:
                meta = json.loads(meta_path.read_text(encoding="utf-8"))
            except Exception:
                continue
            stored = meta.get("stored_name") or ""
            if not stored.lower().endswith(".mbox"):
                continue
            items.append({
                "id": meta.get("id") or meta_path.stem,
                "original_name": meta.get("original_name"),
                "stored_name": stored,
            })

        total = len(items)
        job["progress"] = {"processed": 0, "total": total}
        _write_json(job_path, job)

        if total == 0:
            job["status"] = "error"
            job["error"] = "No .mbox files found to index."
            _write_json(job_path, job)
            return

        # Run the real indexing pipeline (non-interactive)
        from index_emails import run_index_to_dir
        out_dir = INDEX_JOBS_DIR / job_id
        summary = run_index_to_dir([DATA_DIR / it["stored_name"] for it in items], out_dir)

        # Mark as fully processed (coarse-grained)
        job["progress"]["processed"] = total
        job["summary"] = summary
        _write_json(job_path, job)

        job["status"] = "done"
        job["completed"] = datetime.utcnow().isoformat() + "Z"
        _write_json(job_path, job)
    except Exception as e:
        job = {
            "status": "error",
            "error": str(e),
            "completed": datetime.utcnow().isoformat() + "Z"
        }
        _write_json(job_path, job)


@app.post("/index/start", response_class=JSONResponse)
async def index_start(background_tasks: BackgroundTasks):
    job_id = str(uuid.uuid4())
    job_path = INDEX_JOBS_DIR / f"{job_id}.json"
    _write_json(job_path, {"status": "running", "started": datetime.utcnow().isoformat() + "Z", "progress": {"processed": 0, "total": 0}})
    background_tasks.add_task(run_index_job, job_id)
    return {"job_id": job_id}


@app.get("/index/status", response_class=JSONResponse)
async def index_status(job_id: str = Query(...)):
    job_path = INDEX_JOBS_DIR / f"{job_id}.json"
    if not job_path.exists():
        raise HTTPException(status_code=404, detail="Unknown job_id")
    try:
        data = json.loads(job_path.read_text(encoding="utf-8"))
    except Exception:
        raise HTTPException(status_code=500, detail="Could not read job status")
    return data


@app.get("/index/result", response_class=JSONResponse)
async def index_result(job_id: str = Query(...)):
    out_dir = INDEX_JOBS_DIR / job_id
    clusters = out_dir / "clusters.json"
    if not clusters.exists():
        raise HTTPException(status_code=404, detail="No results for this job yet")
    try:
        data = json.loads(clusters.read_text(encoding="utf-8"))
    except Exception:
        raise HTTPException(status_code=500, detail="Could not read results")
    return {"clusters": data}


@app.get("/index/cluster", response_class=JSONResponse)
async def index_cluster(job_id: str = Query(...), cluster_id: str = Query(...)):
    out_dir = INDEX_JOBS_DIR / job_id
    idx_path = out_dir / "cluster_index.json"
    parts_path = out_dir / "parts.json"
    if not idx_path.exists():
        raise HTTPException(status_code=404, detail="No cluster index for this job")
    try:
        idx = json.loads(idx_path.read_text(encoding="utf-8"))
    except Exception:
        raise HTTPException(status_code=500, detail="Could not read cluster index")

    rec = idx.get(cluster_id)
    if rec is None:
        raise HTTPException(status_code=404, detail="Unknown cluster_id")

    postings = rec.get("postings") or []  # [{part_id, role}]

    # Load parts to render email fields
    parts = {}
    if parts_path.exists():
        try:
            parts = json.loads(parts_path.read_text(encoding="utf-8"))
        except Exception:
            parts = {}

    # Deduplicate part_ids preserving order, then normalize
    seen = set()
    ordered = []
    for p in postings:
        pid = p.get("part_id")
        if not pid or pid in seen:
            continue
        seen.add(pid)
        ordered.append(pid)

    def pick(d, *keys):
        for k in keys:
            if k in d and d[k] is not None:
                return d[k]
        return None

    norm = []
    for pid in ordered:
        doc = parts.get(pid, {})
        item = {
            "from": pick(doc, "From", "from"),
            "to": pick(doc, "To", "to"),
            "subject": pick(doc, "Subject", "subject") or "(no subject)",
            "date": pick(doc, "Date", "date"),
            "body": pick(doc, "Body", "body", "text"),
        }
        norm.append(item)

    # Oldest -> newest
    norm.sort(key=lambda x: x.get("date") or "")

    return {"label": rec.get("label") or cluster_id, "postings": norm}


@app.get("/health")
async def health():
    info = {}
    # Basic Python/runtime info
    try:
        import sys
        info["python_executable"] = sys.executable
        info["python_version"] = sys.version
    except Exception as e:
        info["python_info_error"] = str(e)

    # spaCy presence/version
    try:
        import spacy  # type: ignore
        info["spacy"] = {"available": True, "version": getattr(spacy, "__version__", "?")}
    except Exception as e:
        info["spacy"] = {"available": False, "error": str(e)}

    # Tika status
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            r = await client.get(TIKA_URL)
        info["tika"] = {"status_code": r.status_code}
    except Exception as e:
        info["tika"] = {"status": "degraded", "error": str(e)}

    return info
    

@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    existing = []
    seen_ids = set()
    try:
        # Primary: sidecar metadata files created by /upload
        for meta_path in DATA_DIR.glob("*.meta.json"):
            try:
                meta = json.loads(meta_path.read_text(encoding="utf-8"))
            except Exception:
                continue
            doc_id = (meta.get("id") or meta_path.stem).strip()
            label = (meta.get("original_name") or meta.get("stored_name") or doc_id).strip()
            existing.append({"id": doc_id, "label": label})
            seen_ids.add(doc_id)

        # Fallback: show any UUID-named stored files without sidecars
        for p in DATA_DIR.iterdir():
            if not p.is_file():
                continue
            if p.name.endswith(".meta.json"):
                continue
            stem = p.stem
            # Skip files that already have a sidecar entry
            if stem in seen_ids:
                continue
            # Only include things that look like our UUID-named stored files
            try:
                uuid.UUID(stem)
            except Exception:
                continue
            existing.append({"id": stem, "label": p.name})

        existing.sort(key=lambda x: x["label"].lower())
    except Exception:
        existing = []

    return templates.TemplateResponse("index.html", {"request": request, "existing": existing})


@app.post("/upload")
async def upload(files: List[UploadFile] = File(...)):
    if not files:
        raise HTTPException(status_code=400, detail="No files provided")

    saved = []
    try:
        for file in files:
            content = await file.read()
            if not content:
                raise HTTPException(status_code=400, detail=f"Empty file: {file.filename}")

            # Generate a UUID-based filename while preserving the original extension
            doc_id = str(uuid.uuid4())
            _, ext = os.path.splitext(file.filename or "")
            stored_name = f"{doc_id}{ext}"
            stored_path = DATA_DIR / stored_name
            stored_path.write_bytes(content)

            # Write a small sidecar with original name and other details
            meta = {
                "id": doc_id,
                "original_name": file.filename,
                "stored_name": stored_name,
                "content_type": file.content_type,
                "size": len(content),
            }
            (DATA_DIR / f"{doc_id}.meta.json").write_text(
                json.dumps(meta, indent=2), encoding="utf-8"
            )
            saved.append(meta)

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    # Go back to index, which now lists cumulative uploads by original name
    return RedirectResponse(url="/", status_code=303)