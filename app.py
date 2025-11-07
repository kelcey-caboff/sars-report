from fastapi import FastAPI, File, UploadFile, HTTPException, Request, BackgroundTasks, Query
from fastapi.responses import JSONResponse, HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.middleware.cors import CORSMiddleware
from pathlib import Path
from typing import List, Optional
import httpx
import os
import json
import uuid
import sys
from datetime import datetime
import re
from email.utils import parsedate_to_datetime
from pydantic import BaseModel, Field

from contextlib import asynccontextmanager
import shutil


TIKA_URL = "http://localhost:9998/"
DATA_DIR = Path(os.getenv("DATA_DIR", "data"))
DATA_DIR.mkdir(parents=True, exist_ok=True)

INDEX_JOBS_DIR = DATA_DIR / "index_jobs"
INDEX_JOBS_DIR.mkdir(parents=True, exist_ok=True)


@asynccontextmanager
async def lifespan(app: FastAPI):
    print("startup")
    # Maybe create the folder if needed
    DATA_DIR.mkdir(exist_ok=True)
    yield
    print("shutdown → cleaning")
    shutil.rmtree(DATA_DIR, ignore_errors=True)


app = FastAPI(title="Bare-bones SARs Sifter", version="0.1.0", lifespan=lifespan)
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Basic upload limits and helpers
MAX_UPLOAD_BYTES = int(os.getenv("MAX_UPLOAD_BYTES", "104857600"))  # 100 MiB default
CHUNK_SIZE = 1024 * 1024  # 1 MiB

_uuid_re = re.compile(r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$")
_ext_re = re.compile(r"\.[a-z0-9]{1,8}$")


def _safe_ext(ext: str) -> str:
    try:
        ext = (ext or "").lower()
        return ext if _ext_re.match(ext) else ""
    except Exception:
        return ""


def _looks_like_uuid(s: str) -> bool:
    return bool(_uuid_re.match(s or ""))


async def _stream_save_upload(file: UploadFile, dest: Path, max_bytes: int = MAX_UPLOAD_BYTES) -> int:
    """Stream an UploadFile to disk in chunks, enforcing a soft size limit.
    Returns the number of bytes written. Raises HTTPException on limit breach.
    """
    total = 0
    with dest.open("wb") as f:
        while True:
            chunk = await file.read(CHUNK_SIZE)  # type: ignore
            if not chunk:
                break
            total += len(chunk)
            if total > max_bytes:
                # Stop early and remove partial file
                try:
                    f.flush()
                finally:
                    dest.unlink(missing_ok=True)
                raise HTTPException(status_code=413, detail="Uploaded file too large")
            f.write(chunk)
    # Reset cursor so future reads (if any) start from beginning
    try:
        await file.seek(0)  # type: ignore
    except Exception:
        pass
    return total


def _write_json(path: Path, obj: dict):
    # Ensure parent directory exists before writing
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.parent.mkdir(parents=True, exist_ok=True)
    tmp.write_text(json.dumps(obj, indent=2), encoding="utf-8")
    tmp.replace(path)


def _job_dir(job_id: str) -> Path:
    return INDEX_JOBS_DIR / job_id


def _load_cluster_artifacts(job_id: str) -> tuple[dict, dict, dict]:
    """
    Return (cluster_index, id_to_cluster, identifier_postings) for a job.
    Falls back to an empty dict for identifier_postings if that file is missing.
    """
    base = _job_dir(job_id)
    ci = json.loads((base / "cluster_index.json").read_text(encoding="utf-8"))
    itc = json.loads((base / "id_to_cluster.json").read_text(encoding="utf-8"))
    ip_path = base / "identifier_postings.json"
    if ip_path.exists():
        try:
            id_posts = json.loads(ip_path.read_text(encoding="utf-8"))
        except Exception:
            id_posts = {}
    else:
        id_posts = {}
    return ci, itc, id_posts


def _save_cluster_artifacts(job_id: str, cluster_index: dict, id_to_cluster: dict):
    base = _job_dir(job_id)
    # Rebuild clusters.json from cluster_index
    clusters = []
    for cid, data in cluster_index.items():
        clusters.append({
            "id": cid,
            "label": data.get("label"),
            "size": len(data.get("members", [])),
            "members": list(data.get("members", [])),
        })
    clusters.sort(key=lambda r: (-r["size"], r.get("label") or ""))
    (base / "clusters.json").write_text(json.dumps(clusters, indent=2), encoding="utf-8")
    (base / "cluster_index.json").write_text(json.dumps(cluster_index, indent=2), encoding="utf-8")
    (base / "id_to_cluster.json").write_text(json.dumps(id_to_cluster, indent=2), encoding="utf-8")


def _recompute_cluster_postings(cluster_index: dict, identifier_postings: dict, cid: str):
    """
    Rebuild postings for a cluster by unioning postings from its members.
    If identifier_postings is missing/empty (older jobs), leave postings unchanged.
    """
    try:
        if not identifier_postings:
            return
        members = cluster_index.get(cid, {}).get("members", []) or []
        merged: list[dict] = []
        seen = set()
        for ident in members:
            for post in identifier_postings.get(ident, []):
                key = (post.get("part_id"), post.get("role"))
                if key in seen:
                    continue
                seen.add(key)
                merged.append({"part_id": post.get("part_id"), "role": post.get("role")})
        cluster_index[cid]["postings"] = merged
    except Exception:
        # On any error, keep existing postings
        pass


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
            stored = (meta.get("stored_name") or "").strip()
            # Ensure the stored name looks like our UUID + extension and lives in DATA_DIR
            try:
                stored_path = DATA_DIR / Path(stored).name
            except Exception:
                continue
            stem = Path(stored).stem
            if not (_looks_like_uuid(stem) and stored_path.suffix.lower() == ".mbox"):
                continue
            if not stored_path.exists():
                continue
            items.append({
                "id": (meta.get("id") or meta_path.stem),
                "original_name": meta.get("original_name"),
                "stored_name": stored_path.name,
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


@app.get("/index/identifiers", response_class=JSONResponse)
async def index_identifiers(job_id: str = Query(...)):
    base = _job_dir(job_id)
    if not (base / "cluster_index.json").exists():
        raise HTTPException(status_code=404, detail="No results for this job yet")
    try:
        cluster_index, id_to_cluster, _ = _load_cluster_artifacts(job_id)
    except Exception:
        raise HTTPException(status_code=500, detail="Could not load cluster artifacts")
    
    rows = []
    label_by_cid = {cid: (data.get("label") or "") for cid, data in cluster_index.items()}
    members_by_cid = {cid: set(data.get("members", [])) for cid, data in cluster_index.items()}

    for ident, cid in id_to_cluster.items():
        is_gold = ident == label_by_cid.get(cid)
        rows.append({
            "identifier": ident,
            "cluster_id": cid,
            "is_gold": is_gold,
        })
    
    clusters = [
        {"id": cid, "label": label_by_cid.get(cid, ""), "size": len(members_by_cid.get(cid, []))}
        for cid in cluster_index.keys()
    ]
    clusters.sort(key=lambda r: (-r["size"], r.get("label") or ""))
    return {"identifiers": rows, "clusters": clusters}



class MoveModel(BaseModel):
    identifier: str
    target_cluster_id: str  # if unknown, a new cluster will be created


class RelabelModel(BaseModel):
    cluster_id: str
    label: str


class CreateModel(BaseModel):
    label: Optional[str] = None
    members: list[str] = Field(default_factory=list)


class ClusterUpdate(BaseModel):
    job_id: str
    moves: list[MoveModel] = Field(default_factory=list)
    relabels: list[RelabelModel] = Field(default_factory=list)
    creates: list[CreateModel] = Field(default_factory=list)

# ======== Finder Models ========
class FinderRule(BaseModel):
    cluster_id: str
    from_: str = Field(default="any", alias="from")
    to: str = Field(default="any")
    body: str = Field(default="any")

class FinderPayload(BaseModel):
    job_id: str
    rules: list[FinderRule] = Field(default_factory=list)


@app.post("/index/clusters/update", response_class=JSONResponse)
async def index_clusters_update(payload: ClusterUpdate):
    job_id = payload.job_id
    base = _job_dir(job_id)
    if not (base / "cluster_index.json").exists():
        raise HTTPException(status_code=404, detail="No results for this job yet")
    try:
        cluster_index, id_to_cluster, identifier_postings = _load_cluster_artifacts(job_id)
    except Exception:
        raise HTTPException(status_code=500, detail="Could not load cluster artifacts")

    import hashlib, time
    def _new_cid(seed: str) -> str:
        return hashlib.sha1(f"{seed}|{time.time()}".encode()).hexdigest()[:12]

    # Ensure members lists are lists
    for cid, data in cluster_index.items():
        if not isinstance(data.get("members"), list):
            data["members"] = list(data.get("members") or [])
        if "postings" not in data:
            data["postings"] = []

    # 1) Create new clusters
    for c in payload.creates:
        if not c.members:
            continue
        cid = _new_cid("|".join(sorted(c.members)))
        cluster_index[cid] = {"label": c.label or c.members[0], "members": list(dict.fromkeys(c.members)), "postings": []}
        for ident in c.members:
            id_to_cluster[ident] = cid
        _recompute_cluster_postings(cluster_index, identifier_postings, cid)

    # 2) Move identifiers
    for m in payload.moves:
        ident = m.identifier
        if ident not in id_to_cluster:
            continue
        dst = m.target_cluster_id
        if dst not in cluster_index:
            dst = _new_cid(ident)
            cluster_index[dst] = {"label": ident, "members": [], "postings": []}
        src = id_to_cluster[ident]
        if src == dst:
            continue
        # remove from src
        try:
            if ident in cluster_index[src]["members"]:
                cluster_index[src]["members"].remove(ident)
        except Exception:
            pass
        # add to dst
        if ident not in cluster_index[dst]["members"]:
            cluster_index[dst]["members"].append(ident)
        id_to_cluster[ident] = dst
        _recompute_cluster_postings(cluster_index, identifier_postings, src)
        _recompute_cluster_postings(cluster_index, identifier_postings, dst)

    # 3) Relabel (“gold name”)
    for r in payload.relabels:
        if r.cluster_id in cluster_index and r.label:
            cluster_index[r.cluster_id]["label"] = r.label

    # Drop empty clusters
    to_delete = [cid for cid, data in cluster_index.items() if not data.get("members")]
    for cid in to_delete:
        cluster_index.pop(cid, None)

    _save_cluster_artifacts(job_id, cluster_index, id_to_cluster)
    return {"ok": True}


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

    # Oldest -> newest using parsed dates when possible
    def _dt_key(rec):
        d = rec.get("date")
        try:
            if not d:
                return 0
            dt = parsedate_to_datetime(d)
            # Convert to POSIX seconds for stable sorting; handle naive vs aware
            return int(dt.timestamp()) if dt else 0
        except Exception:
            return 0

    norm.sort(key=_dt_key)

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
            # Generate a UUID-based filename while preserving a safe extension
            doc_id = str(uuid.uuid4())
            _, raw_ext = os.path.splitext(file.filename or "")
            ext = _safe_ext(raw_ext)
            stored_name = f"{doc_id}{ext}"
            stored_path = DATA_DIR / stored_name

            # Stream to disk to avoid loading whole file into memory
            try:
                bytes_written = await _stream_save_upload(file, stored_path)
            except HTTPException:
                # Re-raise known HTTP exceptions (e.g., 413)
                raise
            except Exception:
                stored_path.unlink(missing_ok=True)
                raise HTTPException(status_code=500, detail="Upload failed")

            if bytes_written == 0:
                stored_path.unlink(missing_ok=True)
                raise HTTPException(status_code=400, detail=f"Empty file: {file.filename}")

            # Write a small sidecar with original name and other details
            meta = {
                "id": doc_id,
                "original_name": file.filename,
                "stored_name": stored_name,
                "content_type": file.content_type,
                "size": bytes_written,
            }
            (DATA_DIR / f"{doc_id}.meta.json").write_text(
                json.dumps(meta, indent=2), encoding="utf-8"
            )
            saved.append(meta)

    except HTTPException:
        # Pass through specific HTTP errors
        raise
    except Exception:
        raise HTTPException(status_code=500, detail="Upload failed")

    # Go back to index, which now lists cumulative uploads by original name
    return RedirectResponse(url="/", status_code=303)

@app.post("/index/search", response_class=JSONResponse)
async def index_search(payload: FinderPayload):
    job_id = payload.job_id
    base = _job_dir(job_id)
    idx_path = base / "cluster_index.json"
    parts_path = base / "parts.json"
    if not idx_path.exists():
        raise HTTPException(status_code=404, detail="No cluster index for this job")

    # Load artifacts
    try:
        cluster_index, id_to_cluster, identifier_postings = _load_cluster_artifacts(job_id)
    except Exception:
        raise HTTPException(status_code=500, detail="Could not load cluster artifacts")

    # Load parts
    try:
        parts = json.loads(parts_path.read_text(encoding="utf-8")) if parts_path.exists() else {}
    except Exception:
        parts = {}

    # Universe of part_ids
    all_pids = set(parts.keys())

    # Helper: sets for a given cluster id
    def role_sets_for_cluster(cid: str):
        members = set(cluster_index.get(cid, {}).get("members", []) or [])
        from_set, to_set, body_set = set(), set(), set()

        if identifier_postings:
            for ident in members:
                for post in identifier_postings.get(ident, []) or []:
                    pid = post.get("part_id")
                    role = post.get("role")
                    if not pid or pid not in all_pids:
                        continue
                    if role == "from":
                        from_set.add(pid)
                    elif role in ("to", "cc", "bcc", "recipient", "recipients"):
                        to_set.add(pid)
                    elif role == "body":
                        body_set.add(pid)
        else:
            # Fallback: derive by simple string containment in headers/body
            # Note: case-insensitive containment on raw strings; may over-match.
            lowered_members = [m.lower() for m in members]
            for pid, doc in parts.items():
                try:
                    h_from = (doc.get("From") or doc.get("from") or "").lower()
                    h_to = " ".join([
                        (doc.get("To") or doc.get("to") or ""),
                        (doc.get("Cc") or doc.get("cc") or ""),
                        (doc.get("Bcc") or doc.get("bcc") or ""),
                    ]).lower()
                    b = (doc.get("Body") or doc.get("body") or doc.get("text") or "").lower()
                    if any(m in h_from for m in lowered_members):
                        from_set.add(pid)
                    if any(m in h_to for m in lowered_members):
                        to_set.add(pid)
                    if any(m in b for m in lowered_members):
                        body_set.add(pid)
                except Exception:
                    continue

        return from_set, to_set, body_set

    # Apply rules with AND semantics across rules and roles
    candidates = set(all_pids)
    for rule in payload.rules:
        if rule.cluster_id not in cluster_index:
            # Unknown cluster: no matches possible for this rule
            candidates.clear()
            break
        r_from, r_to, r_body = role_sets_for_cluster(rule.cluster_id)
        local = set(all_pids)
        if rule.from_ == "yes":
            local &= r_from
        elif rule.from_ == "no":
            local -= r_from
        # 'any' → no constraint

        if rule.to == "yes":
            local &= r_to
        elif rule.to == "no":
            local -= r_to

        if rule.body == "yes":
            local &= r_body
        elif rule.body == "no":
            local -= r_body

        candidates &= local
        if not candidates:
            break

    # Normalize output like /index/cluster
    def pick(d, *keys):
        for k in keys:
            if k in d and d[k] is not None:
                return d[k]
        return None

    items = []
    for pid in candidates:
        doc = parts.get(pid, {})
        items.append({
            "from": pick(doc, "From", "from"),
            "to": pick(doc, "To", "to"),
            "subject": pick(doc, "Subject", "subject") or "(no subject)",
            "date": pick(doc, "Date", "date"),
            "body": pick(doc, "Body", "body", "text"),
        })

    def _dt_key(rec):
        d = rec.get("date")
        try:
            if not d:
                return 0
            dt = parsedate_to_datetime(d)
            return int(dt.timestamp()) if dt else 0
        except Exception:
            return 0

    items.sort(key=_dt_key)
    return {"matches": items}