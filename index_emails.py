import asyncio
import re
import hashlib
import spacy
from email.utils import getaddresses

from pathlib import Path
from typing import List, Dict, Any, Iterable, Optional, Union
from collections import defaultdict
from process_email import iter_message_bytes, run_mbox
from cluster_names import IdentityClusteringModel

EMAIL_RX = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")


class MboxIndex:
    """
    Build an index from one or more .mbox files that supports:
      - Fuzzy lookup of a person by any alias (name/email)
      - Returning the matching emails (From, To, Subject, Body)
      - Access to the computed identity clusters for UI use
    """

    def __init__(
        self,
        mbox_paths: Union[str, Path, Iterable[Union[str, Path]]],
        *,
        model_path: Union[str, Path] = "name_cluster.model",
        threshold: float = 0.95,
        tika_url: str = "http://localhost:9998",
        spacy_model: str = "en_core_web_trf",
    ) -> None:
        # Normalize paths to a list
        if isinstance(mbox_paths, (str, Path)):
            paths: List[Path] = [Path(mbox_paths)]
        else:
            paths = [Path(p) for p in mbox_paths]

        self.threshold = threshold
        self.parts: Dict[str, Dict[str, Any]] = {}     # part_id -> email dict
        self.postings: Dict[str, List[Dict[str, Any]]] = defaultdict(list)  # identifier -> [{part_id, role}]
        self.identifiers: set[str] = set()

        # Heavy load once
        self._nlp = spacy.load(spacy_model)

        # 1) Parse all mbox files into self.parts
        for mbox_path in paths:
            for msg_bytes in iter_message_bytes(str(mbox_path)):
                mail_boxes = asyncio.run(run_mbox(msg_bytes, str(mbox_path), tika_url))
                for k, v in mail_boxes.items():
                    self.parts[k] = v

        # 2) Build postings and identifier set
        self._index_identifiers()

        # 3) Cluster identifiers using your trained model
        self.cp = IdentityClusteringModel.load(Path(model_path))
        id_list = list(self.identifiers)
        clusters = self.cp.cluster(id_list, threshold=self.threshold)

        # 4) Build cluster indexes for lookup
        self.cluster_index: Dict[str, Dict[str, Any]] = {}
        self.id_to_cluster: Dict[str, str] = {}
        for members in clusters:
            cid = hashlib.sha1("||".join(sorted(members)).encode()).hexdigest()[:12]
            merged = []
            for m in members:
                merged.extend(self.postings.get(m, []))
            label = self._canonical_label(members)
            self.cluster_index[cid] = {
                "label": label,
                "members": sorted(members),
                "postings": merged,
            }
            for m in members:
                self.id_to_cluster[m] = cid

    def _canonical_label(self, members: List[str]) -> str:
        names = [m for m in members if "@" not in m]
        emails = [m for m in members if "@" in m]
        if names:
            names_sorted = sorted(names, key=lambda s: (-(" " in s or "," in s), -len(s), s.lower()))
            return names_sorted[0]
        if emails:
            return sorted(emails)[0]
        return sorted(members, key=lambda s: (-len(s), s.lower()))[0]

    # -------- internal helpers --------

    def _add_identifier(self, value: str, part_id: str, role: str) -> None:
        if not value:
            return
        self.identifiers.add(value)
        self.postings[value].append({"part_id": part_id, "role": role})

    def _add_person_identifiers(self, person: Dict[str, Any], part_id: str, role: str) -> None:
        if not person:
            return
        name = (person.get("name") or "").strip()
        email = (person.get("email") or "").strip()
        if name:
            self._add_identifier(name, part_id, role)
        if email:
            self._add_identifier(email, part_id, role)

    def debug_cluster_postings(self, cluster_id: str) -> Dict[str, int]:
        """Return a count of postings per member identifier for this cluster."""
        out: Dict[str, int] = {}
        if cluster_id not in self.cluster_index:
            return out
        for ident in self.cluster_index[cluster_id]["members"]:
            out[ident] = len(self.postings.get(ident, []))
        return out

    def _index_identifiers(self) -> None:
        for part_id, doc in self.parts.items():
            # From (single dict with name/email/raw)
            from_entry = doc.get("from")
            if isinstance(from_entry, dict):
                self._add_person_identifiers(from_entry, part_id, "from")
            elif isinstance(from_entry, str):
                # Back-compat: if older runs stored a raw string
                self._add_identifier(from_entry, part_id, "from")

            # Recipients (merged To/Cc/Bcc)
            recipients_field = doc.get("recipients") or []
            if recipients_field:
                for rec in recipients_field:
                    if isinstance(rec, dict):
                        self._add_person_identifiers(rec, part_id, "recipient")
                    elif isinstance(rec, str):
                        self._add_identifier(rec, part_id, "recipient")
            else:
                # Fallback for legacy docs that only have 'to'/'cc'/'bcc' as strings/lists
                raw_headers: list[str] = []
                for hdr in ("to", "cc", "bcc"):
                    v = doc.get(hdr)
                    if not v:
                        continue
                    if isinstance(v, list):
                        raw_headers.extend([str(x) for x in v])
                    else:
                        raw_headers.append(str(v))
                if raw_headers:
                    for name, email in getaddresses(raw_headers):
                        name = (name or "").strip()
                        email = (email or "").strip()
                        if name:
                            self._add_identifier(name, part_id, "recipient")
                        if email:
                            self._add_identifier(email, part_id, "recipient")

            # Body: keep as-is for now
            body_text = doc.get("body", "") or ""
            if body_text:
                nlp_doc = self._nlp(body_text)
                for ent in nlp_doc.ents:
                    if ent.label_ == "PERSON":
                        person = ent.text.strip().split("\n")[0]
                        self._add_identifier(person, part_id, "body")

                # emails seen in body (liberal, unanchored)
                for m in re.finditer(r"[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}", body_text, flags=re.IGNORECASE):
                    email = m.group(0).strip().rstrip(".,;:)")
                    self._add_identifier(email, part_id, "body")

    def _render_person(self, p: Any) -> str:
        if isinstance(p, dict):
            name = (p.get("name") or "").strip()
            email = (p.get("email") or "").strip()
            if name and email:
                return f"{name} <{email}>"
            if email:
                return email
            return name
        return str(p)

    def _render_recipients(self, doc: Dict[str, Any]) -> list[str]:
        recs = []
        recipients_field = doc.get("recipients")
        if isinstance(recipients_field, list) and recipients_field:
            recs = [self._render_person(r) for r in recipients_field]
        else:
            # fallback to legacy to/cc/bcc for display purposes
            raw_headers: list[str] = []
            for hdr in ("to", "cc", "bcc"):
                v = doc.get(hdr)
                if not v:
                    continue
                if isinstance(v, list):
                    raw_headers.extend([str(x) for x in v])
                else:
                    raw_headers.append(str(v))
            if raw_headers:
                for name, email in getaddresses(raw_headers):
                    name = (name or "").strip()
                    email = (email or "").strip()
                    if name and email:
                        recs.append(f"{name} <{email}>")
                    elif email:
                        recs.append(email)
                    elif name:
                        recs.append(name)
        return recs

    # -------- public API --------

    def query_cluster(
            self,
            cluster_id: str,
            roles: Optional[Iterable[str]] = None,
            limit: int = 100
    ) -> List[Dict[str, Any]]:
        if cluster_id not in self.cluster_index:
            return []
        occ = list(self.cluster_index[cluster_id]["postings"])  # copy
        if roles:
            # Normalize legacy role names to the unified schema
            legacy_to_recipient = {"to", "cc", "bcc"}
            roles_norm = set()
            for r in roles:
                if r in legacy_to_recipient:
                    roles_norm.add("recipient")
                else:
                    roles_norm.add(r)
            occ = [o for o in occ if o["role"] in roles_norm]
        seen = set()
        results: List[Dict[str, Any]] = []
        for o in occ:
            pid = o["part_id"]
            if pid in seen:
                continue
            seen.add(pid)
            doc = self.parts.get(pid, {})
            from_display = self._render_person(doc.get("from")) if doc.get("from") is not None else None
            to_display = self._render_recipients(doc)
            results.append({
                "From": from_display,
                "To": to_display,
                "Subject": doc.get("subject"),
                "Date": doc.get("date"),
                "Body": (doc.get("body") or doc.get("text") or doc.get("content") or doc.get("payload") or ""),
            })
            if len(results) >= limit:
                break
        return results

    def list_clusters(self, include_members: bool = False) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        for cid, data in self.cluster_index.items():
            row = {
                "id": cid,
                "label": data.get("label"),
                "size": len(data.get("members", [])),
            }
            if include_members:
                row["members"] = list(data.get("members", []))
            out.append(row)
        # Optional: sort by size desc, then label
        out.sort(key=lambda r: (-r["size"], r["label"] or ""))
        return out

    def cluster_part_ids(self, cluster_id: str) -> List[str]:
        if cluster_id not in self.cluster_index:
            return []
        return [o["part_id"] for o in self.cluster_index[cluster_id]["postings"]]


if __name__ == "__main__":
    # index = MboxIndex("/Users/kelcey.swan/Downloads/sample.mbox")
    index = MboxIndex([
        "synthetic_mboxes/mailbox_alice.johnson.mbox",
        "synthetic_mboxes/mailbox_bob.singh.mbox",
        "synthetic_mboxes/mailbox_eve.martin.mbox",
    ])

    # Show clusters available to the user
    clusters = index.list_clusters(include_members=True)
    for c in clusters:
        if c.get('members'):
            print(f"{c['id']} | {c['label']} | size={c['size']} | members={c.get('members')}...")

    cluster_ids = {c["id"] for c in clusters}
    cid = None
    while cid not in cluster_ids:
        cid = input("\nType cluster id: ").strip()

    print(f"Total parts linked to cluster: {len(index.cluster_part_ids(cid))}")

    print("\nMember posting counts:")
    for ident, cnt in index.debug_cluster_postings(cid).items():
        print(f"  {cnt:5d}  {ident}")

    print("\nIdentifier occurrences:")
    cluster_data = index.cluster_index.get(cid, {})
    members = cluster_data.get("members", [])
    for ident in members:
        posts = index.postings.get(ident, [])
        print(f"\n{ident}:")
        for p in posts:
            print(f"  part_id={p['part_id']}  role={p['role']}")

    # Display results
    cluster = next(c for c in clusters if c["id"] == cid)
    print(f"\n--- Emails for: {cluster['label']} (id={cid}) ---\n")

    matches = index.query_cluster(cid, roles=["from", "recipient", "body"])
    sorted_matches = sorted(
        matches,
        key=lambda r: r.get("Date") or "",
        reverse=True
    )
    for m in sorted_matches:
        print(
            f"From: {m['From']}\n"
            f"To: {', '.join(m['To']) if isinstance(m['To'], list) else m['To']}\n"
            f"Subject: {m['Subject']}\n"
            f"Date: {m['Date']}\n"
            f"Body:\n{m['Body']}\n"
            "---------------------------------------------\n"
        )
