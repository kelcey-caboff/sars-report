import httpx
import asyncio
import json
import mailbox
import hashlib
import dateutil.parser
import uuid
from email import message_from_bytes
from email.header import decode_header, make_header
from email.utils import getaddresses
from email.policy import default as default_policy
from email.message import Message
from simhasher import simhash_64


read_parts = set()


def _sha256(data):
    h = hashlib.sha256()
    h.update(data.encode("utf-8", errors="ignore"))
    return h.hexdigest()


def decode_maybe(value):
    if not value:
        return ""
    try:
        return str(make_header(decode_header(value)))
    except Exception:
        return value


def iter_message_bytes(mbox_path):
    mbox = mailbox.mbox(mbox_path, factory=None, create=False)
    try:
        for msg in mbox:
            try:
                yield msg.as_bytes()
            except Exception:
                yield bytes(msg)
    finally:
        mbox.close()


def _payload_bytes(part):
    b = part.get_payload(decode=True)
    if isinstance(b, (bytes, bytearray)):
        return bytes(b)
    try:
        inner = part.get_payload(0)
        if isinstance(inner, Message):
            return inner.as_bytes()
    except Exception:
        pass
    return b""


async def process_document(client: httpx.AsyncClient, msg_bytes: bytes, tika_url: str):
    text_resp = await client.put(f"{tika_url}/tika", content=msg_bytes, headers={"Accept": "text/plain"})
    meta_resp = await client.put(f"{tika_url}/meta", content=msg_bytes, headers={"Accept": "application/json"})
    if text_resp.status_code >= 400:
        pass
    if meta_resp.status_code >= 400:
        pass
    try:
        metadata: dict = meta_resp.json()
    except Exception:
        metadata = {"raw": meta_resp.text}

    plaintext = text_resp.text
    return (metadata, plaintext)


async def extract_all_parts(raw, tika_url, client, filename=None):
    """
    Recursively flatten an email into analysable parts. If the blob does not
    look like an email, return a single generic part representing the whole
    file.
    """

    msg = message_from_bytes(raw, policy=default_policy)

    # --- Normalise participants ---
    def _parse_addresses(value):
        addrs = getaddresses([decode_maybe(value or "")])
        out = []
        seen = set()
        for name, email in addrs:
            name = (name or "").strip() or None
            email = (email or "").strip().lower() or None
            key = (name or "", email or "")
            if key in seen:
                continue
            seen.add(key)
            raw = decode_maybe(value or "")
            out.append({
                "name": name,
                "email": email,
                "raw": raw,
            })
        return out

    from_list = _parse_addresses(msg.get("From"))
    envelope_from = from_list[0] if from_list else {"name": None, "email": None, "raw": decode_maybe(msg.get("From"))}

    # Merge To/CC/BCC into a single recipients list (order preserved; de-duped)
    recipients = []
    seen_recip = set()
    for header in ("To", "Cc", "Bcc"):
        for p in _parse_addresses(msg.get(header)):
            key = (p.get("name") or "", p.get("email") or "")
            if key in seen_recip:
                continue
            seen_recip.add(key)
            recipients.append(p)

    envelope_date = decode_maybe(msg.get("Date"))
    envelope_subject = decode_maybe(msg.get("Subject"))
    message_id = decode_maybe(msg.get("Message-ID") or msg.get("Message-Id") or "")
    message_id = (message_id or "").strip().lower()
    out = []

    async def walk(m, depth):
        ctype = m.get_content_type()
        filename_part = m.get_filename()

        if m.is_multipart():
            for sub in m.iter_parts():
                await walk(sub, depth+1)
            return

        if ctype == "message/rfc822":
            payload = _payload_bytes(m)
            if payload:
                subparts = await extract_all_parts(payload, tika_url, client, filename=filename_part)
                out.extend(subparts)
            return

        metadata, plaintext = await process_document(client, _payload_bytes(m), tika_url)
        part_hash = _sha256(plaintext)
        part_simhash = simhash_64(plaintext)
        date = dateutil.parser.parse(envelope_date, dayfirst=True)

        if part_hash not in read_parts:
            out.append({
                "part_id": str(uuid.uuid4()),
                "part_hash": f"sha256:{part_hash}",
                "part_simhash": part_simhash,
                "message_id": message_id,
                "date": str(date),
                "subject": envelope_subject,
                "from": envelope_from,
                "recipients": recipients,
                "depth": depth,
                "content_type": ctype,
                "filename": filename_part,
                "tika": metadata,
                "body": plaintext
            })
        read_parts.add(part_hash)

    await walk(msg, 0)
    return out


async def run_mbox(mbytes, mbox_path, tika_url):
    parts = {}
    async with httpx.AsyncClient(timeout=120) as client:
        for part in await extract_all_parts(mbytes, tika_url, client, filename=mbox_path):
            parts[part.get("part_id")] = part
    return parts


if __name__ == "__main__":
    mbox_path = "/Users/kelcey.swan/Downloads/sample.mbox"

    parts = {}
    for msg_bytes in iter_message_bytes(mbox_path):
        mail_boxes = asyncio.run(run_mbox(msg_bytes, mbox_path, "http://sars-tika:9998"))
        for k, v in mail_boxes.items():
            parts[k] = v

    print(json.dumps(parts, indent=4))
