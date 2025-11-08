import hashlib
import unicodedata
import html
import re
from typing import Iterable, Tuple, Dict, List


def sha256_text(text: str) -> str:
    hash = hashlib.sha256(
        text.encode(
            "utf-8", errors="ignore"
        )
    ).hexdigest()
    return hash


_WS_RE = re.compile(r"\s+", re.MULTILINE)
_PUNCT_GAPS_RE = re.compile(r"\s*([,.;:!?()\[\]{}<>/\\|@#$%^&*_+=~\-])\s*")


def normalize_text(text: str) -> str:
    """
    Light-touch normalization to make HTML-vs-plain from Tika comparable.
    - Unicode normalize (NFKC)
    - Unescape HTML entities (&nbsp; &amp; etc.)
    - Collapse whitespace
    - Normalize spacing around punctuation
    - Strip leading/trailing whitespace
    """
    if not text:
        return ""
    # Unescape HTML entities first (e.g., &nbsp; -> non-breaking space)
    text = html.unescape(text)
    # Unicode normalize (squash lookalikes, compatibility forms)
    text = unicodedata.normalize("NFKC", text)
    # Replace non-breaking spaces with regular spaces
    text = text.replace("\u00A0", " ")
    # Normalize spacing around punctuation to reduce tokenization drift
    text = _PUNCT_GAPS_RE.sub(r" \1 ", text)
    # Collapse whitespace
    text = _WS_RE.sub(" ", text).strip()
    return text


def _shingles(words: List[str], k: int = 5) -> Iterable[str]:
    for i in range(max(1, len(words) - k + 1)):
        yield " ".join(words[i:i+k])


def simhash_64(text: str, k: int = 5) -> int:
    # Normalize before tokenization for stability across HTML/plain
    text = normalize_text(text)
    words = [w for w in text.lower().split()]
    if not words:
        return 0  # avoid the all-ones artifact when no shingles are produced

    bits = [0]*64
    for sh in _shingles(words, k=k):
        h = int(hashlib.blake2b(sh.encode("utf-8"),
                                digest_size=8).hexdigest(), 16)
        for i in range(64):
            bits[i] += 1 if (h >> i) & 1 else -1
    out = 0
    for i, v in enumerate(bits):
        if v >= 0:
            out |= (1 << i)
    return out


def hamming(a: int, b: int) -> int:
    return (a ^ b).bit_count()


def cluster_simhashes(items: Iterable[Tuple[str, int]], threshold: int = 3) -> Dict[str, str]:
    """
    items: iterable of (doc_id, simhash)
    return: mapping doc_id -> cluster_id
        (cluster_id is the first doc_id in that cluster)
    """
    clusters = []
    mapping = {}
    for doc_id, sh in items:
        placed = False
        for leader_doc, leader_sh in clusters:
            if hamming(sh, leader_sh) <= threshold:
                mapping[doc_id] = leader_doc
                placed = True
                break
        if not placed:
            clusters.append((doc_id, sh))
            mapping[doc_id] = doc_id
    return mapping
