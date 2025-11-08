from typing import List, Tuple, Dict, Set
from pathlib import Path
import itertools
import random
import re
import difflib
import unicodedata
from collections import defaultdict, deque

import joblib
from sklearn.linear_model import LogisticRegression
from nameparser import HumanName
from email.utils import parseaddr, getaddresses


EMAIL_RX = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")

# Near the top of your file
NICKNAME_GROUPS = [
    {"elizabeth", "liz", "beth", "eliza", "betty", "liza", "lisa"},
    {"alexander", "alex", "sandy", "xander"},
    {"anthony", "tony"},
    {"andrew", "andy", "drew"},
    {"margaret", "maggie", "meg", "peggy"},
    {"jonathan", "jon", "john", "johnny"},  # include only if acceptable for your corpus
    {"christopher", "chris"},
    {"patricia", "pat", "patti", "trish"},
    {"rebecca", "becky", "becca", "bex"},
    {"sharon", "shaz", "sheri"},
    {"david", "dave", "davey"},
]
# Build fast lookup: name -> group_id
NICK2GROUP = {alias: i for i, g in enumerate(NICKNAME_GROUPS) for alias in g}


class IdentityClusteringModel:
    """
    Train on nested clusters of strings (names/emails), save the model,
    and later cluster a big flat list into identity groups.
    Uses nameparser to improve features and blocking.
    """
    def __init__(self):
        self.clf = LogisticRegression(max_iter=200, solver="liblinear", class_weight="balanced")
        self.is_trained = False
        self._parse_cache: Dict[str, Dict[str, str]] = {}

    #  Basic normalisation & parsing
    @staticmethod
    def _strip_accents(s: str) -> str:
        return unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("ascii")

    def _normalise(self, s: str) -> str:
        return " ".join(self._strip_accents(s).strip().lower().split())

    @staticmethod
    def _is_email(s: str) -> bool:
        return bool(EMAIL_RX.match(s.strip()))

    @staticmethod
    def _split_email(s: str) -> Tuple[str, str]:
        s = s.strip().lower()
        if "@" not in s:
            return "", ""
        # Maybe a twitter-like handle?
        elif s[0] == "@":
            domain = ""
            local = s[1:]
        else:
            local, domain = s.split("@", 1)
        return local, domain

    def _parse_name(self, s: str) -> Dict[str, str]:
        """
        Parse once and cache. Returns dict with keys:
        first, middle, last, title, suffix, initials
        """
        if s in self._parse_cache:
            return self._parse_cache[s]

        if self._is_email(s):
            out = {
                "first": "",
                "middle": "",
                "last": "",
                "title": "",
                "suffix": "",
                "initials": "",
            }
        else:
            n = HumanName(self._strip_accents(s))
            first = (n.first or "").lower().strip()
            middle = (n.middle or "").lower().strip()
            last = (n.last or "").lower().strip()
            title = (n.title or "").lower().strip()
            suffix = (n.suffix or "").lower().strip()
            initials = "".join(x[0] for x in (first, middle, last) if x)
            out = {
                "first": first,
                "middle": middle,
                "last": last,
                "title": title,
                "suffix": suffix,
                "initials": initials
            }

        self._parse_cache[s] = out
        return out

    # Features
    @staticmethod
    def _seq_ratio(a: str, b: str) -> float:
        return difflib.SequenceMatcher(None, a, b).ratio()

    @staticmethod
    def _tokens(s: str) -> List[str]:
        return [t for t in re.split(r"[^a-z0-9]+", s) if t]

    @staticmethod
    def _jaccard(a: List[str], b: List[str]) -> float:
        if not a and not b:
            return 0.0
        sa, sb = set(a), set(b)
        u = len(sa | sb)
        return len(sa & sb) / u if u else 0.0

    @staticmethod
    def _len_sim(a: str, b: str) -> float:
        la, lb = len(a), len(b)
        if la + lb == 0:
            return 1.0
        return 1.0 - abs(la - lb) / (la + lb)

    @staticmethod
    def _prefix_overlap(a: str, b: str) -> float:
        n = min(len(a), len(b))
        i = 0
        while i < n and a[i] == b[i]:
            i += 1
        return i / max(1, max(len(a), len(b)))

    def _features(self, a: str, b: str) -> List[float]:
        na, nb = self._normalise(a), self._normalise(b)
        ta, tb = self._tokens(na), self._tokens(nb)

        # Emails
        a_is_email = 1.0 if self._is_email(a) else 0.0
        b_is_email = 1.0 if self._is_email(b) else 0.0
        la, da = self._split_email(a) if a_is_email else ("", "")
        lb, db = self._split_email(b) if b_is_email else ("", "")

        same_email = 1.0 if (a_is_email and b_is_email and a.strip().lower() == b.strip().lower()) else 0.0
        same_domain = 1.0 if (da and db and da == db) else 0.0
        local_sim = self._seq_ratio(la, lb) if (la and lb) else 0.0

        # Names via nameparser
        pa, pb = self._parse_name(a), self._parse_name(b)
        last_same = 1.0 if (pa["last"] and pa["last"] == pb["last"]) else 0.0
        last_sim = self._seq_ratio(pa["last"], pb["last"]) if (pa["last"] and pb["last"]) else 0.0

        # First-name vs initial compatibility (A vs Alex)
        first_same = 1.0 if (pa["first"] and pa["first"] == pb["first"]) else 0.0
        first_initial_match = 1.0 if (pa["first"] and pb["first"] and pa["first"][0] == pb["first"][0]) else 0.0
        initials_same = 1.0 if (pa["initials"] and pa["initials"] == pb["initials"]) else 0.0

        ga = NICK2GROUP.get(pa["first"], None)
        gb = NICK2GROUP.get(pb["first"], None)
        nickname_match = 1.0 if (ga is not None and ga == gb) else 0.0

        # Core string similarities
        seq_sim = self._seq_ratio(na, nb)
        tok_sim = self._jaccard(ta, tb)
        pre_sim = self._prefix_overlap(na, nb)
        len_sim = self._len_sim(na, nb)
        len_ratio = (min(len(na), len(nb)) / max(1, max(len(na), len(nb))))

        # name found in email local
        last_in_local = 1.0 if (pa["last"] and (pa["last"] in la or pa["last"] in lb)) else 0.0
        first_in_local = 1.0 if (pa["first"] and (pa["first"] in la or pa["first"] in lb)) else 0.0

        return [
            seq_sim,
            tok_sim,
            pre_sim,
            len_sim,
            len_ratio,
            same_email,
            same_domain,
            local_sim,
            last_same,
            last_sim,
            first_same,
            first_initial_match,
            initials_same,
            nickname_match,
            last_in_local,
            first_in_local,
            a_is_email,
            b_is_email,
        ]

    def _blocking_keys(self, s: str) -> List[str]:
        keys: List[str] = []
        n = self._normalise(s)
        p = self._parse_name(s)

        # Email domain block
        if self._is_email(s):
            local, dom = self._split_email(s)
            if dom:
                keys.append(f"dom:{dom}")

            # NEW: derive name-like blockers from email local-part
            parts = re.split(r"[^a-z0-9]+", local.lower())
            parts = [p for p in parts if p]
            if parts:
                first_tok = parts[0]
                last_tok = parts[-1]
                if last_tok.isalpha() and len(last_tok) >= 2:
                    keys.append(f"ln:{last_tok}")
                    if first_tok and first_tok[0].isalpha():
                        keys.append(f"lnfi:{last_tok}:{first_tok[0]}")

        # Name-based blocks (unchanged, tight)
        if p["last"] and p["first"]:
            keys.append(f"lnfi:{p['last']}:{p['first'][0]}")
            g = NICK2GROUP.get(p["first"])
            if g is not None:
                keys.append(f"lnng:{p['last']}:{g}")  # last name + nickname-group id

        # Ultra-tight fallback only when no better key exists
        if not keys and n:
            keys.append(f"npx5:{n[:5]}")

        return keys

    def _candidate_pairs(self, strings: List[str], max_bucket: int = 5000) -> Set[Tuple[int, int]]:
        key_to_ids: Dict[str, List[int]] = defaultdict(list)
        for i, s in enumerate(strings):
            for k in self._blocking_keys(s):
                key_to_ids[k].append(i)

        pairs: Set[Tuple[int, int]] = set()
        for ids in key_to_ids.values():
            if len(ids) <= 1 or len(ids) > max_bucket:
                continue
            ids.sort()
            pairs.update(itertools.combinations(ids, 2))
        return pairs

    # Supervised pairs from clusters
    def _labelled_pairs_from_clusters(
            self,
            clusters: List[List[str]],
            neg_per_pos: int = 2,
            seed: int = 13536,
    ) -> Tuple[List[List[float]], List[int]]:
        rng = random.Random(seed)
        universe = list({s for cl in clusters for s in cl})
        member = {s: i for i, cl in enumerate(clusters) for s in cl}

        # Positives: all within-cluster pairs
        pos_pairs = [pair for cl in clusters for pair in itertools.combinations(cl, 2)]

        # Negatives: near-length, cross-cluster
        flat_sorted = sorted(universe, key=lambda x: len(self._normalise(x)))
        neg_pairs: List[Tuple[str, str]] = []
        window = 20
        for idx, s in enumerate(flat_sorted):
            lo, hi = max(0, idx - window), min(len(flat_sorted), idx + window + 1)
            cands = [t for t in flat_sorted[lo:hi] if member[t] != member[s] and t != s]
            rng.shuffle(cands)
            neg_pairs.extend((s, t) for t in cands[:neg_per_pos])

        X, y = [], []
        for a, b in pos_pairs:
            X.append(self._features(a, b))
            y.append(1)
        for a, b in neg_pairs:
            X.append(self._features(a, b))
            y.append(0)
        return X, y

    # Public API
    def fit(self, training_clusters: List[List[str]]) -> None:
        X, y = self._labelled_pairs_from_clusters(training_clusters)
        self.clf.fit(X, y)
        self.is_trained = True

    def save(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        joblib.dump({"model": self.clf, "is_trained": self.is_trained}, path)

    @classmethod
    def load(cls, path: Path) -> "IdentityClusteringModel":
        blob = joblib.load(path)
        obj = cls()
        obj.clf = blob["model"]
        obj.is_trained = blob.get("is_trained", True)
        return obj

    def cluster(self, strings: List[str], threshold: float = 0.6) -> List[List[str]]:
        """
        threshold = minimum predicted match probability to connect two strings.
        """
        if not self.is_trained:
            raise RuntimeError("Model not trained. Call fit() or load() first.")
        if not strings:
            return []

        # If an item is just "<email>", drop the angle brackets
        only_email_in_brackets = re.compile(r'^\s*<\s*([^>]+)\s*>\s*$')
        cleaned_strings = []
        for s in strings:
            m = only_email_in_brackets.match(s)
            if m:
                candidate = m.group(1).strip()
                cleaned_strings.append(candidate)
            else:
                cleaned_strings.append(s)
        strings = cleaned_strings

        # Score & build graph
        adj: Dict[int, List[int]] = defaultdict(list)

        # Safely force-link only simple single-address forms: "Name <email>"
        expanded: List[str] = []
        seen_vals: Dict[str, int] = {}
        forced_edges: List[Tuple[int, int]] = []

        def add_item(val: str) -> int:
            if val not in seen_vals:
                seen_vals[val] = len(expanded)
                expanded.append(val)
            return seen_vals[val]

        for s in strings:
            # Detect a *single* address of the form Name <email>, avoiding comma-separated lists
            if ('<' in s and '>' in s) and (',' not in s) and (';' not in s):
                disp, email = parseaddr(s)
                email = (email or '').strip()
                disp = (disp or '').strip()

                # If the display name accidentally captured the header label (e.g., "To: Guido van Rossum"), strip it
                if disp:
                    disp = re.sub(r'^\s*(?:to|from|cc|bcc)\s*:\s*', '', disp, flags=re.IGNORECASE).strip()

                # If parseaddr gave no display name but this looks like a header line, try to grab the name between the label and '<'
                if not disp:
                    m_hdr = re.match(r'^\s*(?:to|from|cc|bcc)\s*:\s*(.*?)\s*<', s, flags=re.IGNORECASE)
                    if m_hdr:
                        disp = m_hdr.group(1).strip()

                if email:
                    j = add_item(email)
                    if disp:
                        i = add_item(disp)
                        forced_edges.append((i, j))  # guarantee link between display name and email
                    # If no display name, we still add the email node above
                    continue  # handled
            # Fallback: keep the original token as-is
            add_item(s)

        strings = expanded

        pairs = self._candidate_pairs(strings)
        if not pairs and not forced_edges:
            return [[s] for s in strings]

        # Add forced edges from expanded headers
        for i_forced, j_forced in forced_edges:
            adj[i_forced].append(j_forced)
            adj[j_forced].append(i_forced)

        for i, j in pairs:
            p = float(self.clf.predict_proba([self._features(strings[i], strings[j])])[0][1])
            if p >= threshold:
                # Minimal guardrail: if last names differ, only allow an edge when emails corroborate strongly
                pa, pb = self._parse_name(strings[i]), self._parse_name(strings[j])
                ai, bi = self._is_email(strings[i]), self._is_email(strings[j])

                if not ai and not bi:
                    gfa = NICK2GROUP.get(pa["first"])
                    gfb = NICK2GROUP.get(pb["first"])
                    nickname_ok = (gfa is not None and gfa == gfb)

                    last_ok = bool(pa["last"] and pb["last"] and pa["last"] == pb["last"])
                    first_ok = bool(
                        (pa["first"] and pb["first"] and pa["first"] == pb["first"]) or
                        (pa["first"] and pb["first"] and pa["first"][0] == pb["first"][0]) or
                        nickname_ok
                    )

                    # If same last name but no compatible first/nickname → do not connect
                    if not (last_ok and first_ok):
                        continue

                last_a, last_b = pa.get("last", ""), pb.get("last", "")

                if (not ai and not bi) and ((last_a != last_b) or (not last_a or not last_b)):
                    ai, bi = self._is_email(strings[i]), self._is_email(strings[j])
                    la, da = self._split_email(strings[i]) if ai else ("", "")
                    lb, db = self._split_email(strings[j]) if bi else ("", "")
                    same_email = ai and bi and strings[i].strip().lower() == strings[j].strip().lower()
                    same_domain = bool(da and db and da == db)
                    local_sim = self._seq_ratio(la, lb) if (la and lb) else 0.0
                    # Require exact email OR same domain with very similar local parts
                    if not (same_email or (same_domain and local_sim >= 0.85)):
                        continue

                adj[i].append(j)
                adj[j].append(i)

        # Connected components
        visited, clusters_idx = set(), []

        def bfs(start: int) -> List[int]:
            q, comp = deque([start]), []
            visited.add(start)
            while q:
                u = q.popleft()
                comp.append(u)
                for v in adj.get(u, []):
                    if v not in visited:
                        visited.add(v)
                        q.append(v)
            return comp

        for i in range(len(strings)):
            if i not in visited:
                clusters_idx.append(sorted(bfs(i)))

        clusters = [[strings[i] for i in comp] for comp in clusters_idx]
        return sorted([sorted(c) for c in clusters], key=lambda c: (-len(c), c))
