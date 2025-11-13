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
from sklearn.ensemble import RandomForestClassifier
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
        # self.clf = LogisticRegression(max_iter=200, solver="liblinear", class_weight="balanced")
        self.clf = RandomForestClassifier(n_estimators=200, max_depth=12, random_state=231, class_weight='balanced')
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

        # --- Structured local-part matching to parsed names ---
        def _local_part_matches_name(local: str, parsed: dict) -> bool:
            """
            Returns True if the local part matches the parsed name in a common way.
            Accepts first.last, firstlast, f.last, firstl, etc.
            """
            local = local.lower()
            first = (parsed.get("first") or "").lower()
            last = (parsed.get("last") or "").lower()
            initials = (parsed.get("initials") or "").lower()
            if not local or not (first or last):
                return False
            tokens = re.split(r"[^a-z0-9]+", local)
            # Match first.last or last.first
            if len(tokens) == 2:
                if ((tokens[0] == first and tokens[1] == last) or
                    (tokens[0] == last and tokens[1] == first)):
                    return True
                # initials + last, guarded
                if first and last and ((tokens[0] == first[0] or tokens[0] == initials) and tokens[1] == last):
                    return True
                # first + last initial, guarded
                if first and last and tokens[0] == first and tokens[1] == last[0]:
                    return True
            # Match firstlast, firstl, flast, etc.
            if first and last:
                if local == first + last:
                    return True
                if local == first + last[0]:
                    return True
                if local == first[0] + last:
                    return True
            # initials (guard initials non-empty)
            if initials and local == initials:
                return True
            return False

        # Compute local_structure_match: does either local part match either parsed name?
        local_structure_match = 0.0
        for local in [la, lb]:
            for parsed in [pa, pb]:
                if _local_part_matches_name(local, parsed):
                    local_structure_match = 1.0
                    break
            if local_structure_match:
                break

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
            local_structure_match,
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

            # Derive blockers from local-part structure
            parts = re.split(r"[^a-z0-9]+", local.lower())
            parts = [p for p in parts if p]
            # If local part has two tokens, typical first+last or last+first
            if len(parts) == 2:
                # Add structured blockers for both orders
                keys.append(f"fl:{parts[0]}:{parts[1]}")
                keys.append(f"fl:{parts[1]}:{parts[0]}")
                # Add human‑friendly fuzzy blockers for initials + last name
                # Example: jlm -> j + martinez
                if parts[0] and parts[1]:
                    # If either token is a single character (initial), treat it as first initial
                    if len(parts[0]) == 1:
                        keys.append(f"initlast:{parts[0]}:{parts[1]}")
                    if len(parts[1]) == 1:
                        keys.append(f"initlast:{parts[1]}:{parts[0]}")
                # Also block by last name
                if parts[1].isalpha() and len(parts[1]) >= 2:
                    keys.append(f"ln:{parts[1]}")
                if parts[0].isalpha() and len(parts[0]) >= 2:
                    keys.append(f"ln:{parts[0]}")
                # Fallback: allow matching when email is clearly first.last and name has matching last name
                # This improves cases like "juan.luis@uni.edu" <-> "Juan Luis Martinez"
                if parts[1].isalpha() and len(parts[1]) >= 2:
                    keys.append(f"fallback_ln:{parts[1]}")
                if parts[0].isalpha() and len(parts[0]) >= 2:
                    keys.append(f"fallback_ln:{parts[0]}")
                # Block by first initial + last
                if parts[0] and parts[1] and parts[0][0].isalpha():
                    keys.append(f"lnfi:{parts[1]}:{parts[0][0]}")
                if parts[1] and parts[0] and parts[1][0].isalpha():
                    keys.append(f"lnfi:{parts[0]}:{parts[1][0]}")
            elif len(parts) == 1 and parts[0]:
                # Try to extract first/last from a single-token local part (e.g., firstlast, flast, firstl)
                local = parts[0]
                # Heuristics for common real-world formats
                # Try to split at likely boundary between first and last (e.g., 'johnsmith', 'jsmith', 'johns')
                # Try last 3, 4, 5 letters as possible last name
                for k in [5, 4, 3]:
                    if len(local) > k + 1:
                        first, last = local[:-k], local[-k:]
                        keys.append(f"fl:{first}:{last}")
                        keys.append(f"fl:{last}:{first}")
                # Also try first letter + rest (fsmith, jdoe, etc)
                if len(local) > 2:
                    keys.append(f"fl:{local[0]}:{local[1:]}")
                    keys.append(f"fl:{local[1:]}:{local[0]}")
                # As fallback, block by the whole local part
                keys.append(f"local:{local}")

            # Add general token-based blockers from local part,
            # stripping digits to allow matches like l.zhao99 <-> Zhao, Li
            for token in parts:
                alpha = re.sub(r'[^a-z]+', '', token)
                if alpha and len(alpha) > 1:
                    keys.append(f"token:{alpha}")

        # Name-based blocks (unchanged, tight)
        if p["last"] and p["first"]:
            keys.append(f"lnfi:{p['last']}:{p['first'][0]}")
            # Add fuzzy name blocker: first initial + last name
            keys.append(f"initlast:{p['first'][0]}:{p['last']}")
            # Provide matching fallback for last name so names connect to emails that only expose last
            keys.append(f"fallback_ln:{p['last']}")
            g = NICK2GROUP.get(p["first"])
            if g is not None:
                keys.append(f"lnng:{p['last']}:{g}")  # last name + nickname-group id

        # Add general token-based blockers from parsed name components
        for token in self._tokens(f"{p['first']} {p['middle']} {p['last']}"):
            if token.isalpha() and len(token) > 1:
                keys.append(f"token:{token}")

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
        print("\nClustering inputs:")
        for s in strings:
            print("  ", repr(s))
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
            s_i, s_j = strings[i], strings[j]
            p = float(self.clf.predict_proba([self._features(s_i, s_j)])[0][1])
            if p < threshold:
                continue

            ai, bi = self._is_email(s_i), self._is_email(s_j)

            # --- Case 1: both are emails ---
            # Be extremely conservative: only merge if they are exactly the same address.
            if ai and bi:
                if s_i.strip().lower() != s_j.strip().lower():
                    continue

            # --- Case 2: one email, one name ---
            elif ai != bi:
                email = s_i if ai else s_j
                name = s_j if ai else s_i
                local, dom = self._split_email(email)
                parsed = self._parse_name(name)
                first = (parsed.get("first") or "").lower()
                last = (parsed.get("last") or "").lower()
                ok = False
                local_low = local.lower()

                # Heuristics: last name appears in local part, or simple first/last patterns
                if last and last in local_low:
                    ok = True
                elif first and last and (first + last) == local_low:
                    ok = True
                elif first and last and (first[0] + last) == local_low:
                    ok = True
                elif first and last and (first + last[0]) == local_low:
                    ok = True

                if not ok:
                    continue

            # --- Case 3: both are names ---
            else:
                pa, pb = self._parse_name(s_i), self._parse_name(s_j)
                la, lb = pa.get("last"), pb.get("last")
                fa, fb = pa.get("first"), pb.get("first")

                # Require a shared last name to merge two pure names
                if not (la and lb and la == lb):
                    continue

                # If we have first names, require compatible initials or nickname-group
                if fa and fb:
                    fa0, fb0 = fa[0], fb[0]
                    ga = NICK2GROUP.get(fa, None)
                    gb = NICK2GROUP.get(fb, None)
                    if (fa0 != fb0) and not (ga is not None and ga == gb):
                        continue

            # Passed all structural checks: accept edge
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


if __name__ == "__main__":
    model = IdentityClusteringModel.load(Path("name_cluster.model"))

    test_data = [
        "guido.van.rossum@example.org",
        "Moxie Marlinspike's",
        "Moxie Marlinspike",
        "Grace Hopper",
        "Alan Turing",
        "Alan Turing <a.turing@example.org>",
        "Moxie Marlinspike <marlinspike@example.com>",
        "Donald Knuth <donald.knuth@example.com>",
        "Guido van Rossum <guido.van.rossum@example.org>",
        "linus.torvalds@example.com",
        "a.turing@example.org",
        "donald.knuth@example.com",
        "Linus Torvalds",
        "jvn@example.com",
        "ada.lovelace@example.org",
        "Ray Kurzweil <ray.kurzweil@example.org>",
        "John von Neumann <jvn@example.com>",
        "Guido van Rossum",
        "grace.hopper@example.com",
        "ray.kurzweil@example.org",
        "Donald Knuth",
        "Hedy Lamarr",
        "Hedy Lamarr <hedy.lamarr@example.org>",
        "Grace Hopper <grace.hopper@example.com>",
        "hedy.lamarr@example.org",
        "someone@example.com",
        "Ada Lovelace <ada.lovelace@example.org>",
        "Ada Lovelace",
        "John von Neumann",
        "Ray Kurzweil",
        "Linus Torvalds <linus.torvalds@example.com>",
        "marlinspike@example.com"
    ]

    clusters = model.cluster(test_data, threshold=0.9)
    for i, cluster in enumerate(clusters):
        print(f"Cluster {i+1}:")
        for item in cluster:
            print("  ", item)

    print("\n🔍 Raw match score between:")
    a = "juan.luis@uni.edu"
    b = "Juan Luis Martinez"
    score = model.clf.predict_proba([model._features(a, b)])[0][1]
    print(f"{a} ↔ {b} = {score:.3f}")
