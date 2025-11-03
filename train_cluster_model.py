import random
import string
from cluster_names import IdentityClusteringModel
from pathlib import Path


def generate_clusters(n_clusters=100, size_range=(9, 12), seed=42):
    random.seed(seed)

    # Canonical->nicknames (add your own easily)
    nick = {
        "Andrew": ["Andy", "Drew"], "Anthony": ["Tony"], "Alexander": ["Alex", "Xander"],
        "Benjamin": ["Ben", "Benny"], "Charles": ["Charlie", "Chuck"], "Christopher": ["Chris", "Topher"],
        "Daniel": ["Dan", "Danny"], "Elizabeth": ["Liz", "Beth", "Eliza", "Lizzy"],
        "Joanna": ["Jo", "Joey"], "Jonathan": ["Jon", "Jonny"], "Joseph": ["Joe", "Joey", "Jo"],
        "Katherine": ["Kate", "Katie", "Kathy", "Kat"], "Margaret": ["Maggie", "Meg", "Peggy"],
        "Matthew": ["Matt"], "Michael": ["Mike", "Mikey"], "Nicholas": ["Nick", "Nicky"],
        "Patricia": ["Pat", "Patty"], "Rebecca": ["Becky", "Becca"], "Richard": ["Rich", "Rick", "Ricky"],
        "Robert": ["Rob", "Bob", "Bobby"], "Samuel": ["Sam", "Sammy"], "Thomas": ["Tom", "Tommy"],
        "William": ["Will", "Bill", "Billy"], "Jo": ["Joanna"], "Andy": ["Andrew"]
    }

    surnames = ["Smith", "Jones", "Brown", "Taylor", "Wilson", "Johnson", "Clark", "Lewis", "Walker",
                "Hall", "Young", "Allen", "King", "Wright", "Scott", "Green", "Baker", "Adams",
                "Harris", "Roberts", "Michaels", "Moore", "Jackson", "Thompson", "Martin"]
    titles = ["Mr", "Mrs", "Ms", "Miss", "Dr", "Prof", "Mx"]
    domains = ["gmail.com", "hotmail.com", "yahoo.com", "example.com", "mail.com", "workplace.co", "department.gov.uk"]

    def diacritic_once(s):
        if random.random() < 0.07:
            rep = {"a": "á", "e": "é", "i": "í", "o": "ó", "u": "ú", "A": "Á", "E": "É", "O": "Ó", "U": "Ú"}
            for i, ch in enumerate(s):
                if ch in rep:
                    return s[:i] + rep[ch] + s[i+1:]
        return s

    def typo(s):
        if len(s) < 3 or random.random() > 0.10:
            return s
        i = random.randrange(len(s))
        op = random.choice(("del", "ins", "sub", "swap"))
        if op == "del":
            return s[:i] + s[i+1:]
        if op == "ins":
            return s[:i] + random.choice(string.ascii_lowercase) + s[i:]
        if op == "sub":
            return s[:i] + random.choice(string.ascii_lowercase) + s[i+1:]
        if op == "swap" and i < len(s)-1:
            return s[:i] + s[i+1] + s[i] + s[i+2:]
        return s

    def email(first, last):
        fst, lst = first.lower(), last.lower()
        pats = [f"{fst}.{lst}", f"{fst}_{lst}", f"{fst}{lst}", f"{fst[0]}{lst}", f"{fst}{lst[0]}"]
        local = random.choice(pats)
        if random.random() < 0.12:
            local += str(random.randint(1, 99))
        return f"{local}@{random.choice(domains)}"

    def initials(first, middle, last, dots=True):
        parts = [first[0], * ([middle[0]] if middle else []), last[0]]
        if dots:
            parts = [p+"." for p in parts]
        return " ".join(parts)

    def gen_variants(first, last):
        middle = random.choice([None, "A", "B", "C", "D", "E", "J", "M"]) if random.random() < 0.4 else None
        pool = set()

        # canonical forms
        pool.add(f"{first} {last}")
        if middle:
            pool.add(f"{first} {middle} {last}")
        pool.add(f"{last}, {first}")
        pool.add(initials(first, middle, last, dots=True))
        pool.add(f"{initials(first, middle, last, dots=True)} {last}")
        pool.add(email(first, last))
        pool.add(f"{random.choice(titles)} {last}")
        pool.add(f"{random.choice(titles)} {first[0]}. {last}")
        pool.add(f"{first}.{last}")
        pool.add(f"{first}{last}")
        pool.add(f"{first.lower()} {last.lower()}")
        pool.add(f"{first.upper()} {last.upper()}")
        pool.add(f"{first[0]}. {last}")

        # nicknames
        for alt in nick.get(first, []):
            pool.add(f"{alt} {last}")
            pool.add(f"{last}, {alt}")
            pool.add(email(alt, last))
            pool.add(f"{alt}.{last}")

        # spice with diacritics/typos
        out = set()
        for v in pool:
            v = " ".join(v.split())
            v = diacritic_once(v)
            if random.random() < 0.35:
                v = typo(v)
            out.add(v)

        # ensure variability by small extra mutations
        extra = []
        for v in random.sample(list(out), k=min(6, len(out))):
            if "@" not in v and random.random() < 0.5:
                extra.append(typo(v))
            else:
                extra.append(v.replace(".", "_") if "@" in v and random.random() < 0.4 else v)
        out.update(extra)
        return list(out)

    clusters = []
    for _ in range(n_clusters):
        # choose a canonical first with nickname coverage bias
        first = random.choice(list(nick.keys()))
        last = random.choice(surnames)
        cand = gen_variants(first, last)
        k = random.randint(*size_range)
        cluster = random.sample(cand, k=min(k, len(cand)))
        clusters.append(cluster)

    return clusters


if __name__ == "__main__":
    clusters = generate_clusters(n_clusters=100, size_range=(5, 7), seed=123)
    print(f"Generated {len(clusters)} clusters. Example:")

    cp = IdentityClusteringModel()
    cp.fit(clusters)
    cp.save(Path("name_cluster.model"))
    del cp
    cp = IdentityClusteringModel.load(Path("name_cluster.model"))

    # Later once trained:
    test_identities = [
        # 1. Alice Henderson
        "Alice Henderson",
        "A. Henderson",
        "Alice J Henderson",
        "Henderson, Alice",
        "<alice.henderson@university.edu>",

        # 2. Daniel Price
        "Daniel Price",
        "Dan Price",
        "D. Price",
        "Price, Daniel",
        "d.price@consulting.co",
        "Managing Director <d.price@consulting.co>",

        # 3. Sarah Beaumont
        "Sarah Beaumont",
        "S. Beaumont",
        "Beaumont, Sarah",
        "Sara Beaumont",
        "sarah.beaumont@gmail.com",
        "Vice-Chancellor <sarah.beaumont@gmail.com>"
    ]
    clusters = cp.cluster(test_identities, threshold=0.95)
    for c in clusters:
        print(c)
