import random
import string
from cluster_names import IdentityClusteringModel
from pathlib import Path

NICKNAME_MAP = {
    "Andrew": ["Andy", "Drew"], "Anthony": ["Tony"], "Alexander": ["Alex", "Xander"],
    "Benjamin": ["Ben", "Benny"], "Charles": ["Charlie", "Chuck"], "Christopher": ["Chris", "Topher"],
    "Daniel": ["Dan", "Danny"], "Elizabeth": ["Liz", "Beth", "Eliza", "Lizzy"],
    "Joanna": ["Jo", "Joey"], "Jonathan": ["Jon", "Jonny"], "Joseph": ["Joe", "Joey", "Jo"],
    "Katherine": ["Kate", "Katie", "Kathy", "Kat"], "Margaret": ["Maggie", "Meg", "Peggy"],
    "Matthew": ["Matt"], "Michael": ["Mike", "Mikey"], "Nicholas": ["Nick", "Nicky"],
    "Patricia": ["Pat", "Patty"], "Rebecca": ["Becky", "Becca"], "Richard": ["Rich", "Rick", "Ricky"],
    "Robert": ["Rob", "Bob", "Bobby"], "Samuel": ["Sam", "Sammy"], "Thomas": ["Tom", "Tommy"],
    "William": ["Will", "Bill", "Billy"], "Jo": ["Joanna"], "Andy": ["Andrew"],
    # Common names with no nicknames for diversity
    "Abigail": [], "Brian": [], "Catherine": [], "Derek": [], "Emily": [], "Frances": [],
    "George": [], "Hannah": [], "Ian": [], "Jack": [], "Karen": [], "Laura": [],
    "Martin": [], "Nina": [], "Oscar": [], "Paula": [], "Quentin": [], "Rachel": [],
    "Steven": [], "Tina": [], "Ursula": [], "Victor": [], "Wendy": [], "Xavier": [],
    "Yvonne": [], "Zachary": [], "Helen": [], "Isabel": [], "Julian": [], "Kevin": [],
    "Liam": [], "Megan": [], "Natalie": [], "Oliver": [], "Peter": [], "Quincy": [],
    "Ruth": [], "Sophie": [], "Trevor": [], "Uma": [], "Vera": [], "Walter": [],
    "Xenia": [], "Yara": [], "Zoe": []
}

#
# Expanded, globally diverse surnames:
surnames = [
    "Smith", "Jones", "Brown", "Taylor", "Wilson", "Johnson", "Clark", "Lewis", "Walker",
    "Hall", "Young", "Allen", "King", "Wright", "Scott", "Green", "Baker", "Adams", "Harris",
    "Roberts", "Michaels", "Moore", "Jackson", "Thompson", "Martin", "Nguyen", "Chen", "Patel",
    "Khan", "Singh", "Kim", "Park", "Tanaka", "Yamamoto", "Garcia", "Lopez", "Martinez", "Silva",
    "Hernandez", "Rossi", "Bianchi", "Kowalski", "Nowak", "Larsen", "Nielsen", "Johansson", "Ivanov",
    "Smirnov", "Popov", "Lee", "Zhang", "Wang", "Zhao", "Sato", "Ito", "Yamada", "Da Silva", "O'Neill"
]
titles = ["Mr", "Mrs", "Ms", "Miss", "Dr", "Prof", "Mx"]
domains = ["gmail.com", "hotmail.com", "yahoo.com", "example.com", "mail.com", "workplace.co", "department.gov.uk"]


def generate_clusters(n_clusters=100, size_range=(9, 12), seed=42):
    random.seed(seed)

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
        domain = random.choice(domains)
        if random.random() < 0.12:
            local += str(random.randint(1, 99))
        return f"{local}@{domain}"

    def initials(first, middle, last, dots=True):
        parts = [first[0], * ([middle[0]] if middle else []), last[0]]
        if dots:
            parts = [p+"." for p in parts]
        return " ".join(parts)

    # --------- Inject header variants for emails ---------
    def inject_header_variants(first, last, email_addr):
        variants = [
            f"{first} {last} <{email_addr}>",
            f"{random.choice(titles)} {last} <{email_addr}>",
            f"{email_addr}",
            f"{random.choice(['Professor', 'Dean', 'Head of Research'])} <{email_addr}>"
        ]
        return variants

    def gen_variants(first, last):
        middle = random.choice([None, "A", "B", "C", "D", "E", "J", "M"]) if random.random() < 0.4 else None
        pool = set()
        add = pool.add

        # canonical forms
        add(f"{first} {last}")
        if middle:
            add(f"{first} {middle} {last}")
        add(f"{last}, {first}")
        add(initials(first, middle, last, dots=True))
        add(f"{initials(first, middle, last, dots=True)} {last}")
        # Centralized email variant generation and header variants
        email_addr = email(first, last)
        add(email_addr)
        for v in inject_header_variants(first, last, email_addr):
            add(v)
        # Add challenging email formats
        alt_local_parts = [
            f"{first.lower()}{last[0].lower()}",
            f"{first[0].lower()}{last.lower()}",
            f"{first.lower()}_{last.lower()}",
            f"{first[0].lower()}.{last.lower()}",
            f"{last.lower()}.{first.lower()}",
            f"{first.lower()}.{last.lower()}99",
        ]
        for local in alt_local_parts:
            addr = f"{local}@{random.choice(domains)}"
            add(addr)
            for v in inject_header_variants(first, last, addr):
                add(v)
        add(f"{random.choice(titles)} {last}")
        add(f"{random.choice(titles)} {first[0]}. {last}")
        add(f"{first}.{last}")
        add(f"{first}{last}")
        add(f"{first.lower()} {last.lower()}")
        add(f"{first.upper()} {last.upper()}")
        add(f"{first[0]}. {last}")

        # nicknames
        for alt in NICKNAME_MAP.get(first, []):
            add(f"{alt} {last}")
            add(f"{last}, {alt}")
            # Centralized email variant generation and header variants for nicknames
            alt_email_addr = email(alt, last)
            add(alt_email_addr)
            for v in inject_header_variants(alt, last, alt_email_addr):
                add(v)
            add(f"{alt}.{last}")

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
        # Expanded international/edge-case first names:
        if random.random() < 0.4:
            first = random.choice([
                "Juan", "Li", "Sven", "Jörg", "Omar", "Fatima", "Raj", "Aiko", "László",
                "José", "Anna-Marie", "Mohamed", "Zahra", "Wei", "Satoshi", "Ines", "León", "Yusuf"
            ])
        else:
            first = random.choice(list(NICKNAME_MAP))
        last = random.choice(surnames)
        cand = gen_variants(first, last)
        k = random.randint(*size_range)
        cluster = random.sample(cand, k=min(k, len(cand)))
        clusters.append(cluster)

    return clusters


if __name__ == "__main__":
    # Generate purely synthetic training data:
    clusters = generate_clusters(n_clusters=1000, size_range=(7, 15), seed=123)
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
