#!/usr/bin/env python3
"""
Generate synthetic mbox data:
- 10 distinct people (names + emails)
- 3 mailbox owners -> 3 .mbox files
- 50 unique messages total
- Messages appear in each owner's mbox if they were sender or recipient
- Includes threads with Message-ID / In-Reply-To / References
"""

from __future__ import annotations
import argparse
import random
from pathlib import Path
from datetime import datetime, timedelta, timezone
import mailbox
import copy
from email.message import EmailMessage
from email.utils import formataddr, make_msgid, format_datetime

# -----------------------
# Config & helpers
# -----------------------

SUBJECT_TEMPLATES = [
    "Project update: {topic}",
    "Re: {topic}",
    "Meeting notes – {topic}",
    "Draft proposal: {topic}",
    "Quick question about {topic}",
    "Follow-up on {topic}",
    "Action items for {topic}",
    "FYI: {topic}",
    "Status: {topic}",
    "Schedule for {topic}",
]

TOPICS = [
    "the mating dances of jumping spiders",
    "hyperbolic geometry in crochet art",
    "the history of forged medieval manuscripts",
    "cicada brood emergence cycles",
    "the physics of butter on hot toast",
    "language games in Wittgenstein's later work",
    "archival preservation of magnetic tape",
    "samizdat culture in the Soviet Union",
    "operational semantics of Lisp macros",
    "terraforming proposals for Venus"
]

BODIES = [
    "Sharing a quick update on {topic}. See details below.",
    "Please review the attached notes for {topic}.",
    "Circling back on {topic}. Any blockers?",
    "Thanks for the input on {topic}. Next steps inline.",
    "Here are the action items for {topic}.",
    "Following up on {topic}.",
    "Let’s finalize the plan for {topic} by EOD.",
    "Summary of decisions related to {topic}.",
    "Questions/comments about {topic} welcome.",
    "I’ve added comments to the doc for {topic}.",
]


def randsyllables(n=3) -> str:
    consonants = "bcdfghjklmnpqrstvwxyz"
    vowels = "aeiou"
    s = []
    for _ in range(n):
        s.append(random.choice(consonants))
        s.append(random.choice(vowels))
    return "".join(s)


def rand_topic() -> str:
    return random.choice(TOPICS)


def rand_subject(topic: str) -> str:
    return random.choice(SUBJECT_TEMPLATES).format(topic=topic)


def rand_body(topic: str) -> str:
    return random.choice(BODIES).format(topic=topic)


NAME_MENTION_TEMPLATES = [
    "As {name} mentioned earlier, we should review this.",
    "Looping in {name} for awareness (no action needed).",
    "Refer back to {name}'s notes on this.",
    "{name} raised a related point in the last meeting.",
    "We should check with {name} before finalizing.",
]


def maybe_add_name_mentions(
    body: str,
    all_people: list[dict],
    involved_emails: set[str],
    chance: float = 0.35,
) -> str:
    """
    Occasionally append one or two people's names to the body text,
    even if they are not recipients. Only picks from people not
    already involved (sender/to/cc).
    """
    try:
        if random.random() >= chance:
            return body
        candidates = [p for p in all_people if p["email"].lower() not in involved_emails]
        if not candidates:
            return body
        k = min(2, len(candidates))
        picks = random.sample(candidates, k=random.randint(1, k))
        extra_lines = []
        for p in picks:
            extra_lines.append(random.choice(NAME_MENTION_TEMPLATES).format(name=p["name"]))
        return body + "\n" + "\n".join(extra_lines)
    except Exception:
        # Be conservative; never break message generation
        return body


def mk_people() -> list[dict]:
    # 10 deterministic-ish people
    seeds = [
        ("Ada Lovelace", "ada.lovelace"),
        ("Grace Hopper", "grace.hopper"),
        ("Alan Turing", "a.turing"),
        ("John von Neumann", "jvn"),
        ("Hedy Lamarr", "hedy.lamarr"),
        ("Donald Knuth", "donald.knuth"),
        ("Ray Kurzweil", "ray.kurzweil"),
        ("Linus Torvalds", "linus.torvalds"),
        ("Guido van Rossum", "guido.van.rossum"),
        ("Moxie Marlinspike", "marlinspike")
    ]
    domains = ["example.org", "example.com"]
    people = []
    for i, (name, local) in enumerate(seeds):
        domain = domains[i % len(domains)]
        email = f"{local}@{domain}"
        people.append({"name": name, "email": email})
    return people


def format_person(p: dict) -> str:
    return formataddr((p["name"], p["email"]))


# -----------------------
# Message fabricator
# -----------------------

def build_message(
    sender: dict,
    to_list: list[dict],
    cc_list: list[dict] | None,
    dt: datetime,
    subject: str,
    body: str,
    msg_id: str | None = None,
    in_reply_to: str | None = None,
    references: list[str] | None = None,
    attachments: list[tuple[str, str]] | None = None,
) -> EmailMessage:
    msg = EmailMessage()
    msg["From"] = format_person(sender)
    msg["To"] = ", ".join(format_person(p) for p in to_list)
    if cc_list:
        msg["Cc"] = ", ".join(format_person(p) for p in cc_list)
    msg["Date"] = format_datetime(dt.astimezone(timezone.utc))
    msg["Subject"] = subject
    # Threading headers
    msg["Message-ID"] = msg_id or make_msgid(domain=sender["email"].split("@", 1)[-1])
    if in_reply_to:
        msg["In-Reply-To"] = in_reply_to
    if references:
        msg["References"] = " ".join(references)

    # A little footer noise to simulate signatures
    footer = (
        "\n\n-- \n"
        f"{sender['name']}\n"
        f"{sender['email']}\n"
        "Confidential – internal only."
    )
    msg.set_content(body + footer)

    # Optional simple text attachments (.txt or .md)
    if attachments:
        for fname, text in attachments:
            subtype = "markdown" if fname.lower().endswith(".md") else "plain"
            # For str payloads, EmailMessage routes to set_text_content, which doesn't accept 'maintype'.
            # So only pass subtype and filename.
            msg.add_attachment(text, subtype=subtype, filename=fname)

    return msg


# -----------------------
# Generation plan
# -----------------------

def generate_messages(people: list[dict], owners: list[dict], total: int = 50, seed: int = 42) -> list[EmailMessage]:
    random.seed(seed)

    # --- How many special messages do we force in? ---
    num_broadcast = 3  # emails sent to all 3 owners from one sender
    num_owner_to_owners = random.choice([1, 2])  # one or two from one owner to the other two
    forced_total = num_broadcast + num_owner_to_owners

    base_total = max(0, total - forced_total)

    # --- Helper to maybe add simple attachments ---
    def maybe_attachments(force: bool = False) -> list[tuple[str, str]] | None:
        if force:
            # Guaranteed one attachment
            if random.random() < 0.5:
                return [(f"notes_{randsyllables(2)}.txt", "Quick notes attached.\n- Item A\n- Item B\n")]
            else:
                return [(f"summary_{randsyllables(2)}.md", "# Summary\n\nThis is a short markdown attachment.\n")]
        # Otherwise, small chance to include 1 attachment
        if random.random() < 0.2:
            return [(f"readme_{randsyllables(2)}.txt", "Hello world from attachment.\n")]
        return None

    # --- Build ordinary threads/singletons as before, but only up to base_total ---
    threads = []
    remaining = base_total

    # Create up to 5 threads (each 4–6 messages) and fill the remainder with singletons
    for _ in range(5):
        length = random.randint(4, 6)
        length = min(length, remaining)
        if length <= 0:
            break
        thread_msgs = []
        topic = rand_topic()
        participants = random.sample(people, k=random.randint(3, 5))
        sender = random.choice(participants)
        recipients = [p for p in participants if p != sender]
        dt0 = datetime.now(timezone.utc) - timedelta(days=random.randint(1, 40), hours=random.randint(0, 12))
        root_msg_id = make_msgid(domain=sender["email"].split("@", 1)[-1])

        to_list = recipients[: random.randint(1, len(recipients))]
        cc_list = (
            recipients[random.randint(0, len(recipients)-1):]
            if len(recipients) > 2 and random.random() < 0.5
            else None
        )
        involved = {sender["email"].lower()} | {p["email"].lower() for p in to_list} | ({p["email"].lower() for p in cc_list} if cc_list else set())
        body0 = rand_body(topic)
        body0 = maybe_add_name_mentions(body0, people, involved)

        msg0 = build_message(
            sender=sender,
            to_list=to_list,
            cc_list=cc_list,
            dt=dt0,
            subject=rand_subject(topic),
            body=body0,
            msg_id=root_msg_id,
            attachments=maybe_attachments(force=False),
        )
        thread_msgs.append(msg0)

        refs = [root_msg_id]
        current_dt = dt0
        for i in range(1, length):
            current_dt += timedelta(hours=random.randint(1, 48))
            reply_sender = random.choice(participants)
            others = [p for p in participants if p != reply_sender]
            in_reply_to = thread_msgs[-1]["Message-ID"]
            to_list = [random.choice(others)]
            cc_list = (random.sample(others, k=min(len(others), random.randint(0, 2)))) or None
            involved = {reply_sender["email"].lower()} | {p["email"].lower() for p in to_list} | ({p["email"].lower() for p in cc_list} if cc_list else set())
            body_i = rand_body(topic) + f"\n\n(Reply {i} in thread)"
            body_i = maybe_add_name_mentions(body_i, people, involved)

            msg_i = build_message(
                sender=reply_sender,
                to_list=to_list,
                cc_list=cc_list,
                dt=current_dt,
                subject="Re: " + rand_subject(topic),
                body=body_i,
                in_reply_to=in_reply_to,
                references=refs[-10:],
                attachments=maybe_attachments(force=False),
            )
            refs.append(msg_i["Message-ID"])
            thread_msgs.append(msg_i)

        threads.append(thread_msgs)
        remaining -= len(thread_msgs)

    singles = []
    for _ in range(remaining):
        participants = random.sample(people, k=random.randint(2, 4))
        sender = random.choice(participants)
        recipients = [p for p in participants if p != sender]
        topic = rand_topic()
        dt = datetime.now(timezone.utc) - timedelta(days=random.randint(1, 90), hours=random.randint(0, 23))
        to_list = recipients[: random.randint(1, len(recipients))]
        cc_list = (
            recipients[random.randint(0, len(recipients)-1):]
            if len(recipients) > 2 and random.random() < 0.5
            else None
        )
        involved = {sender["email"].lower()} | {p["email"].lower() for p in to_list} | ({p["email"].lower() for p in cc_list} if cc_list else set())
        body_s = rand_body(topic)
        body_s = maybe_add_name_mentions(body_s, people, involved)

        # Randomly decide if this singleton is a reply or forward
        is_reply = random.random() < 0.2
        is_forward = not is_reply and random.random() < 0.1
        if is_reply:
            subject = "Re: " + rand_subject(topic)
            body_s = "On some date, someone wrote:\n> Example previous message.\n\n" + body_s
        elif is_forward:
            subject = "Fwd: " + rand_subject(topic)
            body_s = "---------- Forwarded message ---------\nFrom: someone@example.com\nDate: some date\nSubject: forwarded subject\nTo: someone@example.com\n\n" + body_s
        else:
            subject = rand_subject(topic)

        msg = build_message(
            sender=sender,
            to_list=to_list,
            cc_list=cc_list,
            dt=dt,
            subject=subject,
            body=body_s,
            attachments=maybe_attachments(force=False),
        )
        singles.append(msg)

    all_msgs: list[EmailMessage] = [m for thread in threads for m in thread] + singles

    # --- Inject special messages ---
    # 1) Broadcasts to all 3 owners from a single non-owner sender
    non_owner_candidates = [p for p in people if p not in owners]
    b_sender = random.choice(non_owner_candidates) if non_owner_candidates else random.choice(people)
    topic_b = rand_topic()
    base_dt = datetime.now(timezone.utc) - timedelta(days=random.randint(0, 5))
    for i in range(num_broadcast):
        dt = base_dt + timedelta(hours=i)
        body = rand_body(topic_b) + "\n\n(Broadcast to all three owners)"
        attachments = maybe_attachments(force=True)  # ensure an attachment on broadcasts
        msg_b = build_message(
            sender=b_sender,
            to_list=owners,  # To: all owners
            cc_list=None,
            dt=dt,
            subject=rand_subject(topic_b),
            body=body,
            attachments=attachments,
        )
        all_msgs.append(msg_b)

    # 2) One or two emails from one owner to the other two owners
    owner_sender = random.choice(owners)
    other_two = [o for o in owners if o != owner_sender]
    topic_o = rand_topic()
    dt_o = datetime.now(timezone.utc) - timedelta(days=random.randint(0, 7), hours=random.randint(0, 8))
    for i in range(num_owner_to_owners):
        dt = dt_o + timedelta(hours=i * 2)
        body = rand_body(topic_o) + "\n\n(Owner-to-owners message)"
        attachments = maybe_attachments(force=True if i == 0 else False)
        msg_o = build_message(
            sender=owner_sender,
            to_list=other_two,
            cc_list=None,
            dt=dt,
            subject=rand_subject(topic_o),
            body=body,
            attachments=attachments,
        )
        all_msgs.append(msg_o)

    # Shuffle/Order by date for realism
    all_msgs.sort(key=lambda m: m["Date"])
    return all_msgs


# -----------------------
# Mailbox writing
# -----------------------

def write_mailboxes(messages: list[EmailMessage], owners: list[dict], outdir: Path) -> list[Path]:
    """
    Each message is written to any owner's mbox if that owner is the sender
    or is present in To/Cc (simulating Sent/Inbox copies across mailboxes).
    """
    outdir.mkdir(parents=True, exist_ok=True)
    mboxes = []
    handles: dict[str, mailbox.mbox] = {}

    try:
        # Open mboxes
        for owner in owners:
            fname = outdir / f"mailbox_{owner['email'].split('@', 1)[0]}.mbox"
            mbox = mailbox.mbox(fname)
            mbox.lock()
            handles[owner["email"]] = mbox
            mboxes.append(fname)

        # Write messages
        for msg in messages:
            # Collect all addresses in To/Cc for quick membership test
            recips = set()
            for hdr in ("To", "Cc"):
                if msg.get(hdr):
                    try:
                        for addr in msg[hdr].addresses:
                            recips.add(addr.addr_spec.lower())
                    except Exception:
                        # Fallback: naive split
                        recips.update(a.strip().split("<")[-1].rstrip(">").lower() for a in msg[hdr].split(","))
            # Sender address
            try:
                sender_addr = msg["From"].addresses[0].addr_spec.lower()
            except Exception:
                # Fallback parse
                part = msg["From"].split("<")[-1].rstrip(">")
                sender_addr = part.strip().lower()

            # Copy message into each owner's mbox if they are involved
            for owner in owners:
                owner_addr = owner["email"].lower()
                if owner_addr == sender_addr or owner_addr in recips:
                    # Add a marker so you can tell which mailbox this copy lives in
                    m_copy = copy.deepcopy(msg)
                    m_copy["X-Original-Mailbox"] = owner_addr
                    handles[owner_addr].add(m_copy)

        # Flush
        for mbox in handles.values():
            mbox.flush()
    finally:
        for mbox in handles.values():
            try:
                mbox.unlock()
                mbox.close()
            except Exception:
                pass

    return mboxes


# -----------------------
# Main
# -----------------------

def main():
    ap = argparse.ArgumentParser(description="Generate synthetic mbox files for 3 mailboxes / 10 people / 50 emails.")
    ap.add_argument("--outdir", type=Path, default=Path("./synthetic_mboxes"), help="Output directory for .mbox files")
    ap.add_argument("--total", type=int, default=50, help="Number of unique emails to generate")
    ap.add_argument("--seed", type=int, default=42, help="Random seed for reproducibility")
    args = ap.parse_args()

    people = mk_people()
    # Pick 3 mailbox owners from the 10 people
    owners = random.Random(args.seed).sample(people, k=3)

    messages = generate_messages(people, owners, total=args.total, seed=args.seed)

    mboxes = write_mailboxes(messages, owners, args.outdir)

    print(f"Created {len(mboxes)} mbox files in {args.outdir.resolve()}:")
    for p in mboxes:
        print(f"  - {p}")
    print(f"Generated {len(messages)} unique messages involving 10 people.")
    print("Mailbox owners:")
    for o in owners:
        print(f"  - {o['name']} <{o['email']}>")


if __name__ == "__main__":
    main()