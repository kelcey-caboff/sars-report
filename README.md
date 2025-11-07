# SARS Report

Analyse `./mbox` email archives and generate a navigable report suitable for writing **Subject Access Request (SAR)** responses. The app ingests mailboxes, clusters identities (names, email addresses, aliases), and lets you filter messages using precise AND/NOT logic across roles (sender, recipients, body mention). It is designed for clarity, auditability, and speed on real-world messy mail data.

## Highlights

* Drag-and-drop ingestion for one or more `.mbox` files (created to disk, no giant in-memory buffers).
* One-click indexing to extract headers/body, normalise fields, and build identity clusters.
* Identifier and Cluster Editor to refine buckets and "gold" names (the first/labelled name per cluster).
* Email Finder shows a table of identities with tri-state filters (Any/Yes/No) per role:
  * From: is this person the sender?
  * To (+CC/BCC): are they among the recipients?
  * Body: are they mentioned in the message text?
  * Filters are **boolean AND** across all chosen conditions; **No** acts as **boolean NOT**.
* Readable results displayed in message cards with From/To/Date/body, sorted oldest to newest.

## How it works (architecture)

### Backend

A FastAPI app (`app.py`) serves a lightweight API and the HTML UI. It orchestrates an indexing pipeline (`index_emails.py`) that emits structured artefacts per job:

* `parts.json`: normalised message fields keyed by `part_id`.
* `cluster_index.json`: clusters with `label`, `members`, and optional `postings`.
* `id_to_cluster.json`: reverse lookup idenfitier > cluster.

### Indexing

Indexing is executed in a background task when you click "Start indexing"; progress is polled until it is complete.

### Identity clustering

Initial buckets are inferred from names/emails incorporating  SpaCy named entity recognition and using a custom trained machine learning process. You can merge/split and relabel "gold" names in the browser, then persist the changes via `/index/clusters/update`.

## Search model

The email finder builds a set of rules and sends them to `POST /index/search`. For each cluster, the server computes role-specific sets of matching `part_ids` (from/to/body) by unioning member postings. The final result is the intersection across rules, with **No** removing items from the candidate set.

> The UI runs on Bulma CSS with a small amount of vanilla JS. Endpoints live under `/index/*`. 

## Quick start

### 1. Install and run

```bash
# python  3.10+ recommended
python -m venv .venv
source .venv/bin/activate # Windows: .venv\Scripts\activate
pip install --upgrade pip
pip install -r requirements.txt

# The process expects an Apache Tika server to be running at http://localhost:9998/

uvicorn app:app --reload --port 8000
```

Open <http://localhost:8000> in your browser.

### 2. Upload and index

1. User **Upload .mbox files** to add one or more mailboxes.
2. Click **Start indexing**. Progress is show live. When complete, the Identifier and Cluster Editor and email finder are ready.

## Using the Identifier and Cluster Editor

* **Load**:  If you’ve just indexed, the job ID is auto-populated; otherwise paste any previous `job_id`.
* **Review buckets**: Each bucket is a cluster;. The first item is the gold name (used as the display label).
* **Drag & drop**: Move identifiers between buckets. Create a **+ New bucket** to split things out.
* **Relabel**: Change the cluster label to a canonical name (e.g., “John Smith”).
* **Save**: Review the diff, confirm, and the server recomputes any affected postings for you.

These edits are purely structural. The original `parts.json` (message content) remains unchanged for auditability.

## Using the Email Finder

When indexing is complete you will see **Email Finder**:

* Rows = Clusters, your curated identities)
* Columns = Roles, with tri-state controls:
  * Any (ignore this role),
  * Yes (must be in this role),
  * No (must not be in this role)
* Click **Apply filters** to fetch the matches. Examples:
  * "From Tony AND NOT to Claire"
  * "Mentions Bob in body AND NOT in recipients AND NOT from him"

No emails are displayed until you apply at least one filter.

## API surface for integrators

* `POST /upload`: stream-saves files and creates sidecar metadata.
* `POST /index/start`: begin a new background indexing job, returns `job_id`.
* `GET /index/status?job_id=...`: poll job state and progeress.
* `GET /index/result?job_id=...`: list clusters (id, label, size).
* `GET /index/identifiers?job_id=...`: identifiers and cluster membership.
* `POST /index/clusters/update`: apply moves/relabels/creates, server recomputes postings when possible.
* `GET /index/cluster?job_id=...&cluster_id=...`: messages for one cluster (oldest to newest).
* `POST /index/search`: Email Finder query (rules with from/to/body tri-states), returns normalised message cards.

## Data and Privacy

* Files and derived JSON artefacts are stored under a per-run directory (e.g. `/data/index_jobs/\<job_id\>/) and deleted on close.
* Indexing creates human-readable JSON to support inspection and downstream export. There are no external uploads.

## Roadmap

* Pagination and export (CSV/JSON/Markdown) for finder results.
* Deduplication helpers (e.g. exact hash match, thread grouping).
* Snippet highlighting for matched identities in body.
* Auto-redaction or anonymisation of identifiers from the identity editor (e.g. select identifiers to be replaced with either black squares or a unique anonymous identifier like `abc132-312def`).