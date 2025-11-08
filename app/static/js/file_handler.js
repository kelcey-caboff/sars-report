const fileInput = document.getElementById('fileInput');
const fileName  = document.getElementById('fileName');
const form      = document.getElementById('uploadForm');
const statusEl  = document.getElementById('status');
const results   = document.getElementById('results');
const resetBtn  = document.getElementById('resetBtn');
const jobIdInput = document.getElementById('jobIdInput');

function escapeHtml(s) {
  if (s == null) return '';
  return String(s)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');
}

fileInput.addEventListener('change', () => {
  if (!fileInput.files.length) {
    fileName.textContent = 'No files selected';
  } else if (fileInput.files.length === 1) {
    fileName.textContent = fileInput.files[0].name;
  } else {
    fileName.textContent = `${fileInput.files.length} files selected`;
  }
});

resetBtn.addEventListener('click', () => {
  form.reset();
  fileName.textContent = 'No files selected';
  statusEl.textContent = '';
  results.classList.add('is-hidden');
});

form.addEventListener('submit', async (e) => {
  e.preventDefault();
  if (!fileInput.files.length) return;
  results.classList.add('is-hidden');
  statusEl.textContent = 'Uploading…';

  const fd = new FormData();
  for (const f of fileInput.files) {
    fd.append('files', f);
  }

  try {
    const up = await fetch('/upload', { method: 'POST', body: fd });
    if (!up.ok) {
      const errText = await up.text();
      throw new Error(`Upload failed: ${up.status} ${errText}`);
    }
    //const data = await up.json();


    statusEl.textContent = 'Done.';
    window.location = '/';

    results.classList.remove('is-hidden');
  } catch (err) {
    statusEl.textContent = err.message || String(err);
    window.location = '/';
  }
});

const startIndexBtn = document.getElementById('startIndexBtn');
const indexStatus  = document.getElementById('indexStatus');
let pollTimer = null;

let currentJobId = null;
let clustersCache = [];

// Allow other modules (r.g. identifier editor) to set the job id used by Finder
window.setCurrentJobId = (id) => { currentJobId = id; };

async function pollStatus(jobId) {
  try {
    const res = await fetch(`/index/status?job_id=${encodeURIComponent(jobId)}`);
    if (!res.ok) {
      indexStatus.textContent = `Status check failed: ${res.status}`;
      clearInterval(pollTimer);
      startIndexBtn.disabled = false;
      return;
    }
    const data = await res.json();
    if (data.status === 'running') {
      const p = data.progress || { processed: 0, total: 0 };
      indexStatus.textContent = `Indexing… ${p.processed}/${p.total}`;
    } else if (data.status === 'done') {
      const p = data.progress || { processed: 0, total: 0 };
      indexStatus.textContent = `Indexing complete. Processed ${p.processed} file(s).`;
      clearInterval(pollTimer);
      startIndexBtn.disabled = false;
      currentJobId = jobId;
      if (jobIdInput) jobIdInput.value = jobId;
      try { await window.loadIdentifiers(jobId); } catch (e) {}
      loadFinder(jobId);
    } else if (data.status === 'error') {
      indexStatus.textContent = `Indexing error: ${data.error || 'Unknown error'}`;
      clearInterval(pollTimer);
      startIndexBtn.disabled = false;
    } else {
      indexStatus.textContent = `Status: ${data.status}`;
    }
  } catch (e) {
    indexStatus.textContent = `Status error: ${e.message || e}`;
    clearInterval(pollTimer);
    startIndexBtn.disabled = false;
  }
}

startIndexBtn?.addEventListener('click', async () => {
  startIndexBtn.disabled = true;
  indexStatus.textContent = 'Starting…';
  try {
    const res = await fetch('/index/start', { method: 'POST' });
    if (!res.ok) {
      const t = await res.text();
      throw new Error(`Start failed: ${res.status} ${t}`);
    }
    const data = await res.json();
    const jobId = data.job_id;
    indexStatus.textContent = `Job ${jobId} started…`;
    if (pollTimer) clearInterval(pollTimer);
    pollTimer = setInterval(() => pollStatus(jobId), 500);
  } catch (e) {
    indexStatus.textContent = e.message || String(e);
    startIndexBtn.disabled = false;
  }
});

// =========================
// Email Finder UI & logic
// =========================
const finderBox = document.getElementById('finderBox');
const finderTbody = document.getElementById('finderTbody');
const finderApplyBtn = document.getElementById('finderApplyBtn');
const finderClearBtn = document.getElementById('finderClearBtn');
const finderStatus = document.getElementById('finderStatus');
const finderResults = document.getElementById('finderResults');
const finderResultsInner = document.getElementById('finderResultsInner');


function makeTriStateSelect(defaultVal = 'any') {
  const sel = document.createElement('select');
  // we wrap select in a div.select for Bulma, but keep it simple here
  sel.innerHTML = `
    <option value="any">Any</option>
    <option value="yes">Yes</option>
    <option value="no">No</option>
  `;
  sel.value = defaultVal;
  return sel;
}

// Helper to update Bulma wrapper styling based on select value
function updateWrapperState(sel) {
  const wrap = sel._wrap;
  if (!wrap) return;
  // Ensure the native <select> never carries Bulma colour classes
  sel.classList.remove('is-primary', 'is-danger');

  // Remove previous Bulma modifiers from the wrapper
  wrap.classList.remove('is-primary', 'is-danger');

  // Apply based on value to the WRAPPER ONLY
  if (sel.value === 'yes') {
    wrap.classList.add('is-primary');
  } else if (sel.value === 'no') {
    wrap.classList.add('is-danger');
  }
}

function wrapSelect(selectEl) {
  const wrap = document.createElement('div');
  wrap.className = 'select is-small';
  wrap.appendChild(selectEl);
  selectEl._wrap = wrap;
  updateWrapperState(selectEl);
  selectEl.addEventListener('change', () => updateWrapperState(selectEl));
  return wrap;
}

async function loadFinder(jobId) {
  // Always show the card so users see status/errors
  finderBox.style.display = '';
  finderStatus.textContent = 'Loading clusters…';
  finderResults.style.display = 'none';
  finderTbody.innerHTML = '';

  try {
    const res = await fetch(`/index/identifiers?job_id=${encodeURIComponent(jobId)}`);
    if (!res.ok) {
      finderStatus.textContent = `Failed to load clusters: ${res.status}`;
      return;
    }

    const data = await res.json();

    // Accept either an array or a map of clusters, just in case
    let clusters = data.clusters || [];
    if (!Array.isArray(clusters) && clusters && typeof clusters === 'object') {
      clusters = Object.entries(clusters).map(([id, label]) => ({ id, label }));
    }

    if (!clusters.length) {
      finderStatus.textContent = 'No clusters found for this job yet. Finish indexing or load identifiers.';
      return;
    }

    clustersCache = clusters;

    // Build the table rows
    const roles = ['from', 'to', 'body'];
    const frag = document.createDocumentFragment();

    for (const c of clusters) {
      const tr = document.createElement('tr');
      tr.dataset.cid = c.id;

      const tdName = document.createElement('td');
      tdName.textContent = c.label || c.id;
      tr.appendChild(tdName);

      for (const role of roles) {
        const td = document.createElement('td');
        const sel = makeTriStateSelect('any');
        sel.dataset.role = role;
        td.appendChild(wrapSelect(sel));
        tr.appendChild(td);
      }

      frag.appendChild(tr);
    }

    finderTbody.innerHTML = '';
    finderTbody.appendChild(frag);
    finderStatus.textContent = '';  // ready
  } catch (e) {
    console.error(e);
    finderStatus.textContent = e.message || String(e);
  }
}

function collectRulesFromTable() {
  const rules = [];
  for (const tr of finderTbody.querySelectorAll('tr')) {
    const cid = tr.dataset.cid;
    if (!cid) continue;
    const sels = tr.querySelectorAll('select');
    const vals = { from: 'any', to: 'any', body: 'any' };
    sels.forEach(sel => {
      const role = sel.dataset.role;
      if (role && (sel.value === 'yes' || sel.value === 'no' || sel.value === 'any')) {
        vals[role] = sel.value;
      }
    });
    // Only include a rule if at least one role is not 'any'
    if (vals.from !== 'any' || vals.to !== 'any' || vals.body !== 'any') {
      rules.push({ cluster_id: cid, from: vals.from, to: vals.to, body: vals.body });
    }
  }
  return rules;
}

function renderFinderResults(items) {
  const frag = document.createDocumentFragment();
  for (const p of items) {
    const box = document.createElement('div');
    box.className = 'box';
    const from = p.from || '';
    const to = p.to || '';
    const subj = p.subject || '(no subject)';
    const date = p.date || '';
    const body = p.body || '';
    box.innerHTML = `
      <p><strong>FROM:</strong> ${escapeHtml(from)}</p>
      <p><strong>TO:</strong> ${escapeHtml(to)}</p>
      <p><strong>SUBJECT:</strong> ${escapeHtml(subj)}</p>
      <p><strong>DATE:</strong> ${escapeHtml(date)}</p>
      <div class="content"><pre>${escapeHtml(body)}</pre></div>`;
    frag.appendChild(box);
  }
  finderResultsInner.innerHTML = '';
  finderResultsInner.appendChild(frag);
}

finderApplyBtn?.addEventListener('click', async () => {
  if (!currentJobId) {
    finderStatus.textContent = 'No job loaded yet.';
    return;
  }
  const rules = collectRulesFromTable();
  finderStatus.textContent = '';
  finderResults.style.display = 'none';

  if (!rules.length) {
    finderStatus.textContent = 'Select at least one Yes/No to apply filters.';
    return;
  }

  try {
    const res = await fetch('/index/search', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ job_id: currentJobId, rules })
    });
    if (!res.ok) {
      const t = await res.text();
      throw new Error(`Search failed: ${res.status} ${t}`);
    }
    const data = await res.json();
    const items = data.matches || [];
    if (!items.length) {
      finderStatus.textContent = 'No matches found.';
      finderResults.style.display = 'none';
      return;
    }
    renderFinderResults(items);
    finderResults.style.display = '';
  } catch (e) {
    finderStatus.textContent = e.message || String(e);
    finderResults.style.display = 'none';
  }
});

finderClearBtn?.addEventListener('click', () => {
  for (const sel of finderTbody.querySelectorAll('select')) {
    sel.value = 'any';
    updateWrapperState(sel);
  }
  finderStatus.textContent = '';
  finderResults.style.display = 'none';
  finderResultsInner.innerHTML = '';
});

// Expose for use after indexing completes
window.loadFinder = loadFinder;
