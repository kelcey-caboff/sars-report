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

const clustersBox = document.getElementById('clustersBox');
const clusterSelect = document.getElementById('clusterSelect');
const clusterResults = document.getElementById('clusterResults');
let currentJobId = null;
let clustersCache = [];

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
      loadClusters(jobId);
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

async function loadClusters(jobId) {
  try {
    const res = await fetch(`/index/result?job_id=${encodeURIComponent(jobId)}`);
    if (!res.ok) return;
    const data = await res.json();
    const clusters = data.clusters || [];
    if (!clusters.length) return;
    clustersCache = clusters;
    clustersBox.style.display = '';
    clusterSelect.innerHTML = '';
    for (const c of clusters) {
      const opt = document.createElement('option');
      const count = Number.isFinite(c.size)
        ? c.size
        : (Array.isArray(c.members) ? c.members.length : 0);
      opt.value = c.id;
      opt.textContent = `${c.label || c.id}`;
      clusterSelect.appendChild(opt);
    }
    if (clusterSelect.value) {
      showCluster(jobId, clusterSelect.value);
    }
  } catch (e) {
    console.error(e);
  }
}

async function showCluster(jobId, clusterId) {
  if (!clusterId) return;
  const res = await fetch(`/index/cluster?job_id=${encodeURIComponent(jobId)}&cluster_id=${encodeURIComponent(clusterId)}`);
  if (!res.ok) {
    clusterResults.textContent = 'Failed to load cluster.';
    return;
  }
  const data = await res.json();
  const posts = data.postings || [];
  const frag = document.createDocumentFragment();
  for (const p of posts) {
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
      <div class="content"><pre>${escapeHtml(body)}</pre></div>
    `;
    frag.appendChild(box);
  }
  clusterResults.innerHTML = '';
  clusterResults.appendChild(frag);
}

clusterSelect?.addEventListener('change', () => showCluster(currentJobId, clusterSelect.value));
