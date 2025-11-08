
(() => {
  // ---- State & helpers ------------------------------------------------------
  const state = {
    buckets: [],        // [{id, label, items: [ident,...]}]
    originalMap: {},    // {ident -> original cluster_id}
    clusters: [],       // server clusters metadata
  };
  const byId = id => document.getElementById(id);

  const jobIdInput   = byId('jobIdInput');
  const loadBtn      = byId('loadBtn');
  const saveBtn      = byId('saveBtn');
  const addBucketBtn = byId('addBucketBtn');
  const undoBtn      = byId('undoBtn');
  const filterBox    = byId('idFilter');

  const wrap         = byId('editorBucketsWrap');
  const reviewPanel  = byId('reviewPanel');
  const diffBox      = byId('diffBox');
  const closeReview  = byId('closeReview');
  const confirmSave  = byId('confirmSave');

  let history = [];

  function pushHistory() {
    history.push(JSON.parse(JSON.stringify(state)));
    if (history.length > 50) history.shift();
  }

  // Build from API rows/clusters
  function buildBuckets(rows, clusters) {
    const labelById = new Map(clusters.map(c => [c.id, c.label || '']));
    const map = new Map(); // cid -> bucket
    const originalMap = {};
    for (const r of rows) {
      let b = map.get(r.cluster_id);
      if (!b) {
        b = { id: r.cluster_id, label: labelById.get(r.cluster_id) || '', items: [] };
        map.set(r.cluster_id, b);
      }
      b.items.push(r.identifier);
      originalMap[r.identifier] = r.cluster_id;
    }
    const buckets = Array.from(map.values());
    // put gold first if known
    for (const b of buckets) {
      if (b.label) {
        const i = b.items.indexOf(b.label);
        if (i > 0) b.items.splice(0, 0, ...b.items.splice(i,1));
      }
    }
    // sort by label for visual stability
    buckets.sort((a,b) => (a.label||'').localeCompare(b.label||''));
    return { buckets, originalMap };
  }

  // ---- Rendering ------------------------------------------------------------
  function render() {
    if (!state.buckets.length) { wrap.innerHTML = '<em>No identifiers loaded.</em>'; saveBtn.disabled = true; return; }
    saveBtn.disabled = false;

    wrap.innerHTML = '';
    const board = document.createElement('div');
    board.style.display = 'flex';
    board.style.flexWrap = 'wrap';
    wrap.appendChild(board);

    for (const b of state.buckets) {
      const col = document.createElement('section');
      col.className = 'box';
      col.style.margin = '.25rem';
      col.style.minWidth = '280px';
      col.style.maxWidth = '320px';
      col.dataset.cid = b.id;

      col.innerHTML = `
        <div class="field has-addons" style="align-items:center;">
          <p class="control is-expanded">
            <input class="input is-small bucket-label" data-cid="${b.id}" value="${b.label || ''}" placeholder="${b.id.slice(0,6)} label (optional)">
          </p>
          <p class="control"><a class="button is-small is-light del-bucket" data-cid="${b.id}">🗑</a></p>
        </div>
        <ul class="list" style="list-style:none; margin:0; padding:0 .25rem .25rem;">
          ${b.items.map((ident,i)=>`
            <li class="card" data-ident="${ident}" ${i===0?'data-gold="true"':''} style="margin:.35rem 0; padding:.4rem .5rem; display:flex; gap:.5rem; align-items:center;">
              <span class="has-text-warning">${i===0?'★':''}</span>
              <span class="is-family-monospace" style="word-break:break-word;">${ident}</span>
            </li>
          `).join('')}
        </ul>
      `;
      board.appendChild(col);

      // delete bucket: move items to first other bucket
      col.querySelector('.del-bucket')?.addEventListener('click', () => {
        pushHistory();
        const idx = state.buckets.indexOf(b);
        const members = b.items.slice();
        state.buckets.splice(idx,1);
        if (members.length) {
          const target = state.buckets[0] || createBucket();
          target.items.push(...members);
        }
        render(); wireDnD();
      });

      // label change
      col.querySelector('.bucket-label')?.addEventListener('input', (e) => {
        b.label = e.target.value;
      });
    }

    markGolds();
    wireDnD();
    applyFilter(filterBox.value);
  }

  function createBucket() {
    const id = 'new-' + Math.random().toString(36).slice(2,8);
    const b = { id, label:'', items:[] };
    state.buckets.push(b);
    return b;
  }

  function markGolds() {
    wrap.querySelectorAll('.list').forEach(ul => {
      ul.querySelectorAll('.card').forEach((el, idx) => {
        el.dataset.gold = (idx===0);
        el.querySelector('.has-text-warning').textContent = (idx===0) ? '★' : '';
      });
    });
  }

  function applyFilter(term) {
    term = (term||'').trim().toLowerCase();
    wrap.querySelectorAll('.card').forEach(card => {
      const t = card.textContent.toLowerCase();
      card.style.display = term && !t.includes(term) ? 'none' : '';
    });
  }

  filterBox?.addEventListener('input', () => applyFilter(filterBox.value));

  // ---- Drag & Drop via SortableJS ------------------------------------------
  function wireDnD() {
    // for each bucket list
    wrap.querySelectorAll('.list').forEach(listEl => {
      const cid = listEl.closest('section').dataset.cid;
      new Sortable(listEl, {
        group: 'ids',
        animation: 120,
        ghostClass: 'has-background-light',
        multiDrag: true,
        selectedClass: 'has-background-info-light',
        fallbackTolerance: 4,
        onSort: () => {
          // sync one bucket order
          const b = state.buckets.find(x => x.id === cid);
          b.items = [...listEl.querySelectorAll('.card')].map(el => el.dataset.ident);
          markGolds();
        },
        onAdd: () => {
          // resync all buckets from DOM
          syncFromDOM();
          markGolds();
        }
      });
    });
  }

  function syncFromDOM() {
    wrap.querySelectorAll('section.box').forEach(section => {
      const cid = section.dataset.cid;
      const b = state.buckets.find(x => x.id === cid);
      if (!b) return;
      b.items = [...section.querySelectorAll('.card')].map(el => el.dataset.ident);
    });
  }

  // ---- Loading & Saving -----------------------------------------------------
  async function load(jobId) {
    currentJobId = jobId;
    if (jobIdInput) jobIdInput.value = jobId;
    const res = await fetch(`/index/identifiers?job_id=${encodeURIComponent(jobId)}`);
    if (!res.ok) throw new Error('Failed to load identifiers');
    const data = await res.json();
    const built = buildBuckets(data.identifiers || [], data.clusters || []);
    state.buckets    = built.buckets;
    state.originalMap= built.originalMap;
    state.clusters   = data.clusters || [];
    history = [];
    render();
    try { await loadClusters(jobId); } catch (e) {}
    // Make finder appear and point it at the same job
    if (window.setCurrentJobId) window.setCurrentJobId(jobId);
    if (window.loadFinder) window.loadFinder(jobId);
  }

  function buildPayload() {
    const creates = [];
    const moves = [];
    const relabels = [];

    // new buckets
    for (const b of state.buckets) {
      if (b.id.startsWith('new-') && b.items.length) {
        creates.push({ label: b.items[0], members: [...b.items] });
      }
    }
    // membership + relabels
    const newMap = {};
    for (const b of state.buckets) {
      if (b.items.length && !b.id.startsWith('new-')) {
        relabels.push({ cluster_id: b.id, label: b.items[0] });
      }
      for (const ident of b.items) newMap[ident] = b.id;
    }
    // moves for existing identifiers
    for (const [ident, oldCid] of Object.entries(state.originalMap)) {
      const newCid = newMap[ident];
      if (newCid && newCid !== oldCid) {
        moves.push({ identifier: ident, target_cluster_id: newCid });
      }
    }

    return { job_id: jobIdInput.value.trim(), creates, relabels, moves };
  }

  function reviewChanges() {
    const payload = buildPayload();
    diffBox.textContent = JSON.stringify(payload, null, 2);
    reviewPanel.style.display = '';
  }

  async function doSaveConfirmed() {
    const payload = buildPayload();
    if (!payload.job_id) { alert('Provide a job_id first.'); return; }
    const res = await fetch('/index/clusters/update', {
      method: 'POST',
      headers: {'Content-Type':'application/json'},
      body: JSON.stringify(payload)
    });
    if (!res.ok) {
      const t = await res.text().catch(()=> '');
      alert(`Update failed: ${res.status} ${t}`);
      return;
    }
    reviewPanel.style.display = 'none';
    await load(payload.job_id);
  }

  // ---- Buttons --------------------------------------------------------------
  loadBtn?.addEventListener('click', () => {
    const jobId = jobIdInput.value.trim();
    if (jobId) load(jobId).catch(err => alert(err.message));
  });

  saveBtn?.addEventListener('click', () => {
    reviewChanges();
  });

  addBucketBtn?.addEventListener('click', () => {
    pushHistory();
    createBucket(); render();
  });

  undoBtn?.addEventListener('click', () => {
    if (!history.length) return;
    state.buckets = history.pop().buckets;
    render();
  });

  closeReview?.addEventListener('click', () => { reviewPanel.style.display = 'none'; });
  confirmSave?.addEventListener('click', () => { doSaveConfirmed().catch(err => alert(err.message)); });

  // expose loader for the indexing script to call after job completes
  window.loadIdentifiers = load;
})();
