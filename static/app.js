// SyncLinkPro frontend
const $ = (sel) => document.querySelector(sel);
const $$ = (sel) => Array.from(document.querySelectorAll(sel));

let pairs = [];
let activePairId = null;
let currentFilter = "all";
let editingId = null;
let pickerTarget = null; // 'a' | 'b'
// Per-pair live progress — keyed by pair id. Cleared when the pair leaves
// "syncing" status so the sidebar strip disappears. Lets us surface progress
// for ANY currently-syncing pair (scheduled / safety-scan / manual), not just
// the one the user happens to have selected.
let pairProgress = {}; // { [id]: { phase: string, percent: number } }

// ---------- API ----------
const api = {
  listPairs: () => fetch("/api/pairs").then(r => r.json()),
  createPair: (body) => fetch("/api/pairs", {method:"POST", headers:{"Content-Type":"application/json"}, body: JSON.stringify(body)}).then(async r => { if (!r.ok) throw new Error(await r.text()); return r.json(); }),
  updatePair: (id, body) => fetch(`/api/pairs/${id}`, {method:"PATCH", headers:{"Content-Type":"application/json"}, body: JSON.stringify(body)}).then(r => r.json()),
  deletePair: (id) => fetch(`/api/pairs/${id}`, {method:"DELETE"}).then(r => r.json()),
  syncNow: (id) => fetch(`/api/pairs/${id}/sync`, {method:"POST"}).then(r => r.json()),
  pause: (id) => fetch(`/api/pairs/${id}/pause`, {method:"POST"}).then(r => r.json()),
  resume: (id) => fetch(`/api/pairs/${id}/resume`, {method:"POST"}).then(r => r.json()),
  logs: (id) => fetch(`/api/pairs/${id}/logs`).then(r => r.json()),
  browse: (path) => fetch(`/api/browse?path=${encodeURIComponent(path||"")}`).then(r => r.json()),
};

// ---------- render ----------
function renderSidebar() {
  const list = $("#pair-list");
  list.innerHTML = "";
  for (const p of pairs) {
    const li = document.createElement("li");
    li.className = "pair-item" + (p.id === activePairId ? " active" : "");
    const dotClass = p.paused ? "paused" : (p.status || "idle");
    const prog = pairProgress[p.id];
    const showProg = !p.paused && (p.status === "syncing") && prog;
    li.innerHTML = `
      <div class="pair-row">
        <span class="dot ${dotClass}"></span>
        <span class="pair-name">${escape(p.name)}</span>
        ${showProg ? `<span class="pair-pct mono">${prog.percent || 0}%</span>` : ""}
        <span class="pair-mode">1W</span>
      </div>
      ${showProg
        ? `<div class="pair-progress"><div class="pair-progress-fill" style="width:${prog.percent || 0}%"></div></div>`
        : ""}
    `;
    li.onclick = () => selectPair(p.id);
    list.appendChild(li);
  }
}

function renderDetail() {
  const p = pairs.find(x => x.id === activePairId);
  if (!p) { $("#empty-state").classList.remove("hidden"); $("#detail").classList.add("hidden"); return; }
  $("#empty-state").classList.add("hidden");
  $("#detail").classList.remove("hidden");
  $("#d-name").textContent = p.name;
  const status = p.paused ? "paused" : (p.status || "idle");
  const chip = $("#d-status");
  chip.textContent = status.toUpperCase();
  chip.className = "status-chip " + status;
  $("#d-a").textContent = p.folder_a;
  $("#d-b").textContent = p.folder_b;
  $("#d-mode").textContent = "ONE-WAY →";
  $("#d-trigger").textContent = p.trigger.toUpperCase() + (p.schedule ? ` (${p.schedule})` : "");
  $("#d-orphans").textContent = p.delete_orphans ? "YES" : "NO";
  $("#d-last").textContent = p.last_sync || "Never";
  const ig = [...(p.ignore_dirs||[]), ...(p.ignore_patterns||[]), ...(p.ignore_files||[])];
  $("#d-ignore").textContent = ig.length ? ig.join(", ") : "—";
  $("#btn-pause").textContent = p.paused ? "RESUME" : "PAUSE";
}

function renderLogs(logs) {
  const ul = $("#logs");
  ul.innerHTML = "";
  for (const log of logs) {
    // progress/status are transient UI state, never history. Skip defensively
    // in case any old entries are still in the buffer.
    if (log.level === "progress" || log.level === "status") continue;
    if (currentFilter !== "all" && log.level !== currentFilter) continue;
    const li = document.createElement("li");
    li.className = "log";
    const ts = log.ts ? log.ts.split("T")[1] || log.ts : "";
    li.innerHTML = `
      <span class="log-ts">${escape(ts)}</span>
      <span class="log-lvl ${log.level}">${log.level.toUpperCase()}</span>
      <span class="log-msg">${escape(log.message)}</span>
    `;
    ul.appendChild(li);
  }
  ul.scrollTop = ul.scrollHeight;
}

function escape(s) {
  return String(s ?? "").replace(/[&<>"']/g, c => ({"&":"&amp;","<":"&lt;",">":"&gt;","\"":"&quot;","'":"&#39;"}[c]));
}

// ---------- selection ----------
async function selectPair(id) {
  activePairId = id;
  renderSidebar();
  renderDetail();
  // If a sync is already in flight when we land on this pair, surface the bar
  // immediately with whatever progress we've cached from WS events instead of
  // waiting for the next progress tick.
  const p = pairs.find(x => x.id === id);
  if (p && p.status === "syncing") {
    showProgress();
    const cached = pairProgress[id];
    if (cached) updateProgress(cached.phase, cached.percent);
  } else {
    hideProgress();
  }
  const logs = await api.logs(id);
  renderLogs(logs);
}

async function refreshAll() {
  pairs = await api.listPairs();
  renderSidebar();
  renderDetail();
  if (activePairId) {
    // Same in-flight-sync handling as selectPair — page reload during a sync
    // should still show the bar.
    const p = pairs.find(x => x.id === activePairId);
    if (p && p.status === "syncing") {
      showProgress();
      const cached = pairProgress[activePairId];
      if (cached) updateProgress(cached.phase, cached.percent);
    } else {
      hideProgress();
    }
    const logs = await api.logs(activePairId);
    renderLogs(logs);
  }
}

// ---------- WebSocket ----------
let ws, wsLogs = [];
function connectWS() {
  ws = new WebSocket(`ws://${location.host}/ws`);
  ws.onopen = () => {
    $("#ws-dot").classList.add("on");
    $("#ws-label").textContent = "LIVE";
  };
  ws.onclose = () => {
    $("#ws-dot").classList.remove("on");
    $("#ws-label").textContent = "DISCONNECTED";
    setTimeout(connectWS, 2000);
  };
  ws.onmessage = (e) => {
    const data = JSON.parse(e.data);
    // Progress events: store per-pair, update detail bar if active, refresh
    // sidebar so the inline strip moves for whichever pair is syncing (even
    // when that pair isn't selected — e.g. scheduled / safety-scan).
    if (data.level === "progress") {
      pairProgress[data.pair_id] = { phase: data.message, percent: data.percent || 0 };
      if (data.pair_id === activePairId) updateProgress(data.message, data.percent);
      renderSidebar();
      return;
    }
    // Append to current view logs if active pair
    if (data.pair_id === activePairId) {
      wsLogs = [...wsLogs, { ts: new Date().toISOString(), level: data.level, message: data.message }];
      if (wsLogs.length > 200) wsLogs = wsLogs.slice(-200);
      appendLog(wsLogs[wsLogs.length - 1]);
    }
    // Update pair status in-place
    if (data.level === "status") {
      const p = pairs.find(x => x.id === data.pair_id);
      if (p) { p.status = data.message; }
      // Seed/clear per-pair progress so the sidebar strip appears at sync
      // start and disappears the moment sync ends.
      if (data.message === "syncing") {
        if (!pairProgress[data.pair_id]) pairProgress[data.pair_id] = { phase: "Starting…", percent: 0 };
      } else {
        delete pairProgress[data.pair_id];
      }
      renderSidebar();
      if (p && p.id === activePairId) renderDetail();
      if (data.pair_id === activePairId) {
        if (data.message === "syncing") showProgress();
        else hideProgress();
      }
    }
  };
}

function showProgress() { $("#progress-wrap").classList.remove("hidden"); }
function hideProgress() { $("#progress-wrap").classList.add("hidden"); updateProgress("", 0); }
function updateProgress(phase, pct) {
  $("#progress-phase").textContent = phase || "";
  $("#progress-pct").textContent = (pct || 0) + "%";
  $("#progress-fill").style.width = (pct || 0) + "%";
}
function appendLog(log) {
  if (currentFilter !== "all" && log.level !== currentFilter) return;
  const ul = $("#logs");
  const li = document.createElement("li");
  li.className = "log";
  const ts = log.ts ? log.ts.split("T")[1] || log.ts : "";
  li.innerHTML = `
    <span class="log-ts">${escape(ts)}</span>
    <span class="log-lvl ${log.level}">${log.level.toUpperCase()}</span>
    <span class="log-msg">${escape(log.message)}</span>
  `;
  ul.appendChild(li);
  ul.scrollTop = ul.scrollHeight;
}

// ---------- Modal ----------
const DEFAULT_IGNORE_DIRS = [".git", "node_modules", ".next", "dist", ".venv", "venv", "__pycache__", ".sync-metadata"];
const DEFAULT_IGNORE_PATTERNS = ["*.log", "*.tmp", "~$*"];

function openModal(pair = null) {
  editingId = pair ? pair.id : null;
  $("#modal-title").textContent = pair ? "EDIT SYNC PAIR" : "NEW SYNC PAIR";
  $("#f-name").value = pair?.name || "";
  $("#f-a").value = pair?.folder_a || "";
  $("#f-b").value = pair?.folder_b || "";
  $("#f-trigger").value = pair?.trigger || "auto";
  $("#f-schedule").value = pair?.schedule || "";
  $("#f-idirs").value = pair ? (pair.ignore_dirs || []).join(", ") : DEFAULT_IGNORE_DIRS.join(", ");
  $("#f-ipat").value = pair ? (pair.ignore_patterns || []).join(", ") : DEFAULT_IGNORE_PATTERNS.join(", ");
  $("#f-safety").value = pair?.safety_scan_interval ?? "";
  // Delete propagation is always on and not user-toggleable.
  $("#f-orphans").checked = true;
  $("#f-orphans").disabled = true;
  toggleScheduleVisible();
  $("#modal").classList.remove("hidden");
}
function closeModal() { $("#modal").classList.add("hidden"); editingId = null; }

function toggleScheduleVisible() {
  const show = $("#f-trigger").value === "scheduled";
  $("#schedule-label").classList.toggle("hidden", !show);
}

async function saveModal() {
  const split = (s) => s.split(",").map(x=>x.trim()).filter(Boolean);
  const body = {
    name: $("#f-name").value.trim(),
    folder_a: $("#f-a").value.trim(),
    folder_b: $("#f-b").value.trim(),
    trigger: $("#f-trigger").value,
    schedule: $("#f-trigger").value === "scheduled" ? $("#f-schedule").value.trim() || null : null,
    ignore_dirs: split($("#f-idirs").value),
    ignore_patterns: split($("#f-ipat").value),
    safety_scan_interval: $("#f-safety").value ? parseInt($("#f-safety").value, 10) : null,
    delete_orphans: true,
  };
  if (!body.name || !body.folder_a || !body.folder_b) {
    alert("Name, Folder A, and Folder B are required.");
    return;
  }
  try {
    if (editingId) {
      await api.updatePair(editingId, body);
    } else {
      await api.createPair(body);
    }
    closeModal();
    await refreshAll();
  } catch (e) {
    alert("Save failed: " + e.message);
  }
}

// ---------- Folder Picker ----------
let pickerPath = "";
async function openPicker(target) {
  pickerTarget = target;
  pickerPath = "";
  await loadPicker("");
  $("#picker").classList.remove("hidden");
}
function closePicker() { $("#picker").classList.add("hidden"); pickerTarget = null; }
async function loadPicker(path) {
  const data = await api.browse(path);
  pickerPath = data.path;
  $("#picker-path").textContent = data.is_root ? "Select a drive…" : data.path;
  const ul = $("#picker-list");
  ul.innerHTML = "";
  if (!data.is_root && data.parent !== undefined) {
    const li = document.createElement("li");
    li.className = "picker-item parent";
    li.textContent = "..";
    li.onclick = () => loadPicker(data.parent);
    ul.appendChild(li);
  }
  for (const e of (data.entries||[])) {
    const li = document.createElement("li");
    li.className = "picker-item";
    li.textContent = e.name;
    li.onclick = () => loadPicker(e.path);
    ul.appendChild(li);
  }
}
function confirmPicker() {
  if (!pickerPath) { alert("Navigate into a folder first."); return; }
  if (pickerTarget === "a") $("#f-a").value = pickerPath;
  if (pickerTarget === "b") $("#f-b").value = pickerPath;
  closePicker();
}

// ---------- Event wiring ----------
function wire() {
  $("#btn-new").onclick = () => openModal();
  $("#modal-close").onclick = closeModal;
  $("#modal-cancel").onclick = closeModal;
  $("#modal-save").onclick = saveModal;
  $("#f-trigger").onchange = toggleScheduleVisible;
  $$("[data-cron]").forEach(a => a.onclick = (e) => { e.preventDefault(); $("#f-schedule").value = a.getAttribute("data-cron"); });

  $("#pick-a").onclick = () => openPicker("a");
  $("#pick-b").onclick = () => openPicker("b");
  $("#picker-close").onclick = closePicker;
  $("#picker-cancel").onclick = closePicker;
  $("#picker-confirm").onclick = confirmPicker;

  $("#btn-sync-now").onclick = async () => { if (!activePairId) return; await api.syncNow(activePairId); };
  $("#btn-pause").onclick = async () => {
    if (!activePairId) return;
    const p = pairs.find(x => x.id === activePairId);
    if (p.paused) await api.resume(activePairId); else await api.pause(activePairId);
    await refreshAll();
  };
  $("#btn-edit").onclick = () => {
    const p = pairs.find(x => x.id === activePairId);
    if (p) openModal(p);
  };
  $("#btn-delete").onclick = async () => {
    if (!activePairId) return;
    const p = pairs.find(x => x.id === activePairId);
    if (!confirm(`Remove pair "${p.name}"? (Folders and files are NOT deleted.)`)) return;
    await api.deletePair(activePairId);
    activePairId = null;
    await refreshAll();
  };

  $$("[data-filter]").forEach(btn => btn.onclick = () => {
    currentFilter = btn.getAttribute("data-filter");
    $$("[data-filter]").forEach(b => b.classList.toggle("chip-active", b === btn));
    if (activePairId) api.logs(activePairId).then(renderLogs);
  });

  document.addEventListener("keydown", (e) => {
    if (e.key === "Escape") { closeModal(); closePicker(); }
  });
}

// ---------- boot ----------
(async function boot() {
  wire();
  connectWS();
  await refreshAll();
})();
