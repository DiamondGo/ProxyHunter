const state = { proxies: [], selected: new Set(), sort: { key: null, dir: null } };
let jobPollTimer = null;

const SORT_STORAGE_KEY = "proxyhunter_sort";

function loadSortPref() {
  try {
    const raw = localStorage.getItem(SORT_STORAGE_KEY);
    if (raw) state.sort = JSON.parse(raw);
  } catch (err) {
    // ignore - localStorage unavailable or corrupt, just use the default
  }
}

function saveSortPref() {
  try {
    localStorage.setItem(SORT_STORAGE_KEY, JSON.stringify(state.sort));
  } catch (err) {
    // ignore
  }
}

function fmtLatency(p) {
  return p.latency_ms == null ? "-" : Math.round(p.latency_ms);
}

function fmtLocation(p) {
  return [p.country, p.city].filter(Boolean).join(" / ") || "-";
}

function fmtHttps(p) {
  if (!p.supports_https) return t("https.no");
  if (p.https_only) {
    return `${t("https.yes")} <span class="warn-badge" title="${t("https.only443_title")}">${t("https.only443_label")}</span>`;
  }
  return t("https.yes");
}

function fmtPoolStatus(p) {
  if (!p.selected) return "-";
  if (p.pool_failed) {
    return `<span class="warn-badge" title="${t("pool.failed_title")}">${t("pool.failed", { count: p.pool_fail_count })}</span>`;
  }
  return t("pool.in_use");
}

// Each column defines get() (a comparable value used for sorting) and
// render() (the HTML shown in the cell). Click a header to cycle asc -> desc
// -> unsorted (back to the server's default order: alive first, then latency).
const COLUMNS = [
  {
    key: "alive",
    labelKey: "col.status",
    get: (p) => (p.alive ? 1 : 0),
    render: (p) => `<span class="dot ${p.alive ? "alive" : "dead"}"></span>${p.alive ? t("status.alive") : t("status.dead")}`,
  },
  {
    key: "address",
    labelKey: "col.address",
    get: (p) => `${p.ip} ${String(p.port).padStart(5, "0")}`,
    render: (p) => `<code>${p.protocol}://${p.ip}:${p.port}</code>`,
  },
  { key: "protocol", labelKey: "col.protocol", get: (p) => p.protocol || "", render: (p) => p.protocol },
  { key: "location", labelKey: "col.location", get: (p) => fmtLocation(p), render: (p) => fmtLocation(p) },
  { key: "isp", labelKey: "col.isp", get: (p) => p.isp || "", render: (p) => p.isp || "-" },
  { key: "latency", labelKey: "col.latency", get: (p) => p.latency_ms, render: (p) => fmtLatency(p) },
  {
    key: "https",
    labelKey: "col.https",
    get: (p) => (p.supports_https ? 1 : 0),
    render: (p) => fmtHttps(p),
  },
  { key: "anonymity", labelKey: "col.anonymity", get: (p) => p.anonymity || "", render: (p) => p.anonymity || "-" },
  { key: "source", labelKey: "col.source", get: (p) => p.source || "", render: (p) => p.source },
  { key: "checked_at", labelKey: "col.checked_at", get: (p) => p.checked_at || "", render: (p) => p.checked_at || "-" },
  {
    key: "pool",
    labelKey: "col.pool",
    get: (p) => (p.selected ? (p.pool_failed ? 0 : 2) : 1),
    render: (p) => fmtPoolStatus(p),
  },
];

function updateCountLabel() {
  document.getElementById("proxy-count").textContent = t("table.count", { total: state.proxies.length, selected: state.selected.size });
}

function sortedProxies() {
  if (!state.sort.key) return state.proxies;
  const col = COLUMNS.find((c) => c.key === state.sort.key);
  if (!col) return state.proxies;
  const dir = state.sort.dir === "desc" ? -1 : 1;
  return [...state.proxies].sort((a, b) => {
    let va = col.get(a);
    let vb = col.get(b);
    const aEmpty = va == null || va === "";
    const bEmpty = vb == null || vb === "";
    if (aEmpty && bEmpty) return 0;
    if (aEmpty) return 1; // empty/unknown values always sort last, regardless of direction
    if (bEmpty) return -1;
    if (typeof va === "string") va = va.toLowerCase();
    if (typeof vb === "string") vb = vb.toLowerCase();
    if (va < vb) return -1 * dir;
    if (va > vb) return 1 * dir;
    return 0;
  });
}

function renderHeader() {
  const tr = document.getElementById("proxy-thead-row");
  tr.innerHTML = "<th></th>";
  for (const col of COLUMNS) {
    const th = document.createElement("th");
    th.className = "sortable";
    let arrow = "";
    if (state.sort.key === col.key) {
      arrow = state.sort.dir === "asc" ? " ▲" : " ▼";
    }
    th.textContent = t(col.labelKey) + arrow;
    th.addEventListener("click", () => {
      if (state.sort.key !== col.key) {
        state.sort = { key: col.key, dir: "asc" };
      } else if (state.sort.dir === "asc") {
        state.sort = { key: col.key, dir: "desc" };
      } else {
        state.sort = { key: null, dir: null };
      }
      saveSortPref();
      renderTable();
    });
    tr.appendChild(th);
  }
}

function renderTable() {
  renderHeader();

  const tbody = document.getElementById("proxy-tbody");
  tbody.innerHTML = "";
  for (const p of sortedProxies()) {
    const tr = document.createElement("tr");
    const checked = state.selected.has(p.key) ? "checked" : "";
    const cells = COLUMNS.map((col) => `<td>${col.render(p)}</td>`).join("");
    tr.innerHTML = `<td><input type="checkbox" class="row-chk" data-key="${p.key}" ${checked}></td>${cells}`;
    tbody.appendChild(tr);
  }
  updateCountLabel();

  tbody.querySelectorAll(".row-chk").forEach((el) => {
    el.addEventListener("change", () => {
      if (el.checked) state.selected.add(el.dataset.key);
      else state.selected.delete(el.dataset.key);
      updateCountLabel();
    });
  });
}

async function loadProxies() {
  const data = await fetchJSON("/api/proxies");
  state.proxies = data.proxies;
  renderTable();
}

async function loadForwardStatus() {
  const data = await fetchJSON("/api/forward/status");
  const el = document.getElementById("forward-status");
  el.innerHTML = t("forward.status_line", {
    http: data.http_proxy,
    socks: data.socks5_proxy,
    count: data.count,
    usable: data.usable_count,
  });

  const warning = document.getElementById("pool-warning");
  if (data.count > 0 && !data.has_usable) {
    warning.classList.remove("hidden");
  } else {
    warning.classList.add("hidden");
  }
}

function showJobPanel(text) {
  const panel = document.getElementById("job-status");
  panel.classList.remove("hidden");
  panel.textContent = text;
}
function hideJobPanel() {
  document.getElementById("job-status").classList.add("hidden");
}

function fmtJobSummary(status) {
  const s = status.summary;
  if (status.kind === "revalidate") {
    return t("job.summary_revalidate", { checked: s.checked, alive: s.alive });
  }
  return t("job.summary_scrape", {
    scraped: s.scraped,
    unique: s.unique,
    reused: s.reused_alive,
    fresh: s.freshly_checked,
    alive: s.alive,
  });
}

async function pollJob() {
  const status = await fetchJSON("/api/job");
  if (!status.running) {
    if (jobPollTimer) clearInterval(jobPollTimer);
    jobPollTimer = null;
    if (status.error) {
      showJobPanel(t("job.failed", { error: status.error }));
    } else if (status.summary && Object.keys(status.summary).length) {
      showJobPanel(fmtJobSummary(status));
    } else {
      hideJobPanel();
    }
    await loadProxies();
    await loadForwardStatus();
    document.getElementById("btn-scrape").disabled = false;
    return;
  }
  const key = status.kind === "revalidate" ? "job.revalidate_running" : "job.running";
  showJobPanel(t(key, { message: status.message || "..." }));
}

// Covers the case where a scheduled task started a job before this tab was
// opened - without this the panel/poll loop would only ever start from the
// button click handlers below.
async function checkJobOnLoad() {
  const status = await fetchJSON("/api/job");
  if (status.running && !jobPollTimer) {
    document.getElementById("btn-scrape").disabled = true;
    showJobPanel(t("job.running", { message: status.message || "..." }));
    jobPollTimer = setInterval(pollJob, 2000);
  }
}

document.getElementById("btn-refresh").addEventListener("click", () => {
  loadProxies();
  loadForwardStatus();
});

document.getElementById("chk-all").addEventListener("change", (e) => {
  if (e.target.checked) {
    state.proxies.forEach((p) => state.selected.add(p.key));
  } else {
    state.selected.clear();
  }
  renderTable();
});

document.getElementById("btn-scrape").addEventListener("click", async () => {
  document.getElementById("btn-scrape").disabled = true;
  try {
    await fetchJSON("/api/scrape", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: "{}",
    });
    showJobPanel(t("job.submitted"));
    jobPollTimer = setInterval(pollJob, 2000);
  } catch (err) {
    document.getElementById("btn-scrape").disabled = false;
    alert(err.message);
  }
});

document.getElementById("btn-validate-selected").addEventListener("click", async () => {
  const keys = Array.from(state.selected);
  if (!keys.length) {
    alert(t("alert.select_validate"));
    return;
  }
  const btn = document.getElementById("btn-validate-selected");
  btn.disabled = true;
  btn.textContent = t("toolbar.validating");
  try {
    const result = await fetchJSON("/api/validate", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ keys }),
    });
    alert(t("alert.validate_done", { checked: result.checked, alive: result.alive }));
    await loadProxies();
    await loadForwardStatus();
  } catch (err) {
    alert(err.message);
  } finally {
    btn.disabled = false;
    btn.textContent = t("toolbar.validate_selected");
  }
});

document.getElementById("btn-set-pool").addEventListener("click", async () => {
  const keys = Array.from(state.selected);
  if (!keys.length) {
    alert(t("alert.select_pool"));
    return;
  }
  await fetchJSON("/api/forward/select", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ keys }),
  });
  await loadProxies();
  await loadForwardStatus();
});

document.getElementById("btn-clear-pool").addEventListener("click", async () => {
  await fetchJSON("/api/forward/select", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ keys: [] }),
  });
  await loadProxies();
  await loadForwardStatus();
});

document.getElementById("btn-remove-from-pool").addEventListener("click", async () => {
  const keys = Array.from(state.selected);
  if (!keys.length) {
    alert(t("alert.select_remove_pool"));
    return;
  }
  await fetchJSON("/api/forward/remove", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ keys }),
  });
  await loadProxies();
  await loadForwardStatus();
});

document.getElementById("btn-delete-proxies").addEventListener("click", async () => {
  const keys = Array.from(state.selected);
  if (!keys.length) {
    alert(t("alert.select_delete"));
    return;
  }
  if (!confirm(t("confirm.delete_proxies", { count: keys.length }))) {
    return;
  }
  const result = await fetchJSON("/api/proxies/remove", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ keys }),
  });
  state.selected.clear();
  alert(t("alert.deleted", { count: result.removed }));
  await loadProxies();
  await loadForwardStatus();
});

async function boot() {
  await i18nInit();
  loadSortPref();
  await loadProxies();
  await loadForwardStatus();
  checkJobOnLoad();
  setInterval(loadForwardStatus, 8000);
  setInterval(() => {
    if (!jobPollTimer) checkJobOnLoad();
  }, 8000);
}
boot();
