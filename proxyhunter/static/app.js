// sortByView/filtersByView/selectedByView keep "all proxies" and "forward
// pool" sort order, filter selections, and row selections separate;
// state.sort/state.filters/state.selected are live references to the active
// view's object/dict/Set, so existing state.filters[key] = ... /
// state.selected.add(...) mutations transparently land in the right bucket
// and switching the reference back later picks them up again. state.sort is
// the exception - its click handler replaces the object wholesale rather
// than mutating in place, so it also writes back into sortByView explicitly.
const state = {
  proxies: [],
  sortByView: { all: { key: null, dir: null }, pool: { key: null, dir: null } },
  filtersByView: { all: {}, pool: {} },
  selectedByView: { all: new Set(), pool: new Set() },
  view: "all",
};
state.sort = state.sortByView[state.view];
state.filters = state.filtersByView[state.view];
state.selected = state.selectedByView[state.view];
let jobPollTimer = null;

const SORT_STORAGE_KEY = "proxyhunter_sort";

function loadSortPref() {
  try {
    const raw = localStorage.getItem(SORT_STORAGE_KEY);
    if (raw) {
      const parsed = JSON.parse(raw);
      // Old format was a single {key,dir} shared across views - treat it as
      // the "all" view's preference and leave "pool" at its default.
      state.sortByView = "all" in parsed || "pool" in parsed ? parsed : { all: parsed, pool: { key: null, dir: null } };
      state.sort = state.sortByView[state.view];
    }
  } catch (err) {
    // ignore - localStorage unavailable or corrupt, just use the default
  }
}

function saveSortPref() {
  try {
    localStorage.setItem(SORT_STORAGE_KEY, JSON.stringify(state.sortByView));
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

function fmtPoolStats(p) {
  if (!p.selected) return "-";
  const total = p.pool_request_count || 0;
  const success = p.pool_success_count || 0;
  if (total === 0) return t("pool.stats_none");
  return t("pool.stats", { success, total });
}

function fmtPoolStatus(p) {
  const btn = p.selected
    ? `<button type="button" class="pool-action-btn secondary" data-key="${p.key}" data-action="remove">${t("pool.remove_one")}</button>`
    : `<button type="button" class="pool-action-btn secondary" data-key="${p.key}" data-action="add">${t("pool.add_one")}</button>`;
  if (!p.selected) return btn;
  if (p.pool_failed) {
    return `<span class="warn-badge" title="${t("pool.failed_title")}">${t("pool.failed", { count: p.pool_fail_count })}</span>${btn}`;
  }
  return `${t("pool.in_use")}${btn}`;
}

// Each column defines get() (a comparable value used for sorting) and
// render() (the HTML shown in the cell). Click a header to cycle asc -> desc
// -> unsorted (back to the server's default order: alive first, then latency).
//
// Columns also optionally define a filter:
//   filterKind: "select" - a multi-select checkbox dropdown of every distinct
//     filterValue() present in the currently loaded proxies (built/refreshed
//     in refreshFilterOptions). A proxy matches if its filterValue() is in
//     the selected set, or if nothing is selected. filterLabel(), if given,
//     maps a raw value to its display label. filterGroupOf(value), if given,
//     maps a value to a group key ("China / Beijing" -> "China") - values
//     sharing a group are nested under a header checkbox that selects/
//     deselects all of them at once (only shown when a group has >1 member).
//   filterKind: "range" - min/max numeric bounds, applied via filterValue().
const COLUMNS = [
  {
    key: "alive",
    labelKey: "col.status",
    get: (p) => (p.alive ? 1 : 0),
    render: (p) => `<span class="dot ${p.alive ? "alive" : "dead"}"></span>${p.alive ? t("status.alive") : t("status.dead")}`,
    filterKind: "select",
    filterValue: (p) => (p.alive ? "alive" : "dead"),
    filterLabel: (v) => (v === "alive" ? t("status.alive") : t("status.dead")),
  },
  {
    key: "address",
    labelKey: "col.address",
    get: (p) => `${p.ip} ${String(p.port).padStart(5, "0")}`,
    render: (p) => `<code>${p.protocol}://${p.ip}:${p.port}</code>`,
    filterKind: "select",
    filterValue: (p) => `${p.protocol}://${p.ip}:${p.port}`,
  },
  {
    key: "protocol",
    labelKey: "col.protocol",
    get: (p) => p.protocol || "",
    render: (p) => p.protocol,
    filterKind: "select",
    filterValue: (p) => p.protocol || "",
  },
  {
    key: "location",
    labelKey: "col.location",
    get: (p) => fmtLocation(p),
    render: (p) => fmtLocation(p),
    filterKind: "select",
    filterValue: (p) => fmtLocation(p),
    // Groups "China / Beijing", "China / Shanghai", ... under a "China"
    // header so picking the country auto-selects every city under it.
    filterGroupOf: (v) => (v.includes(" / ") ? v.split(" / ")[0] : v),
  },
  {
    key: "isp",
    labelKey: "col.isp",
    get: (p) => p.isp || "",
    render: (p) => p.isp || "-",
    filterKind: "select",
    filterValue: (p) => p.isp || "-",
  },
  {
    key: "latency",
    labelKey: "col.latency",
    get: (p) => p.latency_ms,
    render: (p) => fmtLatency(p),
    filterKind: "range",
    filterValue: (p) => p.latency_ms,
  },
  {
    key: "https",
    labelKey: "col.https",
    get: (p) => (p.supports_https ? 1 : 0),
    render: (p) => fmtHttps(p),
    filterKind: "select",
    filterValue: (p) => (p.supports_https ? "yes" : "no"),
    filterLabel: (v) => (v === "yes" ? t("https.yes") : t("https.no")),
  },
  {
    key: "anonymity",
    labelKey: "col.anonymity",
    get: (p) => p.anonymity || "",
    render: (p) => p.anonymity || "-",
    filterKind: "select",
    filterValue: (p) => p.anonymity || "-",
  },
  {
    key: "source",
    labelKey: "col.source",
    get: (p) => p.source || "",
    render: (p) => p.source,
    filterKind: "select",
    filterValue: (p) => p.source || "-",
  },
  {
    key: "checked_at",
    labelKey: "col.checked_at",
    get: (p) => p.checked_at || "",
    render: (p) => p.checked_at || "-",
    filterKind: "select",
    filterValue: (p) => p.checked_at || "-",
  },
  {
    key: "pool",
    labelKey: "col.pool",
    get: (p) => (p.selected ? (p.pool_failed ? 0 : 2) : 1),
    render: (p) => fmtPoolStatus(p),
    filterKind: "select",
    filterValue: (p) => (p.selected ? (p.pool_failed ? "failed" : "in_use") : "not_in_pool"),
    filterLabel: (v) => {
      if (v === "in_use") return t("pool.in_use");
      if (v === "failed") return t("pool.failed_label");
      return t("pool.not_in_pool");
    },
  },
  {
    key: "pool_stats",
    labelKey: "col.pool_stats",
    get: (p) => p.pool_request_count || 0,
    render: (p) => fmtPoolStats(p),
    filterKind: "range",
    filterValue: (p) => p.pool_request_count || 0,
  },
];

function closeAllFilterPanels() {
  document.querySelectorAll(".ms-filter-panel").forEach((p) => p.classList.add("hidden"));
}

function filterButtonLabel(col) {
  const set = state.filters[col.key];
  if (!set || !set.size) return t("filters.all");
  if (set.size === 1) {
    const [v] = set;
    return col.filterLabel ? col.filterLabel(v) : v;
  }
  return t("filters.n_selected", { n: set.size });
}

function updateFilterButton(col) {
  const btn = document.getElementById(`filter-btn-${col.key}`);
  if (btn) btn.textContent = filterButtonLabel(col);
}

function buildFilterGrid() {
  const grid = document.getElementById("filter-grid");
  grid.innerHTML = "";
  for (const col of COLUMNS) {
    if (!col.filterKind) continue;
    const label = document.createElement("label");
    const span = document.createElement("span");
    span.textContent = t(col.labelKey);
    label.appendChild(span);

    if (col.filterKind === "select") {
      const wrap = document.createElement("div");
      wrap.className = "ms-filter";
      const btn = document.createElement("button");
      btn.type = "button";
      btn.className = "ms-filter-btn";
      btn.id = `filter-btn-${col.key}`;
      btn.textContent = t("filters.all");
      const panel = document.createElement("div");
      panel.className = "ms-filter-panel hidden";
      panel.id = `filter-panel-${col.key}`;
      btn.addEventListener("click", (e) => {
        e.stopPropagation();
        const wasHidden = panel.classList.contains("hidden");
        closeAllFilterPanels();
        if (wasHidden) panel.classList.remove("hidden");
      });
      panel.addEventListener("click", (e) => e.stopPropagation());
      wrap.appendChild(btn);
      wrap.appendChild(panel);
      label.appendChild(wrap);
    } else if (col.filterKind === "range") {
      const row = document.createElement("div");
      row.className = "range-row";
      const minInput = document.createElement("input");
      minInput.type = "number";
      minInput.id = `filter-${col.key}-min`;
      minInput.placeholder = t("filters.min");
      const sep = document.createElement("span");
      sep.textContent = t("filters.range_to");
      const maxInput = document.createElement("input");
      maxInput.type = "number";
      maxInput.id = `filter-${col.key}-max`;
      maxInput.placeholder = t("filters.max");
      const onChange = () => {
        const min = minInput.value === "" ? null : Number(minInput.value);
        const max = maxInput.value === "" ? null : Number(maxInput.value);
        state.filters[col.key] = min == null && max == null ? null : { min, max };
        renderTable();
      };
      minInput.addEventListener("input", onChange);
      maxInput.addEventListener("input", onChange);
      row.appendChild(minInput);
      row.appendChild(sep);
      row.appendChild(maxInput);
      label.appendChild(row);
    }
    grid.appendChild(label);
  }
}

// The "all proxies" / "forward pool" toggle is purely a client-side view
// over the already-loaded state.proxies (every proxy carries `selected`,
// meaning "currently in the forward pool") - applied before column filters
// so filters, options, sorting, and selection all naturally scope to it.
function viewScopedProxies() {
  if (state.view === "pool") return state.proxies.filter((p) => p.selected);
  return state.proxies;
}

function buildFilterOptionRow(label, extraClass) {
  const row = document.createElement("label");
  row.className = `ms-option${extraClass ? " " + extraClass : ""}`;
  const input = document.createElement("input");
  input.type = "checkbox";
  const span = document.createElement("span");
  span.textContent = label;
  row.appendChild(input);
  row.appendChild(span);
  return { row, input };
}

// Rebuilds each select-kind filter's checkbox list from whatever distinct
// values are currently present in the active view, preserving selections
// that are still valid options. Ungrouped columns get one row per distinct
// value; columns with filterGroupOf get their values clustered under a
// header checkbox per group (when a group has more than one member) that
// selects/deselects every value in that group at once.
function populateSelectFilterPanel(col, scoped) {
  const panel = document.getElementById(`filter-panel-${col.key}`);
  if (!panel) return;
  panel.innerHTML = "";

  const values = new Set();
  const groups = col.filterGroupOf ? new Map() : null;
  for (const p of scoped) {
    const v = String(col.filterValue(p));
    values.add(v);
    if (groups) {
      const g = col.filterGroupOf(v);
      if (!groups.has(g)) groups.set(g, new Set());
      groups.get(g).add(v);
    }
  }

  const prevSet = state.filters[col.key] || new Set();
  const currentSet = new Set(Array.from(prevSet).filter((v) => values.has(v)));
  const commit = () => {
    state.filters[col.key] = currentSet.size ? currentSet : null;
    updateFilterButton(col);
    renderTable();
  };

  if (groups) {
    const groupKeys = Array.from(groups.keys()).sort((a, b) => a.localeCompare(b));
    for (const g of groupKeys) {
      const leaves = Array.from(groups.get(g)).sort((a, b) => a.localeCompare(b));
      if (leaves.length > 1) {
        const { row: headerRow, input: headerInput } = buildFilterOptionRow(g, "ms-group");
        panel.appendChild(headerRow);
        const childInputs = [];
        const syncHeader = () => {
          const checkedCount = childInputs.filter((i) => i.checked).length;
          headerInput.checked = checkedCount === childInputs.length;
          headerInput.indeterminate = checkedCount > 0 && checkedCount < childInputs.length;
        };
        for (const leaf of leaves) {
          const leafLabel = col.filterLabel ? col.filterLabel(leaf) : leaf;
          const { row, input } = buildFilterOptionRow(leafLabel, "ms-child");
          input.checked = currentSet.has(leaf);
          input.addEventListener("change", () => {
            if (input.checked) currentSet.add(leaf);
            else currentSet.delete(leaf);
            syncHeader();
            commit();
          });
          panel.appendChild(row);
          childInputs.push(input);
        }
        syncHeader();
        headerInput.addEventListener("change", () => {
          const select = headerInput.checked;
          for (const input of childInputs) input.checked = select;
          for (const leaf of leaves) {
            if (select) currentSet.add(leaf);
            else currentSet.delete(leaf);
          }
          headerInput.indeterminate = false;
          commit();
        });
      } else {
        const leaf = leaves[0];
        const leafLabel = col.filterLabel ? col.filterLabel(leaf) : leaf;
        const { row, input } = buildFilterOptionRow(leafLabel);
        input.checked = currentSet.has(leaf);
        input.addEventListener("change", () => {
          if (input.checked) currentSet.add(leaf);
          else currentSet.delete(leaf);
          commit();
        });
        panel.appendChild(row);
      }
    }
  } else {
    const sorted = Array.from(values).sort((a, b) => a.localeCompare(b));
    for (const v of sorted) {
      const label = col.filterLabel ? col.filterLabel(v) : v;
      const { row, input } = buildFilterOptionRow(label);
      input.checked = currentSet.has(v);
      input.addEventListener("change", () => {
        if (input.checked) currentSet.add(v);
        else currentSet.delete(v);
        commit();
      });
      panel.appendChild(row);
    }
  }

  state.filters[col.key] = currentSet.size ? currentSet : null;
  updateFilterButton(col);
}

function refreshFilterOptions() {
  const scoped = viewScopedProxies();
  for (const col of COLUMNS) {
    if (col.filterKind !== "select") continue;
    populateSelectFilterPanel(col, scoped);
  }
}

// Range filter <input> values live in the DOM, not just in state, so
// switching state.filters to the other view's dict needs this to make the
// visible min/max boxes match what's actually being applied again.
function resyncRangeFilterInputs() {
  for (const col of COLUMNS) {
    if (col.filterKind !== "range") continue;
    const filter = state.filters[col.key];
    const minInput = document.getElementById(`filter-${col.key}-min`);
    const maxInput = document.getElementById(`filter-${col.key}-max`);
    if (minInput) minInput.value = filter && filter.min != null ? filter.min : "";
    if (maxInput) maxInput.value = filter && filter.max != null ? filter.max : "";
  }
}

function filteredProxies() {
  return viewScopedProxies().filter((p) => {
    for (const col of COLUMNS) {
      if (!col.filterKind) continue;
      const filter = state.filters[col.key];
      if (!filter) continue;
      if (col.filterKind === "select") {
        if (filter.size && !filter.has(String(col.filterValue(p)))) return false;
      } else if (col.filterKind === "range") {
        const v = col.filterValue(p);
        if (filter.min != null && (v == null || v < filter.min)) return false;
        if (filter.max != null && (v == null || v > filter.max)) return false;
      }
    }
    return true;
  });
}

// Selections made under a different filter (or the other view) stay
// remembered in state.selected (so re-applying that filter/view shows them
// checked again), but they must never silently count toward "已选" or get
// swept into a bulk action while hidden - only checkboxes actually visible
// right now count as "selected".
function visibleSelectedKeys() {
  const visible = new Set(filteredProxies().map((p) => p.key));
  return Array.from(state.selected).filter((k) => visible.has(k));
}

function updateCountLabel(shown) {
  document.getElementById("proxy-count").textContent = t("table.count", {
    shown,
    total: viewScopedProxies().length,
    selected: visibleSelectedKeys().length,
  });
}

function sortedProxies() {
  const list = filteredProxies();
  if (!state.sort.key) return list;
  const col = COLUMNS.find((c) => c.key === state.sort.key);
  if (!col) return list;
  const dir = state.sort.dir === "desc" ? -1 : 1;
  return [...list].sort((a, b) => {
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
      state.sortByView[state.view] = state.sort;
      saveSortPref();
      renderTable();
    });
    tr.appendChild(th);
  }
}

function renderTable() {
  renderHeader();

  const list = sortedProxies();
  const tbody = document.getElementById("proxy-tbody");
  tbody.innerHTML = "";
  for (const p of list) {
    const tr = document.createElement("tr");
    const checked = state.selected.has(p.key) ? "checked" : "";
    const cells = COLUMNS.map((col) => `<td>${col.render(p)}</td>`).join("");
    tr.innerHTML = `<td><input type="checkbox" class="row-chk" data-key="${p.key}" ${checked}></td>${cells}`;
    tbody.appendChild(tr);
  }
  updateCountLabel(list.length);

  tbody.querySelectorAll(".row-chk").forEach((el) => {
    el.addEventListener("change", () => {
      if (el.checked) state.selected.add(el.dataset.key);
      else state.selected.delete(el.dataset.key);
      updateCountLabel(list.length);
    });
  });
}

async function loadProxies() {
  const data = await fetchJSON("/api/proxies");
  state.proxies = data.proxies;
  refreshFilterOptions();
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
  const visible = sortedProxies();
  if (e.target.checked) {
    visible.forEach((p) => state.selected.add(p.key));
  } else {
    visible.forEach((p) => state.selected.delete(p.key));
  }
  renderTable();
});

document.getElementById("btn-toggle-filters").addEventListener("click", () => {
  document.getElementById("filter-panel").classList.toggle("hidden");
});

const ALL_VIEW_ONLY_BUTTON_IDS = ["btn-scrape", "btn-add-to-pool", "btn-delete-proxies"];
const POOL_VIEW_ONLY_BUTTON_IDS = ["btn-remove-from-pool", "btn-clear-pool"];

function updateViewTabs() {
  document.getElementById("tab-view-all").classList.toggle("active", state.view === "all");
  document.getElementById("tab-view-pool").classList.toggle("active", state.view === "pool");
  for (const id of ALL_VIEW_ONLY_BUTTON_IDS) {
    document.getElementById(id).classList.toggle("hidden", state.view !== "all");
  }
  for (const id of POOL_VIEW_ONLY_BUTTON_IDS) {
    document.getElementById(id).classList.toggle("hidden", state.view !== "pool");
  }
}

function switchView(view) {
  if (state.view === view) return;
  state.view = view;
  state.sort = state.sortByView[state.view];
  state.filters = state.filtersByView[state.view];
  state.selected = state.selectedByView[state.view];
  updateViewTabs();
  refreshFilterOptions();
  resyncRangeFilterInputs();
  closeAllFilterPanels();
  renderTable();
}

document.getElementById("tab-view-all").addEventListener("click", () => switchView("all"));
document.getElementById("tab-view-pool").addEventListener("click", () => switchView("pool"));

// Resets only the filters for the view currently on screen - the other
// view's remembered filters are left untouched.
document.getElementById("btn-reset-filters").addEventListener("click", () => {
  state.filtersByView[state.view] = {};
  state.filters = state.filtersByView[state.view];
  refreshFilterOptions();
  resyncRangeFilterInputs();
  closeAllFilterPanels();
  renderTable();
});

document.addEventListener("click", () => closeAllFilterPanels());

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
  const keys = visibleSelectedKeys();
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

document.getElementById("btn-add-to-pool").addEventListener("click", async () => {
  const keys = visibleSelectedKeys();
  if (!keys.length) {
    alert(t("alert.select_pool"));
    return;
  }
  await fetchJSON("/api/forward/add", {
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
  const keys = visibleSelectedKeys();
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
  const keys = visibleSelectedKeys();
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
  keys.forEach((k) => state.selected.delete(k));
  alert(t("alert.deleted", { count: result.removed }));
  await loadProxies();
  await loadForwardStatus();
});

// Delegated on the (never-replaced) tbody, since its rows are rebuilt on
// every renderTable() - this covers the per-row add/remove button in the
// pool column without needing to re-attach a listener on every render.
document.getElementById("proxy-tbody").addEventListener("click", async (e) => {
  const btn = e.target.closest(".pool-action-btn");
  if (!btn) return;
  const key = btn.dataset.key;
  const action = btn.dataset.action;
  btn.disabled = true;
  try {
    await fetchJSON(action === "add" ? "/api/forward/add" : "/api/forward/remove", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ keys: [key] }),
    });
    await loadProxies();
    await loadForwardStatus();
  } catch (err) {
    alert(err.message);
    btn.disabled = false;
  }
});

async function boot() {
  await i18nInit();
  loadSortPref();
  buildFilterGrid();
  updateViewTabs();
  await loadProxies();
  await loadForwardStatus();
  checkJobOnLoad();
  setInterval(loadForwardStatus, 8000);
  setInterval(() => {
    if (!jobPollTimer) checkJobOnLoad();
  }, 8000);
}
boot();
