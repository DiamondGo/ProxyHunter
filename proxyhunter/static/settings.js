const form = document.getElementById("settings-form");

function setCheckboxGroup(name, values) {
  form.querySelectorAll(`input[name="${name}"]`).forEach((el) => {
    el.checked = values.includes(el.value);
  });
}

function getCheckboxGroup(name) {
  return Array.from(form.querySelectorAll(`input[name="${name}"]:checked`)).map((el) => el.value);
}

function fmtLastRun(ts) {
  if (!ts) return t("sched.never_run");
  return t("sched.last_run", { time: new Date(ts * 1000).toLocaleString() });
}

function populateLanguageOptions() {
  const select = document.getElementById("ui-language-select");
  select.innerHTML = "";
  for (const lang of i18nLanguages) {
    const opt = document.createElement("option");
    opt.value = lang.code;
    opt.textContent = lang.name;
    select.appendChild(opt);
  }
}

function populateForm(settings) {
  setCheckboxGroup("sources", settings.sources || []);
  setCheckboxGroup("protocols", settings.protocols || []);
  form.pages.value = settings.pages ?? "";
  form.workers.value = settings.workers ?? "";
  form.timeout.value = settings.timeout ?? "";
  form.secondary_check.checked = !!settings.secondary_check;
  form.geo_lookup.checked = !!settings.geo_lookup;
  form.geo_verify_via_proxy.checked = !!settings.geo_verify_via_proxy;
  form.recheck_after.value = settings.recheck_after ?? "";
  form.limit.value = settings.limit ?? "";
  form.ui_host.value = settings.ui_host ?? "";
  form.ui_port.value = settings.ui_port ?? "";
  form.forward_host.value = settings.forward_host ?? "";
  form.http_proxy_port.value = settings.http_proxy_port ?? "";
  form.socks_port.value = settings.socks_port ?? "";
  form.ui_language.value = settings.ui_language ?? I18N_DEFAULT_LANG;

  form.sched_full_scrape_enabled.checked = !!settings.sched_full_scrape_enabled;
  form.sched_full_scrape_interval_hours.value = settings.sched_full_scrape_interval_hours ?? "";
  document.getElementById("sched-full-scrape-last-run").textContent = fmtLastRun(settings.sched_full_scrape_last_run);

  form.sched_pool_refresh_enabled.checked = !!settings.sched_pool_refresh_enabled;
  form.sched_pool_refresh_interval_hours.value = settings.sched_pool_refresh_interval_hours ?? "";
  form.sched_pool_top_n.value = settings.sched_pool_top_n ?? "";
  document.getElementById("sched-pool-refresh-last-run").textContent = fmtLastRun(settings.sched_pool_refresh_last_run);

  form.sched_pool_topup_enabled.checked = !!settings.sched_pool_topup_enabled;
  form.sched_pool_topup_interval_hours.value = settings.sched_pool_topup_interval_hours ?? "";
  form.sched_pool_topup_min_count.value = settings.sched_pool_topup_min_count ?? "";
  document.getElementById("sched-pool-topup-last-run").textContent = fmtLastRun(settings.sched_pool_topup_last_run);

  form.sched_revalidate_enabled.checked = !!settings.sched_revalidate_enabled;
  form.sched_revalidate_interval_hours.value = settings.sched_revalidate_interval_hours ?? "";
  document.getElementById("sched-revalidate-last-run").textContent = fmtLastRun(settings.sched_revalidate_last_run);
}

function showRestartBanner(text) {
  document.getElementById("restart-banner-text").textContent = text;
  document.getElementById("restart-banner").classList.remove("hidden");
}
function hideRestartBanner() {
  document.getElementById("restart-banner").classList.add("hidden");
}

async function loadSettings() {
  await i18nLoadLanguages();
  populateLanguageOptions();
  const data = await fetchJSON("/api/settings");
  await i18nSetLanguage(data.settings.ui_language || I18N_DEFAULT_LANG);
  populateForm(data.settings);
  if (data.restart_pending) {
    showRestartBanner(t("restart.network_pending"));
  } else {
    hideRestartBanner();
  }
}

function showSaveStatus(text, isError) {
  const el = document.getElementById("save-status");
  el.classList.remove("hidden");
  el.textContent = text;
  el.style.color = isError ? "var(--dead)" : "var(--text)";
}

form.addEventListener("submit", async (e) => {
  e.preventDefault();
  const payload = {
    sources: getCheckboxGroup("sources"),
    protocols: getCheckboxGroup("protocols"),
    pages: form.pages.value,
    workers: form.workers.value,
    timeout: form.timeout.value,
    secondary_check: form.secondary_check.checked,
    geo_lookup: form.geo_lookup.checked,
    geo_verify_via_proxy: form.geo_verify_via_proxy.checked,
    recheck_after: form.recheck_after.value,
    limit: form.limit.value === "" ? null : form.limit.value,
    ui_host: form.ui_host.value,
    ui_port: form.ui_port.value,
    forward_host: form.forward_host.value,
    http_proxy_port: form.http_proxy_port.value,
    socks_port: form.socks_port.value,
    sched_full_scrape_enabled: form.sched_full_scrape_enabled.checked,
    sched_full_scrape_interval_hours: form.sched_full_scrape_interval_hours.value,
    sched_pool_refresh_enabled: form.sched_pool_refresh_enabled.checked,
    sched_pool_refresh_interval_hours: form.sched_pool_refresh_interval_hours.value,
    sched_pool_top_n: form.sched_pool_top_n.value,
    sched_pool_topup_enabled: form.sched_pool_topup_enabled.checked,
    sched_pool_topup_interval_hours: form.sched_pool_topup_interval_hours.value,
    sched_pool_topup_min_count: form.sched_pool_topup_min_count.value,
    sched_revalidate_enabled: form.sched_revalidate_enabled.checked,
    sched_revalidate_interval_hours: form.sched_revalidate_interval_hours.value,
    ui_language: form.ui_language.value,
  };
  try {
    const result = await fetchJSON("/api/settings", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
    if (result.restart_required) {
      showSaveStatus(t("save.success_restart", { fields: result.changed_network_fields.join(", ") }), false);
      showRestartBanner(t("restart.network_saved"));
    } else {
      showSaveStatus(t("save.success"), false);
      hideRestartBanner();
    }
  } catch (err) {
    showSaveStatus(t("save.error", { error: err.message }), true);
  }
});

async function doRestart() {
  if (!confirm(t("restart.confirm"))) return;
  try {
    await fetchJSON("/api/restart", { method: "POST" });
  } catch (err) {
    // the connection may drop mid-response once exec() replaces the process - that's expected
  }
  showSaveStatus(t("restart.restarting"), false);

  let attempts = 0;
  const timer = setInterval(async () => {
    attempts += 1;
    try {
      await fetchJSON("/api/settings");
      clearInterval(timer);
      window.location.reload();
    } catch (err) {
      if (attempts > 30) {
        clearInterval(timer);
        showSaveStatus(t("restart.reconnect_failed"), true);
      }
    }
  }, 1000);
}

document.getElementById("btn-restart").addEventListener("click", doRestart);
document.getElementById("btn-restart-now").addEventListener("click", doRestart);
document.getElementById("ui-language-select").addEventListener("change", (e) => {
  i18nSetLanguage(e.target.value);
});

loadSettings();
