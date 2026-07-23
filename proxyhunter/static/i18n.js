// Shared client-side i18n. Loaded before app.js/settings.js on every page.
// Adding a new language: drop `static/i18n/<code>.json` and add it to
// `static/i18n/languages.json` - no other code changes needed.

const I18N_DEFAULT_LANG = "zh";

let i18nDict = {};
let i18nLang = I18N_DEFAULT_LANG;
let i18nLanguages = [];

async function fetchJSON(url, options) {
  const resp = await fetch(url, options);
  if (!resp.ok) {
    const text = await resp.text();
    throw new Error(`${resp.status}: ${text}`);
  }
  return resp.json();
}

function t(key, params) {
  let s = i18nDict[key];
  if (s == null) return key;
  if (params) {
    for (const [k, v] of Object.entries(params)) {
      s = s.split(`{${k}}`).join(v);
    }
  }
  return s;
}

function applyStaticTranslations() {
  document.documentElement.lang = i18nLang === "zh" ? "zh-CN" : i18nLang;
  document.querySelectorAll("[data-i18n]").forEach((el) => {
    el.textContent = t(el.getAttribute("data-i18n"));
  });
  document.querySelectorAll("[data-i18n-title]").forEach((el) => {
    el.title = t(el.getAttribute("data-i18n-title"));
  });
  const titleKey = document.documentElement.getAttribute("data-i18n-doc-title");
  if (titleKey) document.title = t(titleKey);
}

async function i18nLoadLanguages() {
  if (i18nLanguages.length) return i18nLanguages;
  try {
    i18nLanguages = await fetchJSON("/static/i18n/languages.json");
  } catch (err) {
    i18nLanguages = [{ code: I18N_DEFAULT_LANG, name: "中文" }];
  }
  return i18nLanguages;
}

async function i18nSetLanguage(lang) {
  try {
    i18nDict = await fetchJSON(`/static/i18n/${lang}.json`);
  } catch (err) {
    i18nDict = {};
  }
  i18nLang = lang;
  applyStaticTranslations();
}

// Fetches the persisted language from settings and applies it. Pages that
// already fetch /api/settings themselves (the settings page) should call
// i18nSetLanguage(settings.ui_language) directly instead, to avoid a
// redundant request.
async function i18nInit() {
  let lang = I18N_DEFAULT_LANG;
  try {
    const data = await fetchJSON("/api/settings");
    if (data.settings && data.settings.ui_language) lang = data.settings.ui_language;
  } catch (err) {
    // settings not reachable yet - fall back to the default language
  }
  await i18nLoadLanguages();
  await i18nSetLanguage(lang);
  return lang;
}
