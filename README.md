Language: **English** | [中文](README.zh.md)

# ProxyHunter — Async Python Proxy Toolkit

ProxyHunter is an **asynchronous Python toolkit powered by asyncio** for free-proxy scraping, validation, and local forwarding. It asynchronously fetches proxy lists from several free proxy sites, concurrently validates each proxy's liveness, speed, geolocation, HTTPS support and anonymity level, and ships a local web dashboard that lets you route local HTTP/SOCKS5 traffic straight through the proxies that pass validation.

## Features

- Async-first architecture: network requests, validation, the web dashboard, forwarding, and scheduling share one managed asyncio event loop, with bounded validation concurrency and backpressured forwarding
- Scrapes proxy lists from [freeproxy.world](https://www.freeproxy.world/) and [proxyscrape.com](https://proxyscrape.com/)
- Concurrently validates liveness, latency, HTTPS support, and anonymity (whether the proxy leaks your real IP), with an optional secondary cross-site check to cut down on false positives
- Auto-fills geolocation (country/city/ISP) for validated proxies
- Local web dashboard (default port 9527): browse, filter, and sort proxies, trigger scrapes/validation manually, manage the forward pool
- Local forward proxies: HTTP (default port 9528) and SOCKS5 (default port 9529), with automatic load balancing and failover across the proxies in the pool
- Scheduled tasks (each independently toggleable and configurable from the settings page):
  1. Periodic full scrape + validation
  2. Periodically add the lowest-latency proxies to the forward pool
  3. Auto top-up the forward pool with the lowest-latency proxies when it runs short
  4. Periodically re-validate already-known proxies
- UI available in Chinese and English (switchable from the settings page; language resources are separate files, making it easy to add more languages later)
- All settings persist to a local JSON file

## Contents

- [ProxyHunter — Async Python Proxy Toolkit](#proxyhunter--async-python-proxy-toolkit)
  - [Features](#features)
  - [Contents](#contents)
  - [Installation](#installation)
  - [Quick start](#quick-start)
  - [Usage](#usage)
    - [Option 1: One-shot scrape + output result files](#option-1-one-shot-scrape--output-result-files)
    - [Option 2: Re-validate known proxies only](#option-2-re-validate-known-proxies-only)
    - [Option 3: Run the local web dashboard + forward proxies](#option-3-run-the-local-web-dashboard--forward-proxies)
  - [Settings and scheduled tasks](#settings-and-scheduled-tasks)
  - [About the data files](#about-the-data-files)
  - [Troubleshooting](#troubleshooting)
  - [Async architecture and lifecycle](#async-architecture-and-lifecycle)
    - [Forwarding boundaries](#forwarding-boundaries)
  - [Development and testing](#development-and-testing)

## Installation

Requires Python 3.10+. Runtime dependencies are aiohttp, aiohttp-socks, python-socks with its asyncio extra, and BeautifulSoup. Requests, Flask, and PySocks are no longer used.
You also need Git to clone the repository and internet access to install dependencies and reach proxy-list/checking services. Run the commands below from a terminal; no package build or separate frontend setup is required.

```bash
git clone https://github.com/DiamondGo/ProxyHunter.git
cd ProxyHunter
python3 -m venv .venv
source .venv/bin/activate   # on Windows: .venv\Scripts\activate
python -m pip install -r requirements.txt
```

On Windows PowerShell, replace the environment creation/activation commands with:

```powershell
py -3 -m venv .venv
.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
```

If activation is unavailable, invoke `.venv/bin/python` on Linux/macOS or `.venv\Scripts\python.exe` on Windows directly instead of `python`; activation is only a convenience. Use `python --version` to verify the interpreter is at least 3.10. Subsequent examples assume an activated environment and the repository root as the current directory.

## Quick start

This is the recommended path if you want to use proxies in a browser or another local application:

1. Start the dashboard and both local forwarders in one terminal:

   ```bash
   python -m proxyhunter --serve
   ```

   Keep this process running. Starting the service does **not** automatically scrape or populate a new forward pool: scheduled tasks are disabled by default.
2. Open **http://127.0.0.1:9527**. The default README language is English, but a new dashboard defaults to Chinese. To change it, open **Settings / 设置 → Interface Settings / 界面设置 → Interface language / 界面语言**, select **English**, and click **Save Settings / 保存设置**. Existing installations retain their saved language.
3. Click **Full Scrape + Validate** and wait for the progress panel to finish. The first run needs access to at least one list source and the validation services. Free proxies frequently fail; a small number of usable results is normal.
4. In **All Proxies**, use **Filters** to select alive proxies, prefer HTTPS support for HTTPS destinations, and sort by latency. Tick one or more rows and click **Add to Forward Pool**. Ticking a checkbox alone does not enable forwarding.
5. Open **Forward Pool** and confirm that **Local Forward Proxies** reports at least one **usable** proxy. The service restores a previously saved pool on later starts, but those proxies may need revalidation.
6. Configure your application's HTTP/HTTPS proxy as host **127.0.0.1**, port **9528**, or its SOCKS5 proxy as host **127.0.0.1**, port **9529**. Leave proxy credentials empty. Port **9527 is the dashboard, not a traffic proxy**.

To verify forwarding, run either command in a **second terminal** after adding a usable proxy:

```bash
# HTTPS traffic through the local HTTP proxy (CONNECT)
curl --connect-timeout 10 --max-time 30 --proxy http://127.0.0.1:9528 "https://api.ipify.org?format=json"
# SOCKS5 with destination DNS resolution through the local proxy
curl --connect-timeout 10 --max-time 30 --proxy socks5h://127.0.0.1:9529 "https://api.ipify.org?format=json"
```

These examples require curl; use `curl.exe` in Windows PowerShell to avoid its legacy alias. A successful response contains the observed exit IP. A response does not prove anonymity; transparent proxies can still expose your real IP. If forwarding fails, revalidate or replace the upstreams before retrying. There is **no direct-connection fallback** when the pool is empty or unusable.

ProxyHunter is an application-level TCP proxy, **not a VPN or system-wide tunnel**. Only applications explicitly configured to use it send traffic through it; UDP is not supported. Press **Ctrl+C** in the service terminal to stop it cleanly. Settings, known proxies, and pool selection persist; in-memory forwarding counters reset on restart.

## Usage

Choose one mode: a batch scrape, revalidation of saved proxies, or the long-running dashboard. Batch modes export files and exit; dashboard jobs update saved state but do **not** automatically generate the batch export files.

### Option 1: One-shot scrape + output result files

```bash
python -m proxyhunter --pages 3 --output-dir ./output
```

When it finishes, `./output` contains:

- `proxies_valid.json` — full fields (latency, geolocation, HTTPS support, anonymity, etc.)
- `proxies_valid.csv`
- `proxies_valid.txt` — one `protocol://ip:port` per line, ready to use as a `--proxy` value

Common flags:

| Flag | Description | Default |
| --- | --- | --- |
| `--sources` | `freeproxy_world`, `proxyscrape`, or both comma-separated; `all` selects both | `all` |
| `--pages` | Pages to scrape from freeproxy.world | `3` |
| `--protocols` | Protocols to request from proxyscrape.com | `http,socks4,socks5` |
| `--workers` | Maximum concurrent asynchronous proxy checks, not OS threads | `50` |
| `--timeout` | Per-request timeout (seconds) | `8` |
| `--limit` | Cap fresh checks, not reused cached results; use a positive number for a trial run | unlimited |
| `--no-secondary-check` | Skip the secondary check; may increase false positives | flag unset: check enabled |
| `--no-geo-lookup` | Skip direct geolocation auto-fill | flag unset: lookup enabled |
| `--geo-verify-via-proxy` | Confirm country/city/ISP through each proxy exit; adds requests | disabled |
| `--output-dir` | Output directory for result files | `./output` |
| `--state-file` | Persistent state file for known proxies | `./proxyhunter_state.json` |
| `--recheck-after` | Reuse-recent-results window in hours | `6` |

For a small fresh trial run, or a slower but more detailed exit-location check:

```bash
python -m proxyhunter --sources proxyscrape --protocols http --workers 20 --timeout 5 --limit 50 --recheck-after 0 --no-geo-lookup --output-dir ./output/quick
python -m proxyhunter --pages 5 --geo-verify-via-proxy --output-dir ./output/detailed
```

`--pages` only affects freeproxy.world. `--protocols` only affects proxyscrape.com requests; it is **not** a global filter on proxies from other sources. Both alive and dead checks are cached for six hours by default: fresh alive records are reused, and fresh dead records are skipped. Use `--recheck-after 0` to force checks for all newly scraped candidates. `--limit` limits checks, not scraping or the final total when cached alive records are reused.

`--timeout` controls individual connection/read waits, not the total job duration; each proxy can require multiple requests. Higher concurrency is not always faster when upstreams or checking services impose limits. Direct geolocation sends proxy IPs to ip-api.com; exit-location verification sends an additional request through each alive proxy. Disabling direct geo does not disable exit-location verification if both flags are supplied.

### Option 2: Re-validate known proxies only

```bash
python -m proxyhunter --revalidate
```

Skips scraping entirely and re-checks every proxy already recorded in `--state-file`.
Use `--min-age 6` to re-check only cached results at least six hours old; the default `--min-age 0` imposes no age restriction.
All modes accept `-v` / `--verbose`; see `python -m proxyhunter --help` for the complete interface.

```bash
python -m proxyhunter --revalidate --min-age 6 --state-file ./proxyhunter_state.json --output-dir ./output/rechecked
python -m proxyhunter --revalidate --min-age 0 --workers 20 --timeout 10 -v
```

| Flag | Description | Default |
| --- | --- | --- |
| `--revalidate` | Re-check saved records, including previously dead proxies, without scraping | off |
| `--min-age` | In revalidation mode, only check records at least this many hours old | `0` (all ages) |
| `-v` / `--verbose` | Enable diagnostic logging | off |
| `-h` / `--help` | Print all options and exit | — |

Revalidation uses `--min-age`, not the scrape-mode `--recheck-after` window. Source/page/protocol options do not apply when no scraping is performed. An empty/missing state file has no candidates to revalidate. Exit code **0** means processing completed, not that any proxy is alive; **1** indicates no candidates or an operational failure, **2** argument parsing errors, and **130** an interrupted batch run. A normal server shutdown can return 0. Check the summary and exported results to determine actual availability.

### Option 3: Run the local web dashboard + forward proxies

```bash
python -m proxyhunter --serve
```

Once running:

- Web dashboard: `http://127.0.0.1:9527`
- Local HTTP forward proxy: `127.0.0.1:9528`
- Local SOCKS5 forward proxy: `127.0.0.1:9529`

From the dashboard you can trigger a full scrape, validate selected proxies, add/remove proxies from the forward pool, configure scheduled tasks, switch the UI language, and edit scrape/network settings. Settings persist to `--settings-file` (default `./proxyhunter_settings.json`); network settings (listen address/ports) require a service restart to take effect, everything else applies immediately.
CLI flags seed the settings file only when it is first created; afterward the existing file wins. A running scrape uses its startup settings snapshot; subsequent jobs read the updated settings.

Common flags:

| Flag | Description | Default |
| --- | --- | --- |
| `--serve` | Run the dashboard, scheduler, and both local forwarders until stopped | off |
| `--ui-host` / `--ui-port` | Web dashboard listen address/port | `127.0.0.1` / `9527` |
| `--forward-host` | Local forward proxy listen address | `127.0.0.1` |
| `--http-proxy-port` | Local HTTP forward proxy port | `9528` |
| `--socks-port` | Local SOCKS5 forward proxy port | `9529` |
| `--settings-file` | Path to the persistent settings file | `./proxyhunter_settings.json` |

Then point your local application's proxy settings at `http://127.0.0.1:9528` (HTTP/HTTPS) or `socks5://127.0.0.1:9529` to route traffic through the forward pool.
All three listeners default to loopback only. The dashboard and forwarders have no authentication: do not expose them publicly by changing the bind address. Free upstream proxies must not be treated as trusted services.

To use different ports and separate state without changing an existing setup:

```bash
python -m proxyhunter --serve --settings-file ./proxyhunter_alternate_settings.json --state-file ./proxyhunter_alternate_state.json --ui-port 9537 --http-proxy-port 9538 --socks-port 9539
```

Use a new settings filename for initial seeding. If that file already exists, edit ports in **Settings**, click **Save Settings**, then **Restart Now**, rather than expecting CLI flags to override it. Reopen the dashboard at its new UI port after a change. `--state-file` still chooses the state path on each invocation; relative paths are relative to the current working directory. Do not run multiple instances against the same state/settings files.

## Settings and scheduled tasks

Open **Settings** and click **Save Settings** after editing. Scrape/validation options affect subsequent jobs; an already-running job keeps its initial snapshot. Pool failure/retry settings affect subsequent forwarding decisions. Listener host/port changes require **Restart Now** or a stop/start. Restarting closes active connections, so applications may need to reconnect.

All four scheduled tasks are **disabled initially** and only run while `--serve` remains active:

| Task | Configured default interval | Behavior when enabled |
| --- | --- | --- |
| Full scrape and validation | 24 hours | Discover candidates and validate them using current cache settings |
| Add lowest-latency proxies | 1 hour | Merge the top **5** alive proxies into the existing pool, deduplicated |
| Pool top-up | 1 hour | Try to restore at least **3** usable proxies using alive records not already in the pool |
| Revalidate known proxies | 12 hours | Re-check all saved proxies regardless of the scrape cache window |

The intervals, top-N count, and minimum pool size are configurable. Automatic pool additions require HTTPS support by default; the corresponding setting can be turned off. Pool additions use known records and cannot guarantee sufficient candidates or revive failed pool entries. Full scrape and scheduled revalidation are mutually exclusive; a busy job is not started twice. Due checks occur about every five seconds, not at an exact wall-clock time. A never-run task can start on the next due check after enabling it; saved last-run timestamps survive restart. Disabling a schedule stops future launches, not a job already in progress.

**Forward pool recovery and controls:**

- An upstream is skipped after **10 consecutive forwarding failures** by default. The retry limit is **3 additional distinct upstreams**, so at most four connection attempts are made when enough candidates exist. Retry occurs only before a client connection/request is committed, not after arbitrary payload has been sent.
- **Validate Selected** performs network checks; successful results clear pool failure marks. **Enable** only resets failure tracking so the upstream can be attempted again—it does not prove the proxy is healthy.
- **Remove Selected from Pool** / **Clear Pool** retain known-proxy records. **Delete Selected** removes those records and their pool membership; a later scrape can rediscover them.
- The **Alive** validation status and a pool **Failed** status measure different things. A cached alive proxy can subsequently fail real traffic. **443-only** means HTTP CONNECT is restricted to port 443; do not rely on that upstream for arbitrary SOCKS5 target ports.

## About the data files

`proxyhunter_state.json` (known proxies and validation results) and `proxyhunter_settings.json` (dashboard settings) are written to the current working directory by default. They are runtime personal data, listed in [`.gitignore`](.gitignore). The existing JSON schemas load without migration. Corrupt content logs warnings and falls back to valid records/defaults; filesystem errors such as permissions or directories are reported. Parent directories for custom state/settings paths must already exist; export directories are created recursively.

Batch exports contain **only alive results**, ordered by measured latency; state also retains dead results for caching and later revalidation. JSON records include protocol/address/source, `alive`, `latency_ms`, `supports_https`, `https_only`, `anonymity`, UTC `checked_at`, and available country/city/ISP information. CSV contains the corresponding export columns; TXT contains proxy URLs. `https_only` describes the non-443 CONNECT probe and can be unknown; anonymity may be `elite`, `anonymous`, `transparent`, or `unknown`. These are point-in-time checks, not uptime, bandwidth, security, or privacy guarantees.

Back up state/settings while the service is stopped. Reusing the same files restores known results and pool selection; starting with different filenames creates an independent setup. There is no dedicated CLI for importing arbitrary proxy lists.

## Troubleshooting

| Symptom | What to check |
| --- | --- |
| Import errors / missing dependencies | Use the same virtual-environment interpreter for installation and execution; rerun `python -m pip install -r requirements.txt`. Verify Python 3.10+. |
| Dashboard cannot be opened / connection refused | Keep `--serve` running and inspect its startup logs. Open the UI port, not a forwarding port; use the actual saved port if settings already exist. |
| Address already in use | Stop the other listener or select unused ports. For an existing settings file, change ports through Settings and restart, or use a new settings filename. |
| Proxy returns 502 / no usable pool | Finish a scrape, explicitly add alive proxies to the pool, and verify its usable count. Revalidate/replace failed entries. The service never silently routes directly instead. |
| Scrapes return no candidates | Confirm source connectivity. Scraping retries through up to three already-validated suitable proxies after direct access fails, but a new empty installation has no fallback candidates. Try the other source or repair connectivity; there is no guaranteed bootstrap bypass. |
| No fresh checks / empty output | Recent dead records may be cached; try `--recheck-after 0` for a scrape or `--revalidate --min-age 0` for saved records. Empty alive exports can also mean every checked candidate failed. |
| CLI flags appear ignored in server mode | Existing settings take precedence; edit them in the dashboard or select a new settings file. Batch flags do not override an already-running process. |
| Checks are slow / HTTPS fails | A proxy can require several timed requests. Try a smaller `--limit`, moderate `--workers`, or a different `--timeout`; check access to validation services. Select HTTPS-capable proxies and respect 443-only restrictions. Do not disable TLS verification to hide failures. |
| State/settings cannot be saved | Check write permissions and existing parent directories. Stop competing instances and retain backups; atomic writes are not cross-process coordination. |
| Dashboard does not reconnect after restart | If the UI host/port changed, open the new address manually. Read the terminal log for startup or save errors. |

For diagnostics, restart the same command with `--verbose`. Logs can contain proxy addresses, target hosts, and network details: redact sensitive information before sharing them. Do not send sensitive plaintext traffic through untrusted public proxies, and keep the unauthenticated listeners on loopback.

## Async architecture and lifecycle

- The CLI creates one managed asyncio event loop. Scraping, validation, geolocation, the aiohttp dashboard, HTTP/SOCKS5 forwarding, and scheduling share it; there are no threaded network servers or networking thread pools.
- Managed tasks retain strong references and allow at most one full scrape/revalidation job. Manual validation still returns completed results; long network calls leave `/api/job` responsive. Each of the four scheduled operations retains its persisted last-run timestamp.
- In-process removal revisions prevent validations/scrapes started before a deletion from writing stale results back. A later scrape can still rediscover that proxy; the on-disk JSON schema is unchanged.
- **Disk work still uses worker threads**: file access, JSON encoding/decoding, and exports are offloaded through asyncio's default executor. This is not a threaded networking model. Short in-memory queries, settings reads, and pool operations remain synchronous on the event loop.
- State/settings writes use detached snapshots, ordered by normalized file path within the same event loop. A temporary file in the same directory is atomically replaced after writing. Cancellation waits for an already-started disk worker before releasing writer ordering, so a stale background write cannot overwrite a later save. This is not an inter-process database lock and does not merge multiple processes' changes.
- SIGINT, SIGTERM, cancellation, partial startup failure, and API restart close listeners, stop scheduling, cancel and join jobs/requests/connections, drain saves, and close Web resources. Restart sends JSON first, then reexecutes with the original process invocation only after outer lifecycle cleanup.
- For a successfully started server, shutdown also checkpoints final state, current settings, and actual pool selection after all mutation owners stop. This retains changes whose queued saves were canceled while waiting for a lock. Failed startup does not overwrite uninitialized data; checkpoint failure prevents reexec while remaining resources are still cleaned up.
- Embedded callers can await [`start_server()`](proxyhunter/lifecycle.py) and eventually close the returned owner, or use its async context manager. Nonpersistent port-zero overrides are supported. Async [`run_server()`](proxyhunter/lifecycle.py) accepts injected stop events, startup hooks, and restart callbacks, so tests never replace the test process. Use [`async_main()`](proxyhunter/cli.py) inside an existing loop rather than the synchronous CLI entry point.
- Store constructors do no I/O; callers await load/save/flush. Settings reads and store queries stay synchronous; see [`settings.py`](proxyhunter/settings.py) and [`store.py`](proxyhunter/store.py). Flush waits for queued saves; it does not save unsubmitted in-memory changes.

### Forwarding boundaries

Plain HTTP uses `Connection: close`. SOCKS-upstream rewriting supports one request/target, not cross-target persistent connections or full HTTP pipeline rewriting. HTTPS uses CONNECT; both `http` and `https` upstream labels mean a plaintext HTTP proxy, not TLS-to-proxy. SOCKS5 supports only no-auth CONNECT, not UDP, BIND, or authentication extensions. Payload counters represent bytes written and drained, not a guarantee of remote application consumption. See [`tests/NETWORKING.md`](tests/NETWORKING.md) for precise DNS, timeout, and retry contracts.

## Development and testing

Install [development dependencies](requirements-dev.txt) in the activated environment:

```bash
python -m pip install -r requirements-dev.txt
```

Run from the project root. Neither unit nor integration tests depend on public free proxies:

```bash
# Complete strict suite: unit, network component, and full application
PYTHONASYNCIODEBUG=1 .venv/bin/python -m pytest -q -W error
# Application unit/API tests
.venv/bin/python -m pytest tests/test_persistence.py tests/test_jobs_scheduler.py tests/test_webapp.py tests/test_cli.py tests/test_application_concurrency.py -q
# Lifecycle and end-to-end tests using real local sockets
PYTHONASYNCIODEBUG=1 .venv/bin/python -m pytest tests/test_lifecycle.py tests/test_integration.py -q -W error
# Whole-package statement and branch coverage, not just networking
PYTHONASYNCIODEBUG=1 .venv/bin/python -m pytest -q -W error --cov=proxyhunter --cov-report=term-missing
.venv/bin/ruff check proxyhunter tests
.venv/bin/ruff format --check proxyhunter tests
```

[`tests/network_fixtures.py`](tests/network_fixtures.py) owns local HTTP, TLS, CONNECT, and SOCKS4/5 services. Certificates/private keys in [`tests/certs`](tests/certs) are **test-only**, not production credentials. Tests cover legacy JSON compatibility, atomic failures/cancellation ordering, jobs/scheduling, every API, CLI subprocess exit codes, real scrape→HTTP/HTTPS validation→store→pool selection→payload through both forwarders, deletion racing with stale validation, startup failure, and restart draining. The strict suite has 659 passing tests on both Python 3.10.20 and Python 3.12.13; whole-package coverage is 98.53% statements, 96.01% branches, 98.04% combined (Python 3.12). A clean environment containing only the new dependencies also passed, confirming no reliance on the old synchronous libraries. See [`tests/NETWORKING.md`](tests/NETWORKING.md) for gaps. Local tests do not establish public proxy reliability, actual OS process replacement, or behavior on every platform.

[`GitHub Actions`](.github/workflows/tests.yml) runs the strict suite, coverage, lint, formatting, and dependency checks on Python 3.10/3.12 on pushes and pull requests. See the repository's Actions tab for the status of individual CI runs.
