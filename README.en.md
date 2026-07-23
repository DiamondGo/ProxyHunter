Language: English | [中文](README.md)

# ProxyHunter

ProxyHunter is a free-proxy scraping and validation tool. It scrapes proxy lists from several free proxy sites, concurrently validates each proxy's liveness, speed, geolocation, HTTPS support and anonymity level, and ships a local web dashboard that lets you route local HTTP/SOCKS5 traffic straight through the proxies that pass validation.

## Features

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

## Installation

Requires Python 3.10+.

```bash
git clone https://github.com/DiamondGo/ProxyHunter.git
cd ProxyHunter
python3 -m venv venv
source venv/bin/activate   # on Windows: venv\Scripts\activate
pip install -r requirements.txt
```

## Usage

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
| `--sources` | Comma-separated sources, or `all` | `all` |
| `--pages` | Pages to scrape from freeproxy.world | `3` |
| `--protocols` | Protocols to request from proxyscrape.com | `http,socks4,socks5` |
| `--workers` | Concurrent validation workers | `50` |
| `--timeout` | Per-request timeout (seconds) | `8` |
| `--limit` | Cap the number of proxies validated (for testing) | unlimited |
| `--no-secondary-check` | Disable the secondary cross-site check | enabled |
| `--no-geo-lookup` | Disable geolocation auto-fill | enabled |
| `--output-dir` | Output directory for result files | `./output` |
| `--state-file` | Persistent state file for known proxies | `./proxyhunter_state.json` |
| `--recheck-after` | Reuse-recent-results window in hours | `6` |

### Option 2: Re-validate known proxies only

```bash
python -m proxyhunter --revalidate
```

Skips scraping entirely and re-checks every proxy already recorded in `--state-file`.

### Option 3: Run the local web dashboard + forward proxies

```bash
python -m proxyhunter --serve
```

Once running:

- Web dashboard: `http://127.0.0.1:9527`
- Local HTTP forward proxy: `127.0.0.1:9528`
- Local SOCKS5 forward proxy: `127.0.0.1:9529`

From the dashboard you can trigger a full scrape, validate selected proxies, add/remove proxies from the forward pool, configure scheduled tasks, switch the UI language, and edit scrape/network settings. Settings persist to `--settings-file` (default `./proxyhunter_settings.json`); network settings (listen address/ports) require a service restart to take effect, everything else applies immediately.

Common flags:

| Flag | Description | Default |
| --- | --- | --- |
| `--ui-host` / `--ui-port` | Web dashboard listen address/port | `127.0.0.1` / `9527` |
| `--forward-host` | Local forward proxy listen address | `127.0.0.1` |
| `--http-proxy-port` | Local HTTP forward proxy port | `9528` |
| `--socks-port` | Local SOCKS5 forward proxy port | `9529` |
| `--settings-file` | Path to the persistent settings file | `./proxyhunter_settings.json` |

Then point your local application's proxy settings at `http://127.0.0.1:9528` (HTTP/HTTPS) or `socks5://127.0.0.1:9529` to route traffic through the forward pool.

## About the data files

`proxyhunter_state.json` (known proxies and validation results) and `proxyhunter_settings.json` (dashboard settings) are written to the project root by default. They're runtime personal data, already listed in `.gitignore`, and are never committed to the repository.
