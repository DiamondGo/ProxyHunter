from __future__ import annotations

import argparse
import csv
import json
import logging
import sys
from collections import Counter
from pathlib import Path

from proxyhunter.geolocate import fill_missing_geo
from proxyhunter.models import Proxy
from proxyhunter.scrapers import dedupe, scrape_all
from proxyhunter.scrapers.fallback import select_fallback_candidates
from proxyhunter.settings import DEFAULT_SETTINGS_FILE
from proxyhunter.store import DEFAULT_STATE_FILE, ProxyStore
from proxyhunter.validator import ProxyValidator

log = logging.getLogger("proxyhunter")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="proxyhunter",
        description="Scrape free proxy lists and validate them concurrently.",
    )
    parser.add_argument(
        "--sources",
        default="all",
        help="Comma-separated sources: freeproxy_world,proxyscrape or 'all' (default: all). Ignored with --revalidate.",
    )
    parser.add_argument(
        "--pages", type=int, default=3,
        help="Pages to scrape from freeproxy.world (default: 3). Ignored with --revalidate.",
    )
    parser.add_argument(
        "--protocols",
        default="http,socks4,socks5",
        help="Comma-separated protocols to request from proxyscrape.com (default: http,socks4,socks5). "
        "Ignored with --revalidate.",
    )
    parser.add_argument("--workers", type=int, default=50, help="Concurrent validation workers (default: 50)")
    parser.add_argument("--timeout", type=float, default=8.0, help="Per-request timeout in seconds (default: 8)")
    parser.add_argument("--limit", type=int, default=None, help="Cap number of proxies validated (for testing)")
    parser.add_argument(
        "--no-secondary-check",
        action="store_true",
        help="Skip the secondary cross-site confirmation request",
    )
    parser.add_argument(
        "--no-geo-lookup",
        action="store_true",
        help="Skip filling in missing country/city via a direct (non-proxied) ip-api.com lookup",
    )
    parser.add_argument(
        "--geo-verify-via-proxy",
        action="store_true",
        help="Confirm country/city/isp by routing a geolocation lookup THROUGH each proxy (overrides "
        "whatever the source site or --geo-lookup said). More accurate but adds one request per alive "
        "proxy, so validation is slower. Off by default.",
    )
    parser.add_argument("--output-dir", default="./output", help="Directory to write result files (default: ./output)")
    parser.add_argument(
        "--state-file",
        default=DEFAULT_STATE_FILE,
        help=f"Path to the persistent state file tracking every proxy ever checked (default: ./{DEFAULT_STATE_FILE})",
    )
    parser.add_argument(
        "--recheck-after",
        type=float,
        default=6.0,
        help="Full-scrape mode only: skip re-validating a proxy whose last check (alive OR dead) is younger "
        "than this many hours - the cached result is reused instead. This is what avoids re-validating "
        "proxies that recently failed. Set to 0 to always re-check everything (default: 6)",
    )
    parser.add_argument(
        "--revalidate",
        action="store_true",
        help="Skip scraping entirely; just re-check every proxy already known in --state-file "
        "(from previous runs) and refresh the output files.",
    )
    parser.add_argument(
        "--min-age",
        type=float,
        default=0.0,
        help="--revalidate only: only re-check proxies whose cached result is older than this many hours; "
        "0 re-checks all known proxies regardless of when they were last checked (default: 0)",
    )
    parser.add_argument(
        "--serve",
        action="store_true",
        help="Launch the local web dashboard plus the local HTTP/SOCKS5 forwarding proxies, instead of "
        "scraping. The dashboard lets you trigger a full scrape, validate selected proxies, and choose "
        "which proxies the local forwarders route traffic through.",
    )
    parser.add_argument("--ui-host", default="127.0.0.1", help="Host for the web dashboard (default: 127.0.0.1)")
    parser.add_argument("--ui-port", type=int, default=9527, help="Port for the web dashboard (default: 9527)")
    parser.add_argument(
        "--forward-host",
        default="127.0.0.1",
        help="Host the local HTTP/SOCKS5 forward proxies bind to (default: 127.0.0.1, i.e. loopback only)",
    )
    parser.add_argument(
        "--http-proxy-port", type=int, default=9528, help="Port for the local HTTP forward proxy (default: 9528)"
    )
    parser.add_argument(
        "--socks-port", type=int, default=9529, help="Port for the local SOCKS5 forward proxy (default: 9529)"
    )
    parser.add_argument(
        "--settings-file",
        default=DEFAULT_SETTINGS_FILE,
        help=f"--serve only: persistent settings file (default: ./{DEFAULT_SETTINGS_FILE}). Once this file "
        "exists it's the source of truth for --serve; the flags above only seed it the first time it's "
        "created. Edit settings afterwards from the dashboard's Settings page.",
    )
    parser.add_argument("-v", "--verbose", action="store_true", help="Verbose logging")
    return parser


def write_outputs(proxies: list[Proxy], output_dir: Path) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    ordered = sorted(proxies, key=lambda p: (p.latency_ms is None, p.latency_ms))

    json_path = output_dir / "proxies_valid.json"
    json_path.write_text(json.dumps([p.to_dict() for p in ordered], indent=2, ensure_ascii=False))

    csv_path = output_dir / "proxies_valid.csv"
    with csv_path.open("w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(
            ["ip", "port", "protocol", "source", "country", "city", "isp",
             "latency_ms", "supports_https", "https_only", "anonymity", "checked_at"]
        )
        for p in ordered:
            writer.writerow(
                [p.ip, p.port, p.protocol, p.source, p.country, p.city, p.isp,
                 p.latency_ms, p.supports_https, p.https_only, p.anonymity, p.checked_at]
            )

    txt_path = output_dir / "proxies_valid.txt"
    txt_path.write_text("\n".join(p.proxy_url() for p in ordered) + ("\n" if ordered else ""))

    log.info("wrote %d valid proxies to %s", len(ordered), output_dir)


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    if args.serve:
        from proxyhunter.webapp import run_server

        sources = ["freeproxy_world", "proxyscrape"] if args.sources == "all" else args.sources.split(",")
        protocols = [p.strip() for p in args.protocols.split(",") if p.strip()]
        cli_seed = {
            "sources": sources,
            "pages": args.pages,
            "protocols": protocols,
            "workers": args.workers,
            "timeout": args.timeout,
            "secondary_check": not args.no_secondary_check,
            "geo_lookup": not args.no_geo_lookup,
            "geo_verify_via_proxy": args.geo_verify_via_proxy,
            "recheck_after": args.recheck_after,
            "limit": args.limit,
            "ui_host": args.ui_host,
            "ui_port": args.ui_port,
            "forward_host": args.forward_host,
            "http_proxy_port": args.http_proxy_port,
            "socks_port": args.socks_port,
            "state_file": args.state_file,
        }
        run_server(settings_file=Path(args.settings_file), cli_seed=cli_seed)
        return 0

    store = ProxyStore(Path(args.state_file))
    scraped_total = unique_total = None
    reused_alive: list[Proxy] = []

    if args.revalidate:
        to_check = store.all_known_proxies()
        if args.min_age > 0:
            to_check = [p for p in to_check if (store.age_hours(p) or 0) >= args.min_age]
        log.info(
            "revalidate mode: %d known proxies loaded from %s (min-age=%sh)",
            len(to_check), args.state_file, args.min_age,
        )
    else:
        sources = ["freeproxy_world", "proxyscrape"] if args.sources == "all" else args.sources.split(",")
        protocols = [p.strip() for p in args.protocols.split(",") if p.strip()]

        log.info("scraping sources=%s pages=%d protocols=%s", sources, args.pages, protocols)
        fallback_proxies = select_fallback_candidates(store.all_known_proxies())
        scraped = scrape_all(sources, pages=args.pages, protocols=protocols, fallback_proxies=fallback_proxies)
        unique = dedupe(scraped)
        scraped_total, unique_total = len(scraped), len(unique)
        log.info("scraped %d proxies, %d unique", scraped_total, unique_total)

        if not unique:
            log.error("no proxies scraped, nothing to validate")
            return 1

        to_check, reused_alive = store.split_fresh(unique, args.recheck_after)
        skipped_dead = unique_total - len(to_check) - len(reused_alive)
        log.info(
            "%d reused from cache (alive, unchanged), %d skipped (recently confirmed dead), %d need (re)validation",
            len(reused_alive), skipped_dead, len(to_check),
        )

    if args.limit:
        to_check = to_check[: args.limit]

    if not to_check and not reused_alive:
        log.error("nothing to validate")
        return 1

    validator = ProxyValidator(
        timeout=args.timeout,
        workers=args.workers,
        secondary_check=not args.no_secondary_check,
        geo_verify_via_proxy=args.geo_verify_via_proxy,
    )
    freshly_checked: list[Proxy] = []
    if to_check:
        log.info("validating %d proxies with %d workers...", len(to_check), args.workers)
        freshly_checked = validator.validate_all(to_check)
        store.upsert_all(freshly_checked)

    fresh_alive = [p for p in freshly_checked if p.alive]
    alive = fresh_alive + reused_alive

    if not args.no_geo_lookup:
        fill_missing_geo(alive)

    write_outputs(alive, Path(args.output_dir))
    store.save()

    anonymity_counts = Counter(p.anonymity or "unknown" for p in alive)
    https_count = sum(1 for p in alive if p.supports_https)

    print("\n=== Summary ===", file=sys.stderr)
    if not args.revalidate:
        print(f"scraped: {scraped_total}  unique: {unique_total}", file=sys.stderr)
    print(f"reused from cache without re-checking: {len(reused_alive)}", file=sys.stderr)
    print(
        f"freshly checked: {len(freshly_checked)} (alive: {len(fresh_alive)}, dead: {len(freshly_checked) - len(fresh_alive)})",
        file=sys.stderr,
    )
    print(f"total alive in output: {len(alive)}", file=sys.stderr)
    print(f"https support: {https_count}/{len(alive)}", file=sys.stderr)
    print(f"anonymity: {dict(anonymity_counts)}", file=sys.stderr)
    print(f"state file: {args.state_file} ({len(store.records)} known proxies total)", file=sys.stderr)

    return 0


if __name__ == "__main__":
    sys.exit(main())
