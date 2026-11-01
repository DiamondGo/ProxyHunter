"""Pure regression baseline, first run against the original synchronous sources."""

from datetime import datetime

import pytest

from proxyhunter.forwarder.pool import ProxyPool
from proxyhunter.models import Proxy
from proxyhunter.scrapers import dedupe
from proxyhunter.scrapers.fallback import proxy_url, select_fallback_candidates
from proxyhunter.scrapers.freeproxy_world import FreeProxyWorldScraper
from proxyhunter.scrapers.proxyscrape import ProxyScrapeScraper


def proxy(port=8000, **kwargs):
    return Proxy("127.0.0.1", port, kwargs.pop("protocol", "http"), "test", **kwargs)


def test_model_keys_dict_and_timestamp():
    p = proxy()
    assert p.key() == ("http", "127.0.0.1", 8000)
    assert p.key_str() == "http|127.0.0.1|8000"
    p.mark_checked()
    assert datetime.fromisoformat(p.checked_at).utcoffset().total_seconds() == 0
    assert p.to_dict()["port"] == 8000


@pytest.mark.parametrize(
    "protocol,scheme", [("http", "http"), ("https", "http"), ("socks4", "socks4"), ("socks5", "socks5")]
)
def test_model_proxy_urls(protocol, scheme):
    assert proxy(protocol=protocol).proxy_url() == f"{scheme}://127.0.0.1:8000"


def test_dedupe_keeps_last_metadata():
    first, last = proxy(country="old"), proxy(country="new")
    assert dedupe([first, proxy(8001), last]) == [last, proxy(8001)]


def test_pool_round_robin_and_distinct_candidates():
    pool = ProxyPool()
    a, b, c = proxy(1), proxy(2), proxy(3)
    pool.set_proxies([a, b, c])
    assert pool.pick_many(2) == [a, b]
    assert pool.pick_many(10) == [c, a, b]
    assert pool.pick() == c
    assert pool.pick_many(0) == []


def test_pool_consecutive_failure_threshold_and_success_reset():
    pool = ProxyPool(fail_threshold=2)
    p = proxy()
    pool.set_proxies([p])
    pool.report_result(p, False)
    assert pool.pick() == p
    pool.report_result(p, True)
    assert pool.get_status()[p.key_str()]["fail_count"] == 0
    pool.report_result(p, False)
    assert pool.has_usable_proxy()
    pool.report_result(p, False)
    assert pool.pick() is None
    assert not pool.has_usable_proxy()
    pool.report_result(p, True)
    assert pool.pick() == p
    assert pool.get_status()[p.key_str()] == {
        "failed": False,
        "fail_count": 0,
        "request_count": 5,
        "success_count": 2,
        "failure_count": 3,
    }


def test_pool_stale_reports_and_clear_remove():
    pool = ProxyPool(fail_threshold=1)
    old, new = proxy(1), proxy(2)
    pool.set_proxies([old])
    pool.set_proxies([new])
    pool.report_result(old, False)
    assert set(pool.get_status()) == {new.key_str()}
    pool.report_result(new, False)
    pool.clear_failed([new.key_str()])
    assert pool.pick() == new
    pool.remove_proxies([new.key_str()])
    pool.report_result(new, True)
    assert pool.get_status() == {}
    assert pool.pick_many(100) == []


def test_pool_dynamic_threshold():
    values = {"pool_fail_threshold": 2}
    pool = ProxyPool(settings=values)
    p = proxy()
    pool.set_proxies([p])
    values["pool_fail_threshold"] = 1
    pool.report_result(p, False)
    assert pool.pick() is None


def test_world_parser():
    html = """<table class="table"><tbody>
    <tr><td>bad</td></tr>
    <tr><td>1.2.3.4</td><td>oops</td><td></td><td></td><td></td><td></td><td></td></tr>
    <tr><td>1.2.3.4</td><td>8080</td><td><a title="Canada">CA</a></td><td>Toronto</td>
    <td></td><td><a>HTTP</a><a>SOCKS5</a></td><td>No</td></tr>
    <tr><td>2.3.4.5</td><td>80</td><td></td><td></td><td></td><td></td><td>Yes</td></tr>
    </tbody></table>"""
    results = list(FreeProxyWorldScraper()._parse(html))
    assert [p.protocol for p in results] == ["http", "socks5", "http"]
    assert results[0].country == "Canada"
    assert results[0].city == "Toronto"
    assert results[0].anonymity == "transparent"
    assert results[2].country is None
    assert results[2].anonymity is None


def test_proxyscrape_parser_original():
    # Same original cases, now the parser is independently synchronous.
    scraper = ProxyScrapeScraper(["http", "invalid"])
    assert scraper.protocols == ["http"]
    results = list(scraper._parse("1.2.3.4:80\n\nnoise\n1.1.1.1:bad\n 2.3.4.5:1080 \n", "http"))
    assert [(p.ip, p.port, p.protocol) for p in results] == [
        ("1.2.3.4", 80, "http"),
        ("2.3.4.5", 1080, "http"),
    ]


def test_fallback_selection_and_sorting():
    dead = proxy(1, alive=False, supports_https=True, latency_ms=1)
    no_connect = proxy(2, alive=True, latency_ms=1)
    slow = proxy(3, alive=True, supports_https=True, latency_ms=100)
    unknown = proxy(4, alive=True, protocol="socks4")
    fast = proxy(5, alive=True, protocol="socks5", latency_ms=2)
    original = [unknown, dead, slow, no_connect, fast]
    assert select_fallback_candidates(original) == [fast, slow, unknown]
    assert original[0] is unknown


@pytest.mark.parametrize(
    "protocol,scheme", [("http", "http"), ("https", "http"), ("socks4", "socks4"), ("socks5", "socks5h")]
)
def test_fallback_dns_urls(protocol, scheme):
    assert proxy_url(proxy(protocol=protocol)) == f"{scheme}://127.0.0.1:8000"
