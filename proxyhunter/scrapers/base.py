from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Iterable

from proxyhunter.models import Proxy


class BaseScraper(ABC):
    name: str = "base"

    @abstractmethod
    def fetch(self, fallback_proxies: list[Proxy] | None = None) -> Iterable[Proxy]:
        """Yield Proxy objects scraped from the source.

        fallback_proxies, if given, are already-validated proxies to retry
        through (up to a few of them) when this source can't be reached
        directly - e.g. because it's blocked/unreachable from this machine.
        """
        raise NotImplementedError
