from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Iterable

from proxyhunter.models import Proxy


class BaseScraper(ABC):
    name: str = "base"

    @abstractmethod
    def fetch(self) -> Iterable[Proxy]:
        """Yield Proxy objects scraped from the source."""
        raise NotImplementedError
