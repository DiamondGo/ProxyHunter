"""Async JSON I/O for stores used on one application event loop.

All filesystem work and JSON encoding/decoding runs in a worker. Writers (and
reads/flush barriers) are FIFO-ordered by absolute, lexically normalized path
within an event loop, including across store instances. This is not a database
transaction or an inter-process/cross-event-loop lock; symlink aliases are not
resolved. Callers must supply detached snapshots and await shutdown/flush before
closing their event loop.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import tempfile
import weakref
from collections.abc import Callable
from pathlib import Path
from typing import TypeVar

log = logging.getLogger(__name__)
T = TypeVar("T")

# Neither idle paths nor closed event loops are retained by this registry.
_locks: weakref.WeakKeyDictionary = weakref.WeakKeyDictionary()


def _path_lock(path: Path) -> asyncio.Lock:
    loop = asyncio.get_running_loop()
    locks = _locks.setdefault(loop, weakref.WeakValueDictionary())
    key = os.path.normcase(os.path.abspath(path))
    lock = locks.get(key)
    if lock is None:
        lock = locks[key] = asyncio.Lock()
    return lock


async def _worker(function: Callable[..., T], *args) -> T:
    """Delay cancellation until the worker stops touching the file.

    Shielding alone is insufficient: it lets the caller release its ordering
    lock while the thread is still writing. Repeated cancellation is absorbed
    until completion, then re-raised. Retrieve worker errors even when cancelled
    so there are no orphaned exceptions; cancellation wins in that case.
    """
    task = asyncio.create_task(asyncio.to_thread(function, *args))
    cancelled = None
    while not task.done():
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError as exc:
            cancelled = exc
        except Exception:
            if cancelled is None:
                raise
    try:
        result = task.result()
    except Exception as exc:
        if cancelled is not None:
            raise cancelled from exc
        raise
    if cancelled is not None:
        raise cancelled
    return result


def _reject_constant(value: str):
    raise ValueError(f"non-finite JSON number: {value}")


def _read_json(path: Path, normalize: Callable[[object], T]) -> tuple[T, bool]:
    try:
        text = path.read_text(encoding="utf-8")
    except FileNotFoundError:
        return normalize(None), False
    except UnicodeError as exc:
        log.warning("could not decode JSON file %s, starting fresh: %s", path, exc)
        return normalize(None), True
    # Missing files are normal; permission/directory/other path errors are not.
    try:
        data = json.loads(text, parse_constant=_reject_constant)
    except (ValueError, RecursionError) as exc:
        log.warning("could not decode JSON file %s, starting fresh: %s", path, exc)
        data = None
    return normalize(data), True


def _atomic_write(path: Path, snapshot: object) -> None:
    # Serialize first: invalid values cannot damage the last good file.
    text = json.dumps(snapshot, indent=2, ensure_ascii=False, allow_nan=False)
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            dir=path.parent,
            prefix=f".{path.name}.",
            suffix=".tmp",
            delete=False,
        ) as stream:
            temporary = Path(stream.name)
            stream.write(text)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        if temporary is not None:
            try:
                temporary.unlink(missing_ok=True)
            except OSError:
                log.warning("could not remove temporary file %s", temporary, exc_info=True)


class JSONFile:
    """A lazy, ordered async disk boundary. Construction performs no I/O."""

    def __init__(self, path: Path):
        self.path = path

    async def load(self, normalize: Callable[[object], T]) -> tuple[T, bool]:
        async with _path_lock(self.path):
            return await _worker(_read_json, self.path, normalize)

    async def save(self, snapshot: object) -> None:
        async with _path_lock(self.path):
            await _worker(_atomic_write, self.path, snapshot)

    async def flush(self) -> None:
        """Wait for operations already queued for this path; do not save state."""
        async with _path_lock(self.path):
            pass
