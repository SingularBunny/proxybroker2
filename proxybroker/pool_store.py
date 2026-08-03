"""Persist verified proxies between runs.

Why
---
Every run starts from an empty pool and re-derives it from scratch, even though
the previous run finished holding dozens of proxies it had already verified.
In the good case that costs one to three minutes of warm-up. In the bad case —
a country filter over free lists — a pool never assembled at all across a
seventeen-hour run.

What is actually slow is *discovery*, not *checking*. Walking ~38 provider lists
yields tens of thousands of candidates, of which a handful survive the geo
filter and the judges. Checking is fast and parallel. So the win is to start the
next run from the short list of addresses already known to have worked, instead
of re-deriving it from forty thousand candidates.

What this deliberately does not do
----------------------------------
Loaded proxies are **not** placed straight into the ready pool. Free proxies die
within hours, and a pool pre-filled with corpses is worse than an empty one: the
consumer gets addresses that fail on the first real request, and the failure
looks like a bug in the consumer. They are pushed through the normal check
instead — they skip discovery, not validation.

Format
------
A JSON object with a schema version and a list of entries. Version is checked on
load: a file written by an older layout is ignored rather than half-parsed,
because a silently mis-read pool would look like "the proxies all died".
"""

from __future__ import annotations

import json
import logging
import os
import tempfile
import time
from typing import Any, Dict, List, Optional, Tuple

log = logging.getLogger(__name__)

#: Bump when the entry layout changes incompatibly.
SCHEMA_VERSION = 1

#: Default lifetime of a stored entry. Free proxies rarely outlive a few hours;
#: a longer window mostly costs failed checks at the start of the next run.
DEFAULT_TTL = 6 * 3600


class PoolStore:
    """Read and write the set of proxies known to have worked.

    :param path: file to persist to. ``None`` disables persistence entirely.
    :param ttl: seconds after which an entry is considered too old to try.
    """

    def __init__(self, path: Optional[str], ttl: int = DEFAULT_TTL):
        self._path = path
        self._ttl = ttl

    @property
    def enabled(self) -> bool:
        return bool(self._path)

    # ------------------------------------------------------------------ #

    def save(self, proxies) -> int:
        """Write the given proxies. Returns how many entries were stored.

        The write goes to a temporary file in the same directory and is then
        renamed over the target. A run killed mid-write would otherwise leave
        truncated JSON, and the next run would start from nothing precisely
        because the previous one had something worth keeping.
        """
        if not self.enabled:
            return 0

        now = int(time.time())
        entries = []
        seen = set()
        for proxy in proxies:
            key = (proxy.host, proxy.port)
            if key in seen:
                continue
            seen.add(key)
            entries.append(
                {
                    "host": proxy.host,
                    "port": proxy.port,
                    "types": sorted(proxy.types),
                    "country": getattr(proxy.geo, "code", None),
                    "avg_resp_time": round(proxy.avg_resp_time, 3),
                    "verified_at": now,
                }
            )

        payload = {"version": SCHEMA_VERSION, "proxies": entries}
        directory = os.path.dirname(os.path.abspath(self._path)) or "."
        try:
            os.makedirs(directory, exist_ok=True)
            fd, tmp = tempfile.mkstemp(dir=directory, suffix=".tmp")
            with os.fdopen(fd, "w", encoding="utf-8") as fh:
                json.dump(payload, fh)
            os.replace(tmp, self._path)
        except (OSError, ValueError) as exc:
            # Persistence is an optimisation. Losing it must never cost the run.
            #
            # `ValueError` is not decoration: a path containing a null byte makes
            # `os.makedirs` raise that instead of `OSError`, and catching only the
            # latter would let an unusable path bring down a run that was
            # otherwise fine.
            log.warning(f"Could not save the proxy pool to {self._path}: {exc!r}")
            return 0

        log.debug(f"Saved {len(entries)} proxies to {self._path}")
        return len(entries)

    def load(self, countries: Optional[List[str]] = None) -> List[Tuple[str, str]]:
        """Return ``[(host, port), ...]`` worth re-checking, freshest first.

        Entries past the TTL are dropped. When *countries* is given, entries
        recorded from other countries are dropped too — re-checking them would
        spend the check budget on addresses the geo filter will reject anyway.
        """
        if not self.enabled:
            return []

        payload = self._read()
        if payload is None:
            return []

        version = payload.get("version")
        if version != SCHEMA_VERSION:
            log.warning(
                f"Ignoring the stored pool: schema version {version!r}, "
                f"expected {SCHEMA_VERSION}"
            )
            return []

        now = time.time()
        wanted = set(countries) if countries else None
        fresh: List[Dict[str, Any]] = []
        expired = 0
        for entry in payload.get("proxies", []):
            if not isinstance(entry, dict) or not entry.get("host"):
                continue
            age = now - entry.get("verified_at", 0)
            if age > self._ttl:
                expired += 1
                continue
            if wanted and entry.get("country") not in wanted:
                continue
            fresh.append(entry)

        fresh.sort(key=lambda e: e.get("verified_at", 0), reverse=True)
        log.info(
            f"Loaded {len(fresh)} proxies from {self._path} "
            f"({expired} expired, TTL {self._ttl}s)"
        )
        return [(e["host"], str(e["port"])) for e in fresh]

    # ------------------------------------------------------------------ #

    def _read(self) -> Optional[dict]:
        try:
            with open(self._path, encoding="utf-8") as fh:
                payload = json.load(fh)
        except FileNotFoundError:
            return None
        except (OSError, ValueError) as exc:
            # A corrupt file is a reason to start cold, not to crash.
            log.warning(f"Could not read the stored pool {self._path}: {exc!r}")
            return None
        return payload if isinstance(payload, dict) else None
