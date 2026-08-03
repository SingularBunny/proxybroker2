"""Tests for persisting the verified pool between runs.

The failure this guards against is not a crash. A pool file that is silently
mis-read, or one full of long-dead addresses, produces a run that looks healthy
and finds nothing — the same shape as the seventeen-hour run that prompted the
feature.
"""

import json
import time

import pytest

from proxybroker import Proxy
from proxybroker.pool_store import SCHEMA_VERSION, PoolStore


def _proxy(host="10.0.0.1", port=8080, types=("HTTP",), country="RU", resp=0.5):
    p = Proxy(host, port)
    p.types.update({t: None for t in types})
    p._geo = p._geo._replace(code=country)
    p._runtimes = [resp]
    return p


class TestSaveAndLoad:
    def test_round_trip(self, tmp_path):
        path = tmp_path / "pool.json"
        store = PoolStore(str(path))

        assert store.save([_proxy("10.0.0.1", 8080), _proxy("10.0.0.2", 3128)]) == 2

        assert set(store.load()) == {("10.0.0.1", "8080"), ("10.0.0.2", "3128")}

    def test_duplicates_are_collapsed(self, tmp_path):
        store = PoolStore(str(tmp_path / "pool.json"))
        assert store.save([_proxy("10.0.0.1"), _proxy("10.0.0.1")]) == 1

    def test_missing_file_is_not_an_error(self, tmp_path):
        assert PoolStore(str(tmp_path / "nope.json")).load() == []

    def test_disabled_when_no_path(self):
        store = PoolStore(None)
        assert not store.enabled
        assert store.save([_proxy()]) == 0
        assert store.load() == []


class TestFreshness:
    def test_expired_entries_are_dropped(self, tmp_path):
        """A day-old free proxy is almost certainly dead; re-checking it is waste."""
        path = tmp_path / "pool.json"
        path.write_text(json.dumps({
            "version": SCHEMA_VERSION,
            "proxies": [{
                "host": "10.0.0.1", "port": 8080, "types": ["HTTP"],
                "country": "RU", "avg_resp_time": 0.5,
                "verified_at": int(time.time()) - 100_000,
            }],
        }))

        assert PoolStore(str(path), ttl=3600).load() == []

    def test_freshest_first(self, tmp_path):
        path = tmp_path / "pool.json"
        now = int(time.time())
        path.write_text(json.dumps({
            "version": SCHEMA_VERSION,
            "proxies": [
                {"host": "10.0.0.1", "port": 1, "verified_at": now - 500},
                {"host": "10.0.0.2", "port": 2, "verified_at": now - 10},
            ],
        }))

        assert PoolStore(str(path)).load()[0] == ("10.0.0.2", "2")

    def test_country_filter(self, tmp_path):
        """Re-checking addresses the geo filter will reject anyway is wasted budget."""
        path = tmp_path / "pool.json"
        now = int(time.time())
        path.write_text(json.dumps({
            "version": SCHEMA_VERSION,
            "proxies": [
                {"host": "10.0.0.1", "port": 1, "country": "RU", "verified_at": now},
                {"host": "10.0.0.2", "port": 2, "country": "DE", "verified_at": now},
            ],
        }))

        assert PoolStore(str(path)).load(countries=["RU"]) == [("10.0.0.1", "1")]


class TestCorruptionIsSurvivable:
    """Persistence is an optimisation. Losing it must never cost the run."""

    def test_truncated_json_starts_cold(self, tmp_path):
        path = tmp_path / "pool.json"
        path.write_text('{"version": 1, "proxies": [{"host"')
        assert PoolStore(str(path)).load() == []

    def test_wrong_schema_version_is_ignored_not_half_parsed(self, tmp_path):
        """Half-reading an old layout would look like 'the proxies all died'."""
        path = tmp_path / "pool.json"
        path.write_text(json.dumps({
            "version": SCHEMA_VERSION + 99,
            "proxies": [{"host": "10.0.0.1", "port": 1, "verified_at": time.time()}],
        }))
        assert PoolStore(str(path)).load() == []

    def test_garbage_entries_are_skipped_not_fatal(self, tmp_path):
        path = tmp_path / "pool.json"
        now = int(time.time())
        path.write_text(json.dumps({
            "version": SCHEMA_VERSION,
            "proxies": [
                "не словарь",
                {"port": 1, "verified_at": now},           # без хоста
                {"host": "10.0.0.9", "port": 9, "verified_at": now},
            ],
        }))
        assert PoolStore(str(path)).load() == [("10.0.0.9", "9")]

    def test_unwritable_path_does_not_raise(self, tmp_path):
        store = PoolStore(str(tmp_path / "nope" / "\0bad" / "pool.json"))
        assert store.save([_proxy()]) == 0

    def test_write_is_atomic(self, tmp_path):
        """A run killed mid-write must not leave truncated JSON.

        Otherwise the next run starts from nothing precisely because the
        previous one had something worth keeping.
        """
        path = tmp_path / "pool.json"
        store = PoolStore(str(path))
        store.save([_proxy("10.0.0.1")])
        store.save([_proxy("10.0.0.2"), _proxy("10.0.0.3")])

        payload = json.loads(path.read_text())
        assert len(payload["proxies"]) == 2
        assert not list(tmp_path.glob("*.tmp")), "temporary file left behind"


class TestBrokerIntegration:
    @pytest.mark.asyncio
    async def test_verified_proxies_are_saved_on_stop(self, tmp_path):
        from proxybroker import Broker

        path = tmp_path / "pool.json"
        broker = Broker(
            timeout=0.1, max_tries=1, providers=[],
            stop_broker_on_sigint=False, pool_file=str(path),
        )
        broker._push_to_result(_proxy("10.0.0.7", 7777))
        broker.stop()

        assert PoolStore(str(path)).load() == [("10.0.0.7", "7777")]

    @pytest.mark.asyncio
    async def test_end_of_stream_marker_is_not_persisted(self, tmp_path):
        """`_done()` pushes None into the queue; it is not a proxy."""
        from proxybroker import Broker

        path = tmp_path / "pool.json"
        broker = Broker(
            timeout=0.1, max_tries=1, providers=[],
            stop_broker_on_sigint=False, pool_file=str(path),
        )
        broker._push_to_result(_proxy("10.0.0.8", 8888))
        broker._push_to_result(None)
        broker.stop()

        assert PoolStore(str(path)).load() == [("10.0.0.8", "8888")]

    @pytest.mark.asyncio
    async def test_saved_when_the_limit_ends_the_run(self, tmp_path):
        """`stop()` is not called by the CLI; `_done()` is the only reliable hook.

        Saving only in `stop()` meant a `find --limit N` run wrote nothing, and
        the autosave task was cancelled before its first tick. Caught on a live
        run, not in review.
        """
        from proxybroker import Broker

        path = tmp_path / "pool.json"
        broker = Broker(
            timeout=0.1, max_tries=1, providers=[],
            stop_broker_on_sigint=False, pool_file=str(path),
        )
        broker._limit = 1
        broker._push_to_result(_proxy("10.0.0.5", 5555))  # доводит limit до 0 → _done()

        assert PoolStore(str(path)).load() == [("10.0.0.5", "5555")]

    @pytest.mark.asyncio
    async def test_nothing_written_without_a_path(self, tmp_path):
        from proxybroker import Broker

        broker = Broker(
            timeout=0.1, max_tries=1, providers=[], stop_broker_on_sigint=False,
        )
        broker._push_to_result(_proxy())
        broker.stop()

        assert not list(tmp_path.iterdir())
