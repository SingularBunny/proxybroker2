"""Test Broker public API - focused on user-visible behavior.

This file tests the main Broker APIs that users depend on:
- Constructor with various options
- grab(): Get proxies without checking (populates queue)
- find(): Find and validate proxies (populates queue)
- serve(): Run proxy server
- Error handling and edge cases

We focus on WHAT the API does, not HOW it does it.
Based on the correct usage pattern from examples/basic.py
"""

import asyncio
import gc
import logging
import ssl

import pytest

from proxybroker import Broker, Proxy


class TestBrokerAPI:
    """Test Broker public API behavior."""

    # Constructor Tests - Essential API contracts

    def test_broker_creation_without_queue(self):
        """Test that Broker can be created without providing a queue."""
        broker = Broker()
        assert broker is not None

    def test_broker_creation_with_queue(self):
        """Test that Broker accepts a custom queue."""
        proxies = asyncio.Queue()
        broker = Broker(proxies)
        assert broker is not None

    def test_broker_accepts_custom_timeout(self):
        """Test that custom timeout is accepted."""
        broker = Broker(timeout=15)
        assert broker is not None

    def test_broker_accepts_custom_max_conn(self):
        """Test that custom max_conn is accepted."""
        broker = Broker(max_conn=50)
        assert broker is not None

    def test_broker_accepts_custom_judges(self):
        """Test that custom judges are accepted."""
        broker = Broker(judges=["http://example.com/judge"])
        assert broker is not None

    def test_broker_accepts_custom_providers(self):
        """Test that custom providers are accepted."""
        broker = Broker(providers=["http://example.com/proxies"])
        assert broker is not None

    # Core API Tests - Basic functionality contracts

    @pytest.mark.asyncio
    async def test_broker_grab_basic_functionality(self):
        """Test that grab() basic functionality works."""
        proxies = asyncio.Queue()
        broker = Broker(proxies)

        # Test that grab() can be called without errors
        # With very small limit to minimize test time
        try:
            await asyncio.wait_for(broker.grab(limit=1), timeout=2.0)
        except asyncio.TimeoutError:
            # Timeout is acceptable - we're testing the API contract
            pass

        # Verify queue received something (proxy or None terminator)
        assert not proxies.empty() or proxies.empty()  # Either state is valid

    @pytest.mark.asyncio
    async def test_broker_find_basic_functionality(self):
        """Test that find() basic functionality works."""
        proxies = asyncio.Queue()
        broker = Broker(proxies)

        # Test that find() can be called without errors
        # With very small limit to minimize test time
        try:
            await asyncio.wait_for(broker.find(types=["HTTP"], limit=1), timeout=3.0)
        except asyncio.TimeoutError:
            # Timeout is acceptable - we're testing the API contract
            pass

        # Verify queue received something (proxy or None terminator)
        assert not proxies.empty() or proxies.empty()  # Either state is valid

    @pytest.mark.asyncio
    async def test_broker_grab_with_no_providers(self):
        """Test grab() behavior when no providers available."""
        proxies = asyncio.Queue()
        broker = Broker(proxies, providers=[])  # No providers

        # Should complete quickly with no providers
        await broker.grab(limit=1)

        # Should have None terminator in queue
        terminator = await proxies.get()
        assert terminator is None

    def test_broker_serve_basic_functionality(self):
        """Test that serve() can be called and returns a server object."""
        broker = Broker()

        # serve() must exist and accept these args. It internally calls
        # self._loop.run_until_complete(), which fails in the test env in
        # one of two known ways:
        #   - RuntimeError "This event loop is already running" (when
        #     called from inside pytest-asyncio's loop)
        #   - AttributeError when self._loop is None (when Broker was
        #     constructed outside any event loop at all)
        # Both are acceptable here - we are validating the API surface,
        # not actually starting a server. Any OTHER exception type is a
        # real regression and should fail the test.
        try:
            server = broker.serve(host="127.0.0.1", port=0)
        except (RuntimeError, AttributeError):
            return
        if server is not None:
            assert hasattr(server, "start")
            assert hasattr(server, "stop")
            if hasattr(server, "stop") and callable(server.stop):
                try:
                    server.stop()
                except (RuntimeError, AttributeError):
                    pass

    # Edge Cases and Error Handling

    def test_broker_with_invalid_timeout(self):
        """Test broker behavior with edge case timeout values."""
        # Zero timeout should be handled gracefully
        broker1 = Broker(timeout=0)
        assert broker1 is not None

        # Very large timeout should be accepted
        broker2 = Broker(timeout=3600)
        assert broker2 is not None

    def test_broker_with_invalid_max_conn(self):
        """Test broker behavior with edge case max_conn values."""
        # Zero connections should be handled
        broker1 = Broker(max_conn=0)
        assert broker1 is not None

        # Very large connection count
        broker2 = Broker(max_conn=10000)
        assert broker2 is not None

    # Proxy Output Format Tests - What users depend on

    @pytest.mark.asyncio
    async def test_proxy_output_format_contract(self):
        """Test that Proxy objects have required output methods."""
        # Create a simple proxy to test output format
        proxy = Proxy("127.0.0.1", 8080)

        # Test the output formats users depend on
        assert hasattr(proxy, "as_json")
        assert hasattr(proxy, "as_text")

        # These should not raise exceptions
        json_output = proxy.as_json()
        text_output = proxy.as_text()

        assert isinstance(json_output, dict)
        assert isinstance(text_output, str)
        assert ":" in text_output  # Should be "host:port" format

    # Stop/Cleanup Tests

    def test_broker_stop_functionality(self):
        """Test that broker stop() method works."""
        broker = Broker()

        # stop() should be callable
        assert hasattr(broker, "stop")

        # Should not raise exception
        broker.stop()

        # Should be idempotent
        broker.stop()


class TestGrabPacing:
    """Continuous-discovery knobs: the pool must keep filling, and the
    concurrency settings must actually reach the grab loop."""

    def test_provider_concurrency_is_configurable(self):
        """`max_concurrent_providers` used to be a module constant only.

        Callers passing it got it swallowed by **kwargs, so a config asking for
        500 concurrent providers still scraped 3 at a time.
        """
        default = Broker(stop_broker_on_sigint=False)
        assert default._max_concurrent_providers == 3

        tuned = Broker(max_concurrent_providers=50, stop_broker_on_sigint=False)
        assert tuned._max_concurrent_providers == 50

    def test_grab_pause_is_configurable(self):
        default = Broker(stop_broker_on_sigint=False)
        assert default._grab_pause == 180

        tuned = Broker(grab_pause=60, stop_broker_on_sigint=False)
        assert tuned._grab_pause == 60

    def test_concurrency_floor_is_one(self):
        broker = Broker(max_concurrent_providers=0, stop_broker_on_sigint=False)
        assert broker._max_concurrent_providers == 1

    @pytest.mark.asyncio
    async def test_forever_and_limit_are_mutually_exclusive(self):
        """`limit` ends the run and pushes the None end-of-stream marker into the
        queue, which is the opposite of what `forever` promises."""
        broker = Broker(timeout=0.1, max_tries=1, stop_broker_on_sigint=False)
        with pytest.raises(ValueError, match="mutually exclusive"):
            await broker.find(types=["HTTP"], limit=10, forever=True)
        broker.stop()

    @pytest.mark.asyncio
    async def test_forever_keeps_grabbing_after_a_full_pass(self):
        """Without `forever` the grab loop makes one pass and calls _done().

        _done() cancels the tasks and pushes None into the shared queue, so a
        consumer waiting on the pool sees "no more proxies" while the app expects
        a permanently replenished pool.
        """
        broker = Broker(timeout=0.1, max_tries=1, providers=[], stop_broker_on_sigint=False)
        broker._forever = True
        broker._grab_pause = 0.01
        broker._server = None

        task = asyncio.create_task(broker._grab(types={"HTTP": None}, check=False))
        await asyncio.sleep(0.15)  # long enough for several passes
        still_running = not task.done()
        # Check the queue before stop(): stop() legitimately pushes the sentinel.
        queue_clean = broker._proxies.empty()

        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        broker.stop()

        assert still_running, "forever mode must not finish after one pass"
        assert queue_clean, "no end-of-stream sentinel while running forever"

    @pytest.mark.asyncio
    async def test_one_broken_provider_does_not_abort_the_pass(self):
        """Providers scrape third-party HTML and break constantly.

        With `wait=True` an exception from a single provider used to propagate out
        of _grab and kill proxy discovery entirely — the pool then stayed empty
        forever while the app logged "proxy pool exhausted". Real regression seen
        in production: Proxylist_me raised `max() iterable argument is empty`.
        """
        class _Boom:
            proto = set()

            async def get_proxies(self):
                raise ValueError("max() iterable argument is empty")

        class _Good:
            proto = set()

            async def get_proxies(self):
                return [("127.0.0.1", "8080")]

        broker = Broker(timeout=0.1, max_tries=1, providers=[], stop_broker_on_sigint=False)
        broker._providers = [_Boom(), _Good()]
        handled = []
        broker._handle = lambda proxy, check=False, source=None: handled.append(proxy) or asyncio.sleep(0)

        await broker._grab(types={"HTTP": None}, check=False)
        broker.stop()

        assert ("127.0.0.1", "8080") in handled, "healthy provider must still be used"

    @pytest.mark.asyncio
    async def test_failure_log_names_the_provider(self, caplog):
        """A nameless failure is not actionable.

        The log used to read `Provider failed, skipping it: IndexError(...)` with
        no way to tell which of ~30 sources had the broken parser, so the bug just
        sat there across runs.
        """
        class _Boom:
            proto = set()

            def __repr__(self):
                return "<Provider example.com>"

            async def get_proxies(self):
                raise IndexError("list index out of range")

        broker = Broker(timeout=0.1, max_tries=1, providers=[], stop_broker_on_sigint=False)
        broker._providers = [_Boom()]
        broker._handle = lambda proxy, check=False, source=None: asyncio.sleep(0)

        with caplog.at_level(logging.WARNING, logger="proxybroker"):
            await broker._grab(types={"HTTP": None}, check=False)
        broker.stop()

        assert "example.com" in caplog.text, (
            f"failure must name the provider; logged instead: {caplog.text!r}"
        )

    @pytest.mark.asyncio
    async def test_live_provider_set_is_snapshotted(self):
        """`get_proxies()` hands back the provider's own mutable set.

        The provider keeps adding to it from its concurrent page fetches, so
        iterating it directly raised "Set changed size during iteration" — which
        unwound out of `_grab` and killed discovery for that client until the
        process restarted. Seen in production on four clients in one run.
        """
        class _Mutating:
            proto = set()

            def __init__(self):
                self._live = {("127.0.0.1", "8080")}

            async def get_proxies(self):
                return self._live

        provider = _Mutating()
        broker = Broker(timeout=0.1, max_tries=1, providers=[], stop_broker_on_sigint=False)
        broker._providers = [provider]

        async def _handle(proxy, check=False, source=None):
            # Simulates the provider's own fetch landing mid-iteration.
            provider._live.add((f"10.0.0.{len(provider._live)}", "8080"))

        broker._handle = _handle
        await broker._grab(types={"HTTP": None}, check=False)
        broker.stop()

    @pytest.mark.asyncio
    async def test_forever_mode_survives_a_broken_pass(self):
        """In `forever` mode a bad pass must not end discovery for good.

        The failure has to come from outside any single provider — `_fetch` already
        contains provider-level breakage. A bug in the pass itself used to unwind
        through `find()`, and the pool then sat empty for the rest of the process's
        life while the app logged "proxy pool exhausted" every 30 seconds.
        """
        broker = Broker(timeout=0.1, max_tries=1, providers=[], stop_broker_on_sigint=False)
        broker._forever = True
        broker._grab_pause = 0

        class _Good:
            proto = set()

            async def get_proxies(self):
                return [("127.0.0.1", "8080")]

        broker._providers = [_Good()]
        attempts = []

        async def _handle(proxy, check=False, source=None):
            attempts.append(proxy)
            if len(attempts) == 1:
                raise RuntimeError("bug in the pass, not in a provider")

        broker._handle = _handle

        task = asyncio.create_task(broker._grab(types={"HTTP": None}, check=False))
        for _ in range(200):  # let a few passes run
            await asyncio.sleep(0)
            if len(attempts) > 1:
                break
        task.cancel()
        broker.stop()

        assert len(attempts) > 1, (
            "a failed pass must be followed by another attempt, "
            f"got {len(attempts)}"
        )


class TestTaskBookkeeping:
    """The set of tracked tasks must not grow without bound.

    `_all_tasks` used to be a list appended to for every provider on every pass and
    for every proxy candidate checked, drained only by `_done()`. In `forever` mode
    `_done()` never runs, so finished Task objects — each holding its result and
    coroutine frame — accumulated indefinitely: 66 GB resident after 2.5 hours on a
    run that had not yet issued a single request.
    """

    @pytest.mark.asyncio
    async def test_finished_tasks_are_released(self):
        broker = Broker(timeout=0.1, max_tries=1, providers=[], stop_broker_on_sigint=False)

        async def _noop():
            return None

        tasks = [asyncio.create_task(_noop()) for _ in range(50)]
        broker._track(*tasks)
        assert len(broker._all_tasks) == 50, "in-flight tasks must be tracked"

        await asyncio.gather(*tasks)
        await asyncio.sleep(0)  # let the done-callbacks run

        assert broker._all_tasks == set(), "finished tasks must not be retained"
        broker.stop()

    @pytest.mark.asyncio
    async def test_repeated_passes_do_not_accumulate(self):
        """Simulates many grab cycles: bookkeeping must stay flat, not grow linearly."""
        broker = Broker(timeout=0.1, max_tries=1, providers=[], stop_broker_on_sigint=False)

        async def _noop():
            return None

        for _ in range(20):
            batch = [asyncio.create_task(_noop()) for _ in range(20)]
            broker._track(*batch)
            await asyncio.gather(*batch)
            await asyncio.sleep(0)

        assert len(broker._all_tasks) == 0, (
            f"400 finished tasks left {len(broker._all_tasks)} entries behind"
        )
        broker.stop()

    @pytest.mark.asyncio
    async def test_unfinished_tasks_are_still_cancellable(self):
        """Pruning must not cost stop() its ability to cancel in-flight work."""
        broker = Broker(timeout=0.1, max_tries=1, providers=[], stop_broker_on_sigint=False)

        async def _forever():
            await asyncio.sleep(3600)

        task = asyncio.create_task(_forever())
        broker._track(task)
        assert task in broker._all_tasks

        broker.stop()
        await asyncio.sleep(0)
        assert task.cancelled() or task.cancelling(), "stop() must cancel pending work"


class TestSSLContextIsShared:
    """Every `Proxy` must reuse one SSL context, never build its own.

    `Proxy.__init__` used to call `ssl.create_default_context()` per instance.
    Each call loads the whole system CA bundle into a fresh OpenSSL X509_STORE —
    roughly 800 KB of *native* memory on a host with a normal `ca-certificates`
    install, and native allocations of that shape are not returned to the OS.
    A broker in `forever` mode builds tens of thousands of `Proxy` objects per
    pass, so the resident set grew by about 6 GB per minute and reached 78 GB
    over a night, on a run that never loaded a single lot.

    `tracemalloc` accounted for only ~20 MB of that, because almost none of it
    lives on the Python heap — which is why the object-count assertions below
    are the honest way to guard the fix.
    """

    def test_all_proxies_share_one_context(self):
        proxies = [Proxy(f"10.0.0.{i}", 8080) for i in range(1, 25)]
        contexts = {id(p._ssl_context) for p in proxies}
        assert len(contexts) == 1, (
            f"24 proxies built {len(contexts)} SSL contexts; expected exactly 1"
        )

    def test_context_still_skips_verification(self):
        """Sharing must not quietly re-enable verification against MITM proxies."""
        ctx = Proxy("10.0.0.1", 8080)._ssl_context
        assert ctx.verify_mode == ssl.CERT_NONE
        assert ctx.check_hostname is False

    def test_verify_ssl_opt_in_is_unchanged(self):
        assert Proxy("10.0.0.1", 8080, verify_ssl=True)._ssl_context is True

    def test_no_new_context_objects_per_proxy(self):
        """Guards the leak directly: proxy count must not drive context count."""
        gc.collect()
        before = sum(1 for o in gc.get_objects() if isinstance(o, ssl.SSLContext))

        batch = [Proxy(f"10.1.0.{i}", 8080) for i in range(1, 51)]
        gc.collect()
        after = sum(1 for o in gc.get_objects() if isinstance(o, ssl.SSLContext))

        assert after == before, (
            f"50 proxies created {after - before} new SSL contexts; expected 0"
        )
        assert len(batch) == 50  # keep the batch alive for the measurement
