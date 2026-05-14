"""Test Server public API - focused on user-visible behavior.

This file tests how users actually use the Server:
- Starting a proxy server that routes requests through found proxies
- Proxy rotation and failure handling
- Connection management
- Server lifecycle (start/stop/context manager)

We focus on WHAT the server does for users, not HOW it does it internally.
Based on the real usage pattern from examples/proxy_server.py
"""

import asyncio
from unittest.mock import MagicMock

import pytest

from proxybroker import Proxy
from proxybroker.errors import NoProxyError
from proxybroker.server import ProxyPool, Server


class TestServerAPI:
    """Test Server public API behavior."""

    @pytest.fixture
    def mock_proxy_queue(self):
        """Create a queue for testing (proxies added in individual tests)."""
        return asyncio.Queue()

    def create_mock_proxy(self, host="1.2.3.4", port=8080, schemes=("HTTP", "HTTPS")):
        """Helper to create a mock proxy."""
        proxy = MagicMock(spec=Proxy)
        proxy.host = host
        proxy.port = port
        proxy.schemes = schemes
        proxy.avg_resp_time = 1.5
        proxy.error_rate = 0.1
        proxy.stat = {"requests": 5, "errors": {}}
        return proxy

    # Core Server Lifecycle Tests

    def test_server_can_be_created(self, mock_proxy_queue):
        """Test that Server can be instantiated with basic parameters."""
        server = Server(host="127.0.0.1", port=8888, proxies=mock_proxy_queue)
        assert server.host == "127.0.0.1"
        assert server.port == 8888

    @pytest.mark.asyncio
    async def test_server_start_creates_listening_server(self, mock_proxy_queue):
        """Test that server.start() creates a listening server."""
        server = Server(
            host="127.0.0.1",
            port=0,  # Use any available port
            proxies=mock_proxy_queue,
        )

        await server.start()

        # Should have created a listening server
        assert server._server is not None
        assert server._server.sockets  # Should have listening sockets

        # Clean up
        server.stop()

    @pytest.mark.asyncio
    async def test_server_async_context_manager(self, mock_proxy_queue):
        """Test Server as async context manager."""
        async with Server("127.0.0.1", 0, mock_proxy_queue) as server:
            # Server should be started
            assert server._server is not None
            assert server._server.sockets

        # Server should be closed after context
        assert server._server is None

    def test_server_with_custom_parameters(self, mock_proxy_queue):
        """Test Server accepts configuration parameters."""
        server = Server(
            host="0.0.0.0",
            port=9999,
            proxies=mock_proxy_queue,
            timeout=15,
            max_tries=5,
            prefer_connect=True,
        )

        assert server.host == "0.0.0.0"
        assert server.port == 9999

    # ProxyPool Behavior Tests - Focus on user-visible behavior

    @pytest.mark.asyncio
    async def test_proxy_pool_provides_proxies_for_requests(self):
        """Test that ProxyPool provides proxies when requested."""
        queue = asyncio.Queue()

        # Add a proxy to the queue
        proxy = MagicMock(spec=Proxy)
        proxy.schemes = ("HTTP", "HTTPS")
        proxy.avg_resp_time = 1.0
        await queue.put(proxy)

        pool = ProxyPool(queue)

        # Should be able to get a proxy for HTTP requests
        result = await pool.get("HTTP")
        assert result is proxy

    @pytest.mark.asyncio
    async def test_proxy_pool_handles_empty_queue(self):
        """Test ProxyPool behavior when no proxies are available."""
        queue = asyncio.Queue()
        # Don't add any proxies

        pool = ProxyPool(queue)

        # Should timeout gracefully when no proxies available
        with pytest.raises((asyncio.TimeoutError, NoProxyError)):
            await asyncio.wait_for(pool.get("HTTP"), timeout=0.5)

    @pytest.mark.asyncio
    async def test_proxy_pool_respects_scheme_requirements(self):
        """Test that ProxyPool only returns compatible proxies."""
        queue = asyncio.Queue()

        # Add HTTP-only proxy
        http_proxy = MagicMock(spec=Proxy)
        http_proxy.schemes = ("HTTP",)
        http_proxy.avg_resp_time = 1.0
        await queue.put(http_proxy)

        pool = ProxyPool(queue)

        # Should get the proxy for HTTP
        result = await pool.get("HTTP")
        assert result is http_proxy

        # Should not get it for HTTPS (wrong scheme)
        # This would require more complex mocking to test properly

    def test_proxy_pool_quality_thresholds(self):
        """Test ProxyPool accepts quality configuration."""
        queue = asyncio.Queue()

        pool = ProxyPool(queue, max_error_rate=0.3, max_resp_time=5, min_req_proxy=10)

        assert pool is not None
        # Quality thresholds should be configurable

    # Error Handling Tests

    @pytest.mark.asyncio
    async def test_server_handles_connection_errors_gracefully(self, mock_proxy_queue):
        """Test server handles client connection errors."""
        server = Server("127.0.0.1", 0, mock_proxy_queue)

        # This test would ideally make actual connections and test error handling
        # For now, just verify server can be created and started
        await server.start()
        assert server._server is not None
        server.stop()

    # Integration-style Tests (closer to real usage)

    @pytest.mark.asyncio
    async def test_server_proxy_rotation_concept(self, mock_proxy_queue):
        """Test that server can handle multiple proxies (concept test)."""
        # Add multiple proxies to queue
        queue = asyncio.Queue()

        for i in range(3):
            proxy = MagicMock(spec=Proxy)
            proxy.host = f"proxy{i}.example.com"
            proxy.port = 8080 + i
            proxy.schemes = ("HTTP", "HTTPS")
            proxy.avg_resp_time = 1.0 + i * 0.5
            await queue.put(proxy)

        server = Server("127.0.0.1", 0, queue)
        await server.start()

        # Server should be able to handle multiple proxies
        # (Actual rotation testing would require real HTTP requests)
        assert server._server is not None

        server.stop()

    # Server API Control Tests

    @pytest.mark.asyncio
    async def test_server_api_endpoints_exist(self, mock_proxy_queue):
        """Test that server exposes API endpoints for control."""
        server = Server("127.0.0.1", 0, mock_proxy_queue)
        await server.start()

        # The server should handle requests to special "proxycontrol" host
        # This is tested by making actual HTTP requests in integration tests
        # Here we just verify the server starts successfully
        assert server._server is not None

        server.stop()

    # Configuration and Customization Tests

    def test_server_accepts_broker_serve_parameters(self):
        """Test that Server accepts the same parameters as broker.serve()."""
        queue = asyncio.Queue()

        # These are the parameters from examples/proxy_server.py
        server = Server(
            host="127.0.0.1",
            port=8888,
            proxies=queue,
            timeout=8,
            max_tries=3,
            prefer_connect=True,
            max_error_rate=0.5,
            max_resp_time=8,
            backlog=100,
        )

        # Verify the server was created with the expected configuration
        assert server.host == "127.0.0.1"
        assert server.port == 8888

    # Cleanup and Resource Management Tests

    @pytest.mark.asyncio
    async def test_server_cleanup_on_stop(self, mock_proxy_queue):
        """Test that server properly cleans up resources on stop."""
        server = Server("127.0.0.1", 0, mock_proxy_queue)
        await server.start()

        # Server should be running
        assert server._server is not None

        # Stop should clean up
        server.stop()
        assert server._server is None

    @pytest.mark.asyncio
    async def test_server_async_cleanup_with_aclose(self, mock_proxy_queue):
        """Test that server.aclose() cleans up without stopping event loop."""
        server = Server("127.0.0.1", 0, mock_proxy_queue)
        await server.start()

        assert server._server is not None

        # aclose() should clean up async-safely
        await server.aclose()
        assert server._server is None

        # Event loop should still be running (we can call more async code)
        await asyncio.sleep(0.001)  # This would fail if loop was stopped


class TestProxyPoolErgonomics:
    """Tests for the ergonomic helpers added to ProxyPool:
    get(timeout=), acquire(), get_any().
    """

    def _make_proxy(self, host="1.2.3.4", port=8080, schemes=("HTTP", "HTTPS")):
        proxy = MagicMock(spec=Proxy)
        proxy.host = host
        proxy.port = port
        proxy.schemes = schemes
        proxy.avg_resp_time = 1.0
        proxy.error_rate = 0.0
        proxy.stat = {"requests": 0, "errors": {}}
        return proxy

    # ── get(timeout=) ────────────────────────────────────────────────────────

    @pytest.mark.asyncio
    async def test_get_with_timeout_returns_proxy_before_deadline(self):
        """get(scheme, timeout=N) returns a proxy that arrives within N seconds."""
        queue = asyncio.Queue()
        proxy = self._make_proxy(schemes=("HTTP",))
        await queue.put(proxy)

        pool = ProxyPool(queue, min_queue=1)
        result = await pool.get("HTTP", timeout=2.0)
        assert result is proxy

    @pytest.mark.asyncio
    async def test_get_with_timeout_raises_no_proxy_error_when_empty(self):
        """get(scheme, timeout=N) raises NoProxyError when queue stays empty."""
        queue = asyncio.Queue()
        pool = ProxyPool(queue, min_queue=1)

        with pytest.raises(NoProxyError):
            await pool.get("HTTP", timeout=0.1)

    @pytest.mark.asyncio
    async def test_get_timeout_overrides_pool_import_timeout(self):
        """Per-call timeout takes precedence over the pool-level import_timeout."""
        queue = asyncio.Queue()
        # Pool has a very long default import_timeout
        pool = ProxyPool(queue, min_queue=1, import_timeout=60.0)

        # But per-call timeout is short – should raise quickly
        with pytest.raises(NoProxyError):
            await pool.get("HTTP", timeout=0.1)

    @pytest.mark.asyncio
    async def test_get_without_timeout_uses_pool_import_timeout(self):
        """When timeout=None, the pool's import_timeout is used (existing behaviour)."""
        queue = asyncio.Queue()
        pool = ProxyPool(queue, min_queue=1, import_timeout=0.1)

        with pytest.raises(NoProxyError):
            await pool.get("HTTP")  # no explicit timeout → uses import_timeout=0.1

    # ── acquire() ────────────────────────────────────────────────────────────

    @pytest.mark.asyncio
    async def test_acquire_yields_proxy_and_returns_it_on_exit(self):
        """acquire() yields the proxy and calls put() when the block exits normally."""
        queue = asyncio.Queue()
        proxy = self._make_proxy(schemes=("HTTP",))
        await queue.put(proxy)

        pool = ProxyPool(queue, min_queue=1)

        async with pool.acquire("HTTP", timeout=2.0) as p:
            assert p is proxy

        # After the context the proxy should be back in _newcomers (stat.requests < min_req_proxy)
        assert proxy in pool._newcomers

    @pytest.mark.asyncio
    async def test_acquire_returns_proxy_to_pool_on_exception(self):
        """acquire() still calls put() even when an exception is raised inside."""
        queue = asyncio.Queue()
        proxy = self._make_proxy(schemes=("HTTPS",))
        await queue.put(proxy)

        pool = ProxyPool(queue, min_queue=1)

        with pytest.raises(RuntimeError, match="deliberate"):
            async with pool.acquire("HTTPS", timeout=2.0) as p:
                assert p is proxy
                raise RuntimeError("deliberate")

        # Proxy must have been returned to pool despite the exception
        assert proxy in pool._newcomers

    @pytest.mark.asyncio
    async def test_acquire_raises_no_proxy_error_when_empty(self):
        """acquire() propagates NoProxyError when no proxy is available."""
        queue = asyncio.Queue()
        pool = ProxyPool(queue, min_queue=1)

        with pytest.raises(NoProxyError):
            async with pool.acquire("HTTP", timeout=0.1):
                pass  # should never reach here

    # ── get_any() ────────────────────────────────────────────────────────────

    @pytest.mark.asyncio
    async def test_get_any_returns_proxy_matching_first_available_scheme(self):
        """get_any() returns a proxy as soon as one scheme matches."""
        queue = asyncio.Queue()
        # Only HTTP proxy available
        proxy = self._make_proxy(schemes=("HTTP",))
        await queue.put(proxy)

        pool = ProxyPool(queue, min_queue=1)
        # Ask for HTTP or HTTPS – should get the HTTP one
        result = await pool.get_any(("HTTP", "HTTPS"), timeout=2.0)
        assert result is proxy

    @pytest.mark.asyncio
    async def test_get_any_falls_back_to_second_scheme(self):
        """get_any() tries each scheme in order and returns the first match."""
        queue = asyncio.Queue()
        # Only HTTPS proxy available
        proxy = self._make_proxy(schemes=("HTTPS",))
        await queue.put(proxy)

        pool = ProxyPool(queue, min_queue=1)
        # 'HTTP' will timeout, then 'HTTPS' should succeed
        result = await pool.get_any(("HTTP", "HTTPS"), timeout=0.1)
        assert result is proxy

    @pytest.mark.asyncio
    async def test_get_any_raises_no_proxy_error_when_all_schemes_exhausted(self):
        """get_any() raises NoProxyError if no scheme has an available proxy."""
        queue = asyncio.Queue()
        pool = ProxyPool(queue, min_queue=1)

        with pytest.raises(NoProxyError):
            await pool.get_any(("HTTP", "HTTPS"), timeout=0.05)

    @pytest.mark.asyncio
    async def test_get_any_default_schemes_are_http_and_https(self):
        """get_any() defaults to trying both HTTP and HTTPS."""
        queue = asyncio.Queue()
        proxy = self._make_proxy(schemes=("HTTP",))
        await queue.put(proxy)

        pool = ProxyPool(queue, min_queue=1)
        # Call without explicit schemes – should use default ('HTTP', 'HTTPS')
        result = await pool.get_any(timeout=2.0)
        assert result is proxy
