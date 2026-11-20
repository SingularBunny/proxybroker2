"""Re-checking the saved pool must not end the broker.

``find()`` warm-starts from the pool saved by the previous run: the saved
addresses go through the normal check alongside provider discovery. The
loader used for that ended with ``self._done()`` — correct when ``data=`` is
the whole job, fatal for a warm start. ``_done()`` cancels every task,
including provider discovery, and pushes the ``None`` end-of-stream marker.
A ``forever`` broker stopped itself seconds after start, every run after the
first one that saved a pool; the caller's ``find(wait=True)`` returned
normally, so no retry fired (measured 2026-11-21: pool empty for hours).
"""

import asyncio

from proxybroker import Broker


async def _never_ending_grab(self, types=None, check=False):
    await asyncio.Event().wait()


async def test_warm_start_does_not_stop_forever_broker(mocker):
    mocker.patch(
        "proxybroker.resolver.Resolver.get_real_ext_ips",
        new=mocker.AsyncMock(return_value={"203.0.113.5"}),
    )
    mocker.patch("proxybroker.checker.Checker.check_judges", new=mocker.AsyncMock())
    mocker.patch.object(Broker, "_grab", _never_ending_grab)
    queue = asyncio.Queue()
    broker = Broker(queue=queue, providers=[])
    mocker.patch.object(
        broker._pool_store, "load", return_value=[("198.51.100.7", 1080)]
    )
    handled = mocker.patch.object(broker, "_handle", new=mocker.AsyncMock())
    try:
        await broker.find(types=["SOCKS5"], forever=True)
        for _ in range(20):
            await asyncio.sleep(0.01)
        assert handled.await_count == 1, "saved pool was not re-checked"
        assert not broker._grab_task.done(), "warm start stopped provider discovery"
        assert queue.empty(), "warm start pushed the end-of-stream marker"
    finally:
        broker.stop()


async def test_explicit_data_still_finishes(mocker):
    """``find(data=...)`` is a bounded job: the loader ends it, as before."""
    mocker.patch(
        "proxybroker.resolver.Resolver.get_real_ext_ips",
        new=mocker.AsyncMock(return_value={"203.0.113.5"}),
    )
    mocker.patch("proxybroker.checker.Checker.check_judges", new=mocker.AsyncMock())
    queue = asyncio.Queue()
    broker = Broker(queue=queue, providers=[])
    mocker.patch.object(broker, "_handle", new=mocker.AsyncMock())
    await broker.find(types=["SOCKS5"], data=["198.51.100.7:1080"], wait=True)
    for _ in range(20):
        await asyncio.sleep(0.01)
    assert queue.get_nowait() is None
