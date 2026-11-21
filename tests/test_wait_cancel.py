"""Cancelling the caller of ``find(wait=True)`` must cancel it, not return."""
import asyncio

import pytest

from proxybroker import Broker


async def _never_ending_grab(self, types=None, check=False):
    await asyncio.Event().wait()


async def test_caller_cancel_propagates(mocker):
    mocker.patch(
        "proxybroker.resolver.Resolver.get_real_ext_ips",
        new=mocker.AsyncMock(return_value={"203.0.113.5"}),
    )
    mocker.patch("proxybroker.checker.Checker.check_judges", new=mocker.AsyncMock())
    mocker.patch.object(Broker, "_grab", _never_ending_grab)
    broker = Broker(queue=asyncio.Queue(), providers=[])
    task = asyncio.create_task(broker.find(types=["SOCKS5"], forever=True, wait=True))
    await asyncio.sleep(0.05)
    task.cancel()
    try:
        with pytest.raises(asyncio.CancelledError):
            await task
    finally:
        broker.stop()


async def test_limit_reached_still_returns(mocker):
    mocker.patch(
        "proxybroker.resolver.Resolver.get_real_ext_ips",
        new=mocker.AsyncMock(return_value={"203.0.113.5"}),
    )
    mocker.patch("proxybroker.checker.Checker.check_judges", new=mocker.AsyncMock())
    mocker.patch.object(Broker, "_grab", _never_ending_grab)
    broker = Broker(queue=asyncio.Queue(), providers=[])
    asyncio.get_running_loop().call_later(0.05, broker._done)
    await asyncio.wait_for(broker.find(types=["SOCKS5"], limit=1, wait=True), 2)
