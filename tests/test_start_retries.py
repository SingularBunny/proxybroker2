"""A brief connectivity drop at startup must not fail ``find()``."""
import asyncio

import pytest

from proxybroker import Broker


async def test_ext_ip_retried_then_succeeds(mocker):
    probe = mocker.AsyncMock(
        side_effect=[RuntimeError("Could not get the external IP"), {"203.0.113.5"}]
    )
    mocker.patch("proxybroker.resolver.Resolver.get_real_ext_ips", new=probe)
    mocker.patch("proxybroker.checker.Checker.check_judges", new=mocker.AsyncMock())
    broker = Broker(queue=asyncio.Queue(), providers=[], start_retry_pause=0)
    try:
        await broker.find(types=["SOCKS5"], forever=True)
        assert probe.await_count == 2
    finally:
        broker.stop()


async def test_judges_retried_then_succeed(mocker):
    mocker.patch(
        "proxybroker.resolver.Resolver.get_real_ext_ips",
        new=mocker.AsyncMock(return_value={"203.0.113.5"}),
    )
    judges = mocker.AsyncMock(side_effect=[RuntimeError("Not found judges"), None])
    mocker.patch("proxybroker.checker.Checker.check_judges", new=judges)
    broker = Broker(queue=asyncio.Queue(), providers=[], start_retry_pause=0)
    try:
        await broker.find(types=["SOCKS5"], forever=True)
        assert judges.await_count == 2
    finally:
        broker.stop()


async def test_gives_up_after_retries(mocker):
    probe = mocker.AsyncMock(side_effect=RuntimeError("Could not get the external IP"))
    mocker.patch("proxybroker.resolver.Resolver.get_real_ext_ips", new=probe)
    broker = Broker(queue=asyncio.Queue(), providers=[], start_retries=3, start_retry_pause=0)
    try:
        with pytest.raises(RuntimeError):
            await broker.find(types=["SOCKS5"], forever=True)
        assert probe.await_count == 3
    finally:
        broker.stop()
