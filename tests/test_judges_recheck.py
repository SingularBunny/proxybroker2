"""A failed judge check must not kill a long-running broker silently.

Judges are checked once, at the start of ``find()``. On a flaky network that
check fails often: in a live measurement on 2026-11-20, two rounds out of six
lost every judge (``OSError 113``, timeouts) a few seconds apart from rounds
where all of them passed.

Two things used to happen, both invisible to the caller:

* ``check_judges`` ran as a background task that nobody awaited, so its
  ``RuntimeError("Not found judges")`` was dropped and ``find()`` returned
  normally — a caller's retry-on-exception never fired;
* a partial failure (HTTPS judge up, HTTP judges down) switched HTTP, SOCKS4
  and SOCKS5 checking off for the rest of the broker's life, with only a
  ``warnings.warn`` that Python prints once per process. A ``forever`` broker
  kept collecting tens of thousands of candidates and passed none of them.
"""

import asyncio

import pytest

from proxybroker import Broker
from proxybroker.checker import Checker
from proxybroker.judge import Judge

HTTP_JUDGE = "http://judge-http.test/"
HTTPS_JUDGE = "https://judge-https.test/"


@pytest.fixture(autouse=True)
def _clean_judges():
    Judge.clear()
    yield
    Judge.clear()


@pytest.fixture
def judges_up(monkeypatch):
    """Control which judge schemes pass their check. Mutable between calls."""
    up = set()

    async def fake_check(self, real_ext_ips=None, real_ext_ip=None):
        if self.scheme in up:
            self.is_working = True
            self.available[self.scheme].append(self)
            self.ev[self.scheme].set()

    monkeypatch.setattr(Judge, "check", fake_check)
    return up


def _checker(types):
    return Checker(
        judges=[HTTP_JUDGE, HTTPS_JUDGE],
        real_ext_ips={"203.0.113.5"},
        types=types,
    )


async def test_partial_failure_disables_protocols_until_judges_return(judges_up):
    checker = _checker({"HTTPS": None, "SOCKS5": None})

    judges_up.add("HTTPS")
    with pytest.warns(UserWarning, match="Not found judges"):
        await checker.check_judges()
    assert checker.disabled_protocols == {"SOCKS5"}

    judges_up.add("HTTP")
    await checker.check_judges()
    assert checker.disabled_protocols == set()
    assert "SOCKS5" in checker._ngtrs


async def test_recheck_does_not_duplicate_working_judges(judges_up):
    checker = _checker({"HTTPS": None})
    judges_up.add("HTTPS")
    await checker.check_judges()
    await checker.check_judges()
    assert len(Judge.available["HTTPS"]) == 1


async def test_all_judges_failed_raises(judges_up):
    checker = _checker({"HTTPS": None, "SOCKS5": None})
    with pytest.warns(UserWarning), pytest.raises(RuntimeError, match="Not found judges"):
        await checker.check_judges()


async def test_find_surfaces_judge_failure(judges_up, mocker):
    """The caller must see it: a retry on exception is the recovery path."""
    mocker.patch(
        "proxybroker.resolver.Resolver.get_real_ext_ips",
        new=mocker.AsyncMock(return_value={"203.0.113.5"}),
    )
    broker = Broker(queue=asyncio.Queue(), judges=[HTTP_JUDGE, HTTPS_JUDGE], providers=[])
    try:
        with pytest.warns(UserWarning), pytest.raises(RuntimeError, match="Not found judges"):
            await broker.find(types=["HTTPS", "SOCKS5"], forever=True)
    finally:
        broker.stop()


async def test_forever_pass_rechecks_disabled_protocols(judges_up, mocker):
    """Network back → the next pass turns SOCKS checking on again."""
    mocker.patch(
        "proxybroker.resolver.Resolver.get_real_ext_ips",
        new=mocker.AsyncMock(return_value={"203.0.113.5"}),
    )
    judges_up.add("HTTPS")
    broker = Broker(
        queue=asyncio.Queue(),
        judges=[HTTP_JUDGE, HTTPS_JUDGE],
        providers=[],
        grab_pause=0,
    )
    try:
        with pytest.warns(UserWarning):
            await broker.find(types=["HTTPS", "SOCKS5"], forever=True)
        assert broker._checker.disabled_protocols == {"SOCKS5"}

        judges_up.add("HTTP")
        for _ in range(50):
            await asyncio.sleep(0.01)
            if not broker._checker.disabled_protocols:
                break
        assert broker._checker.disabled_protocols == set()
    finally:
        broker.stop()
