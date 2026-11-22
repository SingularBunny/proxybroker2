"""Several verify URLs work like judges: any one answering passes the proxy."""
from proxybroker.checker import Checker


def _checker(urls):
    return Checker(judges=["http://j.test/"], real_ext_ips={"203.0.113.5"}, verify_url=urls)


async def test_any_url_passes(mocker):
    ch = _checker(["https://a.test/", "https://b.test/"])
    seen = []

    async def one(proxy, url):
        seen.append(url)
        return url == "https://b.test/"

    mocker.patch.object(ch, "_verify_one", side_effect=one)
    assert await ch._verify_against_url(object())
    assert "https://b.test/" in seen


async def test_all_down_fails(mocker):
    ch = _checker(["https://a.test/", "https://b.test/"])
    one = mocker.patch.object(ch, "_verify_one", new=mocker.AsyncMock(return_value=False))
    assert not await ch._verify_against_url(object())
    assert one.await_count == 2


def test_single_string_still_accepted():
    ch = _checker("https://a.test/")
    assert ch._verify_urls == ["https://a.test/"] and ch._verify_url == "https://a.test/"


def test_empty_disables():
    assert _checker([])._verify_url is None
