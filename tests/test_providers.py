"""Behaviour tests for the Provider base class.

These tests exercise Provider in isolation without any network calls.
The 50+ concrete provider subclasses share this base, so coverage here
flows through the whole providers module.
"""

import pytest

from unittest.mock import AsyncMock

from proxybroker.providers import Provider, Spys_ru


class TestProviderConstruction:
    def test_url_extraction_sets_domain(self):
        p = Provider(url="http://www.example.com/proxylist")
        assert p.domain == "www.example.com"
        assert p.url == "http://www.example.com/proxylist"

    def test_no_url_omits_domain_attribute(self):
        # Provider must not crash when constructed without a url
        # (some discovery paths set the url later).
        p = Provider()
        assert p.url is None
        assert not hasattr(p, "domain") or p.domain is not None

    def test_default_proto_is_empty_tuple(self):
        p = Provider(url="http://example.com")
        assert p.proto == ()

    def test_custom_proto_preserved(self):
        p = Provider(url="http://example.com", proto=("HTTP", "HTTPS"))
        assert p.proto == ("HTTP", "HTTPS")

    def test_initial_proxies_is_empty_set(self):
        p = Provider(url="http://example.com")
        assert p.proxies == set()


class TestProviderProxiesSetter:
    """Provider.proxies setter is the contract every subclass relies on."""

    def test_setter_filters_empty_ports(self):
        """Items with an empty port string must be dropped."""
        p = Provider(url="http://example.com", proto=("HTTP",))
        p.proxies = [("192.0.2.1", "8080"), ("198.51.100.1", "")]
        # Only the entry with a real port should land
        assert any(item[0] == "192.0.2.1" for item in p.proxies)
        assert all(item[0] != "198.51.100.1" for item in p.proxies)

    def test_setter_attaches_proto_tuple(self):
        """Each stored entry is (host, port, proto-tuple)."""
        p = Provider(url="http://example.com", proto=("HTTP", "HTTPS"))
        p.proxies = [("192.0.2.1", "8080")]
        entry = next(iter(p.proxies))
        assert entry == ("192.0.2.1", "8080", ("HTTP", "HTTPS"))

    def test_setter_dedupes_via_set(self):
        """Adding the same (host, port) twice must store one entry."""
        p = Provider(url="http://example.com", proto=("HTTP",))
        p.proxies = [("192.0.2.1", "8080"), ("192.0.2.1", "8080")]
        assert len(p.proxies) == 1

    def test_setter_appends_across_calls(self):
        """Subsequent assignments add to (not replace) the proxy set."""
        p = Provider(url="http://example.com", proto=("HTTP",))
        p.proxies = [("192.0.2.1", "8080")]
        p.proxies = [("198.51.100.1", "3128")]
        hosts = {entry[0] for entry in p.proxies}
        assert hosts == {"192.0.2.1", "198.51.100.1"}


class TestProviderFindProxies:
    """find_proxies() / _find_proxies() use the global IP:port regex."""

    def test_finds_ip_port_pairs_in_arbitrary_text(self):
        """The regex pattern works on raw HTML scraped from provider sites."""
        page = """
        <html><body>
        <table>
          <tr><td>192.0.2.1:8080</td></tr>
          <tr><td>198.51.100.1:3128</td></tr>
          <tr><td>not-a-proxy</td></tr>
        </table>
        </body></html>
        """
        p = Provider(url="http://example.com")
        results = p.find_proxies(page)
        assert ("192.0.2.1", "8080") in results
        assert ("198.51.100.1", "3128") in results

    def test_empty_page_returns_empty(self):
        p = Provider(url="http://example.com")
        assert p.find_proxies("") == []

    def test_page_with_no_proxies_returns_empty(self):
        p = Provider(url="http://example.com")
        assert p.find_proxies("just some prose without any proxies") == []

    def test_ipv6_mapped_does_not_spawn_phantom_ipv4(self):
        """IPv4-mapped IPv6 entries like `[::ffff:192.0.2.1]:8080` must
        produce ONLY the v6 entry — not also a phantom `192.0.2.1:8080`
        from the legacy IPv4 regex matching the embedded v4 octets.

        Provider feeds in the wild occasionally emit v4-mapped form;
        without masking the bracketed span before the v4 pass, every
        such proxy spawns an additional invalid v4 endpoint that the
        provider never advertised.
        """
        p = Provider(url="http://example.com")
        page = "real proxy: [::ffff:192.0.2.1]:8080 trailer"
        results = p.find_proxies(page)
        assert ("::ffff:192.0.2.1", "8080") in results
        assert not any(host == "192.0.2.1" for host, _port in results)

    def test_ipv6_bracketed_alongside_v4_pairs(self):
        """Mixed v4/v6 page: both forms extracted, no cross-pollination."""
        p = Provider(url="http://example.com")
        page = "v4: 192.0.2.5:3128 v6: [2001:db8::1]:9090 more v4: 198.51.100.10:8080"
        results = p.find_proxies(page)
        assert ("192.0.2.5", "3128") in results
        assert ("198.51.100.10", "8080") in results
        assert ("2001:db8::1", "9090") in results


@pytest.mark.asyncio
async def test_find_on_pages_handles_empty_url_list():
    """Edge case: callers occasionally hand _find_on_pages an empty list."""
    p = Provider(url="http://example.com")
    # Should return cleanly without raising or scheduling tasks.
    await p._find_on_pages([])
    assert p.proxies == set()


class TestMixedProtocolLists:
    """Aggregated lists mix protocols in one file and mark each entry with a scheme.

    Applying the provider's declared protocol to every entry checks most of them with
    the wrong one, and they get discarded as dead. Real case: the proxifly RU list
    holds 15 http, 20 socks4 and 29 socks5 addresses; registered wholesale as HTTP it
    yielded an empty pool while 64 usable proxies sat in the source.
    """

    def _provider(self, proto=("HTTP", "CONNECT:80")):
        from proxybroker.providers import Provider

        return Provider(url="http://example.test/list.txt", proto=proto)

    def test_scheme_prefix_overrides_declared_protocol(self):
        provider = self._provider()
        page = "socks5://1.2.3.4:1080\nsocks4://5.6.7.8:1081\nhttp://9.9.9.9:8080\n"
        found = [("1.2.3.4", "1080"), ("5.6.7.8", "1081"), ("9.9.9.9", "8080")]

        result = {(h, p): proto for h, p, proto in provider._proxies_with_schemes(page, found)}
        assert result[("1.2.3.4", "1080")] == ("SOCKS5",)
        assert result[("5.6.7.8", "1081")] == ("SOCKS4",)
        assert "HTTP" in result[("9.9.9.9", "8080")]

    def test_plain_list_keeps_declared_protocol(self):
        """No schemes in the file → nothing changes, old behaviour preserved."""
        provider = self._provider()
        assert provider._proxies_with_schemes("1.2.3.4:1080\n", [("1.2.3.4", "1080")]) is None

    def test_entries_without_scheme_fall_back(self):
        provider = self._provider(proto=("HTTPS",))
        page = "socks5://1.2.3.4:1080\n5.6.7.8:3128\n"
        found = [("1.2.3.4", "1080"), ("5.6.7.8", "3128")]

        result = {(h, p): proto for h, p, proto in provider._proxies_with_schemes(page, found)}
        assert result[("1.2.3.4", "1080")] == ("SOCKS5",)
        assert result[("5.6.7.8", "3128")] == ("HTTPS",), "unmarked entry keeps the default"

    def test_scheme_matching_is_case_insensitive(self):
        provider = self._provider()
        result = provider._proxies_with_schemes(
            "SOCKS5://1.2.3.4:1080\n", [("1.2.3.4", "1080")]
        )
        assert result[0][2] == ("SOCKS5",)

    def test_ports_without_a_match_are_skipped(self):
        provider = self._provider()
        result = provider._proxies_with_schemes(
            "socks5://1.2.3.4:1080\n", [("1.2.3.4", None)]
        )
        assert result == []


class TestSpysRuRobustness:
    """A scraper must degrade to "no proxies this pass", never to an exception.

    `Spys_ru._pipe` did `re.findall(...)[0]` on the session id embedded in the page.
    A block, a captcha or a redesign all produce a page without one, and the
    provider raised `IndexError: list index out of range` on every pass. Before
    failures started naming their provider the log just said
    "Provider failed, skipping it: IndexError(...)", so the bug sat there run after
    run with nothing to point at.
    """

    def _provider(self):
        return Spys_ru(url="http://spys.one/proxies/", proto=("HTTP",))

    @pytest.mark.asyncio
    async def test_page_without_session_id_yields_nothing(self):
        provider = self._provider()
        provider.get = AsyncMock(return_value="<html>captcha, no session here</html>")
        provider._find_on_pages = AsyncMock()

        await provider._pipe()  # must not raise

        provider._find_on_pages.assert_not_called()

    @pytest.mark.asyncio
    async def test_empty_page_yields_nothing(self):
        provider = self._provider()
        provider.get = AsyncMock(return_value="")
        provider._find_on_pages = AsyncMock()

        await provider._pipe()

        provider._find_on_pages.assert_not_called()

    @pytest.mark.asyncio
    async def test_session_id_is_still_used_when_present(self):
        """The guard must not cost the provider its working path."""
        provider = self._provider()
        session = "a" * 32
        provider.get = AsyncMock(return_value=f"<script>x='{session}'</script>")
        provider._find_on_pages = AsyncMock()

        await provider._pipe()

        provider._find_on_pages.assert_called_once()
        urls = provider._find_on_pages.call_args[0][0]
        assert all(u["data"]["xf0"] == session for u in urls)

    def test_undecodable_port_does_not_lose_the_page(self):
        """An unknown symbol must cost one port, not every proxy on the page."""
        provider = self._provider()
        page = "1.2.3.4:8080\n5.6.7.8+(q1q1^z9z9)\n"

        found = provider.find_proxies(page)  # must not raise KeyError

        assert ("1.2.3.4", "8080") in found

    def test_symbol_table_is_per_instance(self):
        """As a class attribute it was shared by every instance and never cleared.

        That let one pass's symbol table decode another pass's page, and it grew
        for the life of the process.
        """
        first, second = self._provider(), self._provider()
        first.charEqNum["x1y2"] = 7

        assert "x1y2" not in second.charEqNum
