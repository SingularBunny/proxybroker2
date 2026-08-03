# Troubleshooting

## Proxy pool stays empty forever (0 proxies found)

This is the most common symptom and has several distinct root causes.

### Root cause 1: Split routing / transparent caching proxy

**Symptom:** All judges fail validation. Log shows judge responses received but proxies never
verified. No errors — just `[0/0]` forever.

**What happens:** On machines behind a transparent caching proxy (common in corporate networks,
some VPS providers), HTTP requests exit through a different IP depending on headers:

- Requests *without* cache-bypass headers → exit via IP A
- Requests *with* `Cache-Control: no-cache, Pragma: no-cache` → exit via IP B

ProxyBroker sends judge requests with cache-bypass headers (via `get_headers()`), so judges
see IP B. But `get_real_ext_ip()` (prior to the fix) used plain requests and got IP A.
Judge checks `real_ext_ip in page` → A ≠ B → all judges fail.

**Fix (already applied in current codebase):** `resolver.py` → `get_real_ext_ip()` now passes
the same headers as judge requests, so both use the same network path.

**How to diagnose:** Run two curl commands on your machine:
```bash
# Plain request
curl -s https://api.ipify.org

# Cache-bypass request (simulates what proxybroker sends)
curl -s -H "Cache-Control: no-cache" -H "Pragma: no-cache" https://api.ipify.org
```
If the two IPs differ — you have split routing.

---

### Root cause 2: Broken HTTPS judge URL

**Symptom:** HTTPS judges never verified. `types=["HTTPS"]` produces empty pool.

**Known broken URL:** `https://httpbin.org/get?show_env` → returns **502 Bad Gateway**.
The `?show_env` parameter is no longer supported.

**Working URL:** `https://httpbin.org/get` (no query string)

Response includes:
```json
{
    "origin": "1.2.3.4",
    "headers": {
        "User-Agent": "...rv=<random_value>..."
    }
}
```
Both `real_ext_ip` (in `origin`) and `rv` (in `headers`) are present — satisfies judge
validation requirements.

**Default judge list recommendation:**
```python
judge_urls = [
    "https://httpbin.org/get",       # HTTPS — works reliably
    "http://azenv.net/",             # HTTP — classic, reliable
    "http://proxyjudge.us/azenv.php",# HTTP — reliable
    "http://ip.spys.ru/",            # HTTP — reliable
]
```

---

### Root cause 3: `types=["HTTPS"]` with no working HTTPS judge

**Symptom:** Setting `types=["HTTPS"]` results in 0 proxies even though HTTPS proxies exist.

**What happens:** HTTPS negotiator activation depends on a verified HTTPS judge. If all HTTPS
judge URLs fail (e.g. `?show_env` → 502), no HTTPS negotiators activate, so HTTPS proxies
are never validated.

**Fix:** Use a working HTTPS judge URL (see Root cause 2 above).

---

## Using SOCKS proxies with aiohttp

ProxyBroker validates SOCKS4/SOCKS5 proxies, but `aiohttp` does not support SOCKS natively.
Use `aiohttp-socks`:

```bash
pip install aiohttp-socks
```

```python
from aiohttp_socks import ProxyConnector

# Detect proxy type — use proxy.types, NOT proxy.schemes
# proxy.schemes returns ("HTTP", "HTTPS") — TCP-level capability, not proxy protocol
# proxy.types is a dict with the actual protocol: {"SOCKS5": ..., "HTTPS": ...}

is_socks = "SOCKS5" in proxy.types or "SOCKS4" in proxy.types
if is_socks:
    proto = "socks5" if "SOCKS5" in proxy.types else "socks4"
    connector = ProxyConnector.from_url(f"{proto}://{proxy.host}:{proxy.port}")
    session = aiohttp.ClientSession(connector=connector)
else:
    session = aiohttp.ClientSession()
    # Use proxy= parameter for HTTP/HTTPS proxies
```

**Common mistake:** `proxy.schemes & {"SOCKS5"}` raises `TypeError` because `proxy.schemes`
is a `tuple`, not a `set`. Always use `"SOCKS5" in proxy.types`.

---

## Why prefer SOCKS4/SOCKS5 over HTTP for HTTPS target URLs?

When your target URL is HTTPS, the proxy must tunnel TLS. Protocol support:

| Proxy type | HTTPS tunneling | How |
|------------|-----------------|-----|
| HTTP       | Only if CONNECT supported | Sends `CONNECT host:443`, then TLS inside |
| HTTPS      | Yes (via CONNECT) | Same as HTTP but channel itself is encrypted |
| SOCKS4     | Yes | TCP-level tunnel, TLS transparent |
| SOCKS5     | Yes | TCP-level tunnel, TLS transparent |

Most free HTTP proxies do **not** support `CONNECT`, so they cannot be used for HTTPS targets.
SOCKS4/5 proxies tunnel raw TCP — TLS just passes through.

If your target API is HTTPS-only: use `types=["HTTPS", "SOCKS4", "SOCKS5"]` for the broadest
working pool.

---

## Proxy broker runs but pool never reaches minimum size

**Check 1:** Are your provider URLs returning proxies at all?
```bash
curl -s "https://your-provider-url.com/proxies.txt" | head -5
```

**Check 2:** Are you filtering by country? Many provider lists contain mostly non-RU proxies.
For Russian proxies specifically, prefer provider URLs with `country=ru` in the query string
rather than relying on post-fetch country filtering.

**Check 3:** `None` sentinel in queue — when the broker finishes scanning all providers with
0 valid proxies, it puts `None` in the queue. If you're checking `queue.qsize() > 0` to detect
readiness, a single `None` will trigger a false positive. Check for actual proxy objects instead.
