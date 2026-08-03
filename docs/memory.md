# Memory behaviour of a long-running broker

A `Broker` in `forever` mode is a very different workload from the one the
library was originally written for. A one-shot `find(limit=10)` builds a few
thousand objects and exits, so an allocation that is never released costs
nothing anyone would notice. A broker that walks the provider list every 60
seconds for 17 hours makes the same allocation about a million times.

Two leaks of that shape have been found and fixed. Both were invisible in a
short run, and the second was invisible to `tracemalloc` as well. Anything
allocated per `Proxy` or per check should be read with this page in mind.

## Task bookkeeping (fixed)

`Broker._all_tasks` was a list, appended to for every provider on every pass
and for every proxy candidate checked, drained only by `_done()`. In `forever`
mode `_done()` never runs, so finished `Task` objects — each holding its result
and coroutine frame — accumulated indefinitely.

Observed: 66 GB resident after 2.5 hours, on a run that had not yet issued a
single request.

Fixed by making `_all_tasks` a `set` and having each task remove itself on
completion:

```python
self._all_tasks = set()

def _track(self, *tasks):
    for task in tasks:
        self._all_tasks.add(task)
        task.add_done_callback(self._all_tasks.discard)
```

`stop()` still cancels whatever is genuinely in flight, so the pruning costs
nothing in shutdown behaviour. Guarded by `TestTaskBookkeeping` in
`tests/test_api.py`.

## Per-proxy SSL contexts (fixed)

`Proxy.__init__` called `ssl.create_default_context()` for every instance.
That call loads the entire system CA bundle into a fresh OpenSSL
`X509_STORE`. On a host with a normal `ca-certificates` install that is
roughly **800 KB of native memory per proxy**, and native allocations of that
shape are not returned to the OS when the Python object is collected.

`Checker._verify_against_url` had the same defect, once per verified proxy.

Observed on a `forever` run filtered to a single country, measured with
`tools/mem_probe.py` in the consuming project:

| elapsed | RSS | proxies built | per proxy |
| --- | --- | --- | --- |
| start | 84 MB | 0 | — |
| 1 min | 6 430 MB | 7 700 | 826 KB |
| 2 min | 19 112 MB | 23 144 | 826 KB |

Left alone overnight this reached 78 GB in 17 hours.

The context carries no per-proxy state — `Proxy` only ever reads it, handing
it to `start_tls` — and sharing one context across many connections is the
ordinary way the stdlib is meant to be used. It is now built once, lazily, and
shared. `Proxy(verify_ssl=True)` is unaffected: that path never built a
context in the first place.

### Why `tracemalloc` did not find it

`tracemalloc` accounted for about 20 MB of a 19 GB growth, because almost none
of it lives on the Python heap. The `SSLContext` wrapper object is small; the
CA store behind it is not, and it is invisible to the Python allocator.

The signal that did identify it was the **object count**, not the size:
`ssl.py:438` showed `+7 700` then `+23 144` allocations, matching the number
of proxies built exactly. When RSS growth and `tracemalloc` totals disagree by
three orders of magnitude, look for a Python object that is cheap itself and
expensive underneath — an SSL context, a compiled regex cache, a DNS resolver,
a database connection.

Guarded by `TestSSLContextIsShared` in `tests/test_api.py`, which asserts on
the count of live `ssl.SSLContext` objects rather than on memory, since the
cost is not measurable from inside Python.

## Reviewing new code

Anything constructed inside `Proxy.__init__`, `Checker.check`, or a provider's
`get_proxies()` runs at proxy scale. Before adding one, ask whether it is
stateless enough to be built once and shared. In particular:

- `ssl.create_default_context()` — shared, see above
- `aiodns.DNSResolver` — one per `Broker`, passed down
- `aiohttp.ClientSession` — already per-check and closed via `async with`

`Resolver._cached_hosts` is a class-level dict that is deliberately never
cleared. It is keyed by hostname, and the only hostnames resolved are the
judges, so it stays small; proxy hosts are IP literals and never enter it.
