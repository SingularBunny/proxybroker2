import asyncio
import io
import signal
import warnings
from collections import Counter, defaultdict
from functools import partial
from pprint import pprint

from .checker import Checker
from .errors import ResolveError
from .negative_cache import (
    DEFAULT_MAX_ENTRIES as NEG_CACHE_MAX,
    DEFAULT_TTL as NEG_CACHE_TTL,
    NegativeCache,
)
from .pool_store import DEFAULT_TTL as DEFAULT_POOL_TTL, PoolStore
from .providers import PROVIDERS, Provider
from .stats import PoolStats
from .proxy import Proxy
from .resolver import Resolver
from .server import Server
from .utils import (
    IPPortPatternLine,
    IPv6BracketedPortPattern,
    canonicalize_ip,
    log,
)

# Pause between grabbing cycles; in seconds.
GRAB_PAUSE = 180

# The maximum number of providers that are parsed concurrently
MAX_CONCURRENT_PROVIDERS = 3

# Сколько ждать один источник, прежде чем пропустить его в этом проходе.
PROVIDER_TIMEOUT = 60


class Broker:
    """The Broker.

    | One broker to rule them all, one broker to find them,
    | One broker to bring them all and in the darkness bind them.

    :param asyncio.Queue queue: (optional) Queue of found/checked proxies
    :param int timeout: (optional) Timeout of a request in seconds
    :param int max_conn:
        (optional) The maximum number of concurrent checks of proxies
    :param int max_tries:
        (optional) The maximum number of attempts to check a proxy
    :param list judges:
        (optional) Urls of pages that show HTTP headers and IP address.
        Or :class:`~proxybroker.judge.Judge` objects
    :param list providers:
        (optional) Urls of pages where to find proxies.
        Or :class:`~proxybroker.providers.Provider` objects
    :param bool verify_ssl:
        (optional) Flag indicating whether to check the SSL certificates.
        Set to True to check ssl certifications
    :param loop: (optional) asyncio compatible event loop
    :param stop_broker_on_sigint: (optional) whether set SIGINT signal on broker object.
        Useful for a thread other than main thread.
    :param list ip_hosts:
        (optional) URLs used to detect this machine's external IP address.
        Each URL must return either a plain-text IP or a JSON object with an
        ``origin``, ``ip``, or ``query`` field (httpbin-compatible).
        Defaults to :attr:`Resolver._ip_hosts`.
        Set this to the same host(s) used as judges to guarantee IP consistency
        on multi-homed machines where different services may see different IPs.
    :param list provider_dirs:
        (optional) List of directories from which to load YAML/JSON
        provider config files at startup. Loaded providers are appended
        to ``providers`` (or to the default list if ``providers`` is
        ``None``). Safe for Docker bind-mounts: only data files are read,
        no Python is executed. Pass ``providers=[]`` together with
        ``provider_dirs=[...]`` to use ONLY the directory-loaded
        providers and disable the bundled defaults.

    .. deprecated:: 0.2.0
        Use :attr:`max_conn` and :attr:`max_tries` instead of
        :attr:`max_concurrent_conn` and :attr:`attempts_conn`.
    """

    def __init__(
        self,
        queue=None,
        timeout=8,
        max_conn=200,
        max_tries=3,
        judges=None,
        providers=None,
        verify_ssl=False,
        loop=None,
        stop_broker_on_sigint=True,
        ip_hosts=None,
        provider_dirs=None,
        verify_url=None,
        verify_timeout=10,
        verify_ok_statuses=None,
        max_concurrent_providers=None,
        grab_pause=None,
        pool_file=None,
        pool_ttl=DEFAULT_POOL_TTL,
        pool_save_interval=60,
        provider_timeout=PROVIDER_TIMEOUT,
        dead_ttl=NEG_CACHE_TTL,
        dead_max_entries=NEG_CACHE_MAX,
        **kwargs,
    ):
        # Both were module-level constants only, so callers passing them got them
        # swallowed by **kwargs and silently ignored: a config asking for 500
        # concurrent providers still scraped 3 at a time.
        self._max_concurrent_providers = (
            MAX_CONCURRENT_PROVIDERS
            if max_concurrent_providers is None
            else max(1, int(max_concurrent_providers))
        )
        self._grab_pause = GRAB_PAUSE if grab_pause is None else max(0, int(grab_pause))
        # Бюджет на один источник. В режиме `forever` длина прохода определяет
        # скорость пополнения пула, поэтому зависший на мёртвом сокете источник
        # задерживает прокси всех остальных.
        self._provider_timeout = max(1, int(provider_timeout))

        self._loop = self._resolve_loop(loop)
        self._proxies = queue or asyncio.Queue()
        self._resolver = Resolver(loop=self._loop, ip_hosts=ip_hosts)
        self._timeout = timeout
        self._verify_ssl = verify_ssl
        self._verify_url = verify_url
        self._verify_timeout = verify_timeout
        self._verify_ok_statuses = verify_ok_statuses

        # Verified proxies survive the process, so the next run starts from a
        # short list of addresses known to have worked instead of re-deriving
        # one from ~40 000 candidates. See `pool_store.py`.
        self._pool_store = PoolStore(pool_file, ttl=pool_ttl)
        self._pool_save_interval = pool_save_interval
        self._pool_save_task = None
        # Everything that passed the check this run, kept for saving. Bounded by
        # the number of *working* proxies, which is orders of magnitude below
        # the number of candidates — this is not the list that leaked in B12.
        self._verified = {}

        # Воронка «кандидат → проверка → пул». Без неё единственным сигналом о
        # состоянии остаются косвенные строки в логе — за 17 часов их было 868,
        # и ни одна не отвечала на вопрос, где именно теряются прокси.
        self.stats = PoolStats()
        # `unique_proxies` очищается между проходами намеренно — иначе пул
        # перестал бы принимать переопубликованный, но живой адрес. Платой шла
        # перепроверка тех же мёртвых адресов каждый цикл: ~3500 из 3800
        # кандидатов за проход. Кэш срезает их, не мешая живым вернуться.
        self._dead = NegativeCache(ttl=dead_ttl, max_entries=dead_max_entries)
        #: Кто первым принёс адрес — нужно, чтобы отнести прошедшую проверку к
        #: провайдеру. Живёт один проход, очищается вместе с `unique_proxies`.
        self._source_of = {}

        self.unique_proxies = {}
        # A set, and every task removes itself when it finishes — see _track().
        #
        # This was a plain list that only ever grew: a task was appended for every
        # provider on every pass and for every single proxy candidate checked, while
        # the only drain was _done(). That was survivable while a caller recreated the
        # Broker each cycle, but in `forever` mode _done() never runs, so completed
        # Task objects — each holding its result and coroutine frame — accumulated
        # without limit. Observed in production: 66 GB resident after 2.5 hours, on a
        # run that had not yet made a single request.
        self._all_tasks = set()
        self._checker = None
        self._server = None
        self._signal_handler_registered = False
        self._limit = 0  # not limited
        self._countries = None
        self._forever = False
        self._grab_task = None

        max_conn, max_tries = self._resolve_deprecated_limits(
            max_conn=max_conn,
            max_tries=max_tries,
            kwargs=kwargs,
        )

        # The maximum number of concurrent checking proxies
        self._on_check = asyncio.Queue(maxsize=max_conn)
        self._max_tries = max_tries
        self._judges = judges

        # Resolve the provider list. Contract:
        #   providers=None  -> use the bundled PROVIDERS defaults
        #   providers=[...] -> use exactly that list (empty stays empty)
        # provider_dirs entries are appended to whichever base was chosen,
        # so passing providers=[] with provider_dirs=['/configs'] yields
        # ONLY the directory-loaded providers.
        base_providers = self._resolve_providers(
            providers=providers, provider_dirs=provider_dirs
        )

        self._providers = [
            p if isinstance(p, Provider) else Provider(p) for p in base_providers
        ]
        if stop_broker_on_sigint and self._loop:
            try:
                self._loop.add_signal_handler(signal.SIGINT, self.stop)
                self._signal_handler_registered = True
                # add_signal_handler() is not implemented on Win
                # https://docs.python.org/3.5/library/asyncio-eventloops.html#windows
            except NotImplementedError:
                pass

    @staticmethod
    def _resolve_loop(loop):
        """Return running loop when available, else fall back to ``loop``."""
        try:
            return loop or asyncio.get_running_loop()
        except RuntimeError:
            # No running event loop, will be set later
            return loop

    @staticmethod
    def _resolve_deprecated_limits(*, max_conn, max_tries, kwargs):
        """Resolve deprecated limit kwargs into ``(max_conn, max_tries)``.

        Supports ``max_concurrent_conn`` and ``attempts_conn`` legacy kwargs
        while emitting deprecation warnings.

        :return: Tuple of resolved ``(max_conn, max_tries)`` values
        """
        max_concurrent_conn = kwargs.get("max_concurrent_conn")
        if max_concurrent_conn:
            warnings.warn(
                "`max_concurrent_conn` is deprecated, use `max_conn` instead",
                DeprecationWarning,
                stacklevel=2,
            )
            if isinstance(max_concurrent_conn, asyncio.Semaphore):
                max_conn = max_concurrent_conn._value
            else:
                max_conn = max_concurrent_conn

        attempts_conn = kwargs.get("attempts_conn")
        if attempts_conn:
            warnings.warn(
                "`attempts_conn` is deprecated, use `max_tries` instead",
                DeprecationWarning,
                stacklevel=2,
            )
            max_tries = attempts_conn
        return max_conn, max_tries

    @staticmethod
    def _resolve_providers(*, providers, provider_dirs):
        """Resolve final provider inputs from defaults, explicit list and dirs.

        ``providers=None`` uses bundled defaults; an explicit empty list stays
        empty. Any ``provider_dirs`` entries are appended to the selected base.
        """
        base_providers = list(PROVIDERS) if providers is None else list(providers)
        if not provider_dirs:
            return base_providers

        from .provider_utils import load_provider_configs_from_directory

        for directory in provider_dirs:
            base_providers.extend(load_provider_configs_from_directory(directory))
        return base_providers

    async def grab(self, *, countries=None, limit=0):
        """Gather proxies from the providers without checking.

        :param list countries: (optional) List of ISO country codes
                               where should be located proxies
        :param int limit: (optional) The maximum number of proxies

        :ref:`Example of usage <proxybroker-examples-grab>`.
        """
        self._countries = countries
        self._limit = limit
        task = asyncio.create_task(self._grab(check=False))
        self._track(task)

    async def find(
        self,
        *,
        types=None,
        data=None,
        countries=None,
        post=False,
        strict=False,
        dnsbl=None,
        limit=0,
        wait=False,
        forever=False,
        **kwargs,
    ):
        """Gather and check proxies from providers or from a passed data.

        :ref:`Example of usage <proxybroker-examples-find>`.

        :param list types:
            Types (protocols) that need to be check on support by proxy.
            Supported: HTTP, HTTPS, SOCKS4, SOCKS5, CONNECT:80, CONNECT:25
            And levels of anonymity (HTTP only): Transparent, Anonymous, High
        :param data:
            (optional) String or list with proxies. Also can be a file-like
            object supports `read()` method. Used instead of providers
        :param list countries:
            (optional) List of ISO country codes where should be located
            proxies
        :param bool post:
            (optional) Flag indicating use POST instead of GET for requests
            when checking proxies
        :param bool strict:
            (optional) Flag indicating that anonymity levels of types
            (protocols) supported by a proxy must be equal to the requested
            types and levels of anonymity. By default, strict mode is off and
            for a successful check is enough to satisfy any one of the
            requested types
        :param list dnsbl:
            (optional) Spam databases for proxy checking.
            `Wiki <https://en.wikipedia.org/wiki/DNSBL>`_
        :param int limit: (optional) The maximum number of proxies
        :param bool wait:
            (optional) Block until grabbing finishes instead of returning as soon
            as the tasks are scheduled. Has no meaning together with *forever*.
        :param bool forever:
            (optional) Keep replenishing the queue indefinitely: after every pass
            over the providers sleep *grab_pause* seconds and start again, so the
            consumer can just wait on the queue for a fresh proxy. Without it the
            broker makes a single pass, then stops and pushes ``None`` into the
            queue as an end-of-stream marker. Mutually exclusive with *limit*.
        :param str verify_url:
            (optional) URL to test each proxy against after judge validation.
            Proxies that receive a response with status >= 400 or that time out
            are discarded. Requires ``aiohttp`` and ``aiohttp-socks``.

            Useful against sites that reject whole IP ranges — one probe saves
            many failed requests later.

            **Counter-productive against sites that allow roughly one request
            per fresh IP.** The probe spends that request, so the proxy enters
            the pool already burned. Real example: a client scraping cian.ru
            plumbed this parameter through five API clients and then disabled it
            everywhere for exactly this reason.

            There is deliberately no CLI flag for this: whether it helps depends
            on the target site's blocking model, and a flag invites use without
            that judgement.
        :param float verify_timeout:
            (optional) Timeout in seconds for the *verify_url* request
            (default: 10).
        :param set verify_ok_statuses:
            (optional) Set of HTTP status codes considered "passing".
            Defaults to any status < 400 (i.e. 1xx, 2xx, 3xx).

        :raises ValueError:
            If :attr:`types` not given.

        .. versionchanged:: 0.2.0
            Added: :attr:`post`, :attr:`strict`, :attr:`dnsbl`.
            Changed: :attr:`types` is required.
        """
        # Validate the call before doing any network work: these are programming
        # errors, and finding out after a DNS round-trip only obscures them.
        if forever and limit:
            raise ValueError(
                "`limit` and `forever` are mutually exclusive: reaching the limit "
                "stops the broker and pushes the end-of-stream sentinel into the queue."
            )

        ips = await self._resolver.get_real_ext_ips()
        types = _update_types(types)

        if not types:
            raise ValueError("`types` is required")

        self._checker = Checker(
            judges=self._judges,
            timeout=self._timeout,
            verify_ssl=self._verify_ssl,
            max_tries=self._max_tries,
            real_ext_ips=ips,
            types=types,
            post=post,
            strict=strict,
            dnsbl=dnsbl,
            loop=self._loop,
            verify_url=self._verify_url,
            verify_timeout=self._verify_timeout,
            verify_ok_statuses=self._verify_ok_statuses,
        )
        self._countries = countries
        self._limit = limit
        self._forever = forever

        tasks = [asyncio.create_task(self._checker.check_judges())]
        if data:
            task = asyncio.create_task(self._load(data, check=True))
        else:
            # Re-check what worked last time before walking the providers. These
            # go through the normal check — they skip discovery, not validation,
            # because a pool pre-filled with dead addresses is worse than an
            # empty one.
            known = self._pool_store.load(countries=countries)
            self.stats.note_from_store(len(known))
            if known:
                warm = asyncio.create_task(self._load(known, check=True))
                tasks.append(warm)
                self._track(warm)
            task = asyncio.create_task(self._grab(types, check=True))
        tasks.append(task)
        self._track(*tasks)
        self._grab_task = task
        self._start_pool_autosave()

        if wait:
            await self.wait_until_done()

    async def wait_until_done(self):
        """Wait until grabbing/checking finishes (or ``limit`` proxies are found).

        ``find()`` only schedules tasks and returns, which is easy to misread as
        "run to completion" — a caller doing ``await broker.find(...)`` and then
        measuring the queue sees nothing yet, and dropping the Broker afterwards
        orphans tasks that keep running. Await this to actually block.
        """
        task = getattr(self, "_grab_task", None)
        if task is None:
            return
        try:
            await task
        except asyncio.CancelledError:
            # _done() cancels the grab task once `limit` is reached — expected.
            pass

    def serve(self, host="127.0.0.1", port=8888, limit=100, **kwargs):
        """Start a local proxy server.

        The server distributes incoming requests to a pool of found proxies.

        When the server receives an incoming request, it chooses the optimal
        proxy (based on the percentage of errors and average response time)
        and passes to it the incoming request.

        In addition to the parameters listed below are also accept all the
        parameters of the :meth:`.find` method and passed it to gather proxies
        to a pool.

        :ref:`Example of usage <proxybroker-examples-server>`.

        :param str host: (optional) Host of local proxy server
        :param int port: (optional) Port of local proxy server
        :param int limit:
            (optional) When will be found a requested number of working
            proxies, checking of new proxies will be lazily paused.
            Checking will be resumed if all the found proxies will be discarded
            in the process of working with them (see :attr:`max_error_rate`,
            :attr:`max_resp_time`). And will continue until it finds one
            working proxy and paused again. The default value is 100
        :param int max_tries:
            (optional) The maximum number of attempts to handle an incoming
            request. If not specified, it will use the value specified during
            the creation of the :class:`Broker` object. Attempts can be made
            with different proxies. The default value is 3
        :param int strategy:
            (optional) The strategy used for picking proxy from pool.
            The default value is 'best'
        :param int min_queue:
            (optional) The minimum number of proxies to choose from
                before deciding which is the most suitable to use.
                The default value is 5
        :param int min_req_proxy:
            (optional) The minimum number of processed requests to estimate the
            quality of proxy (in accordance with :attr:`max_error_rate` and
            :attr:`max_resp_time`). The default value is 5
        :param int max_error_rate:
            (optional) The maximum percentage of requests that ended with
            an error. For example: 0.5 = 50%. If proxy.error_rate exceeds this
            value, proxy will be removed from the pool.
            The default value is 0.5
        :param int max_resp_time:
            (optional) The maximum response time in seconds.
            If proxy.avg_resp_time exceeds this value, proxy will be removed
            from the pool. The default value is 8
        :param bool prefer_connect:
            (optional) Flag that indicates whether to use the CONNECT method
            if possible. For example: If is set to True and a proxy supports
            HTTP proto (GET or POST requests) and CONNECT method, the server
            will try to use CONNECT method and only after that send the
            original request. The default value is False
        :param list http_allowed_codes:
            (optional) Acceptable HTTP codes returned by proxy on requests.
            If a proxy return code, not included in this list, it will be
            considered as a proxy error, not a wrong/unavailable address.
            For example, if a proxy will return a ``404 Not Found`` response -
            this will be considered as an error of a proxy.
            Checks only for HTTP protocol, HTTPS not supported at the moment.
            By default the list is empty and the response code is not verified
        :param int backlog:
            (optional) The maximum number of queued connections passed to
            listen. The default value is 100

        :raises ValueError:
            If :attr:`limit` is less than or equal to zero.
            Because a parsing of providers will be endless

        .. versionadded:: 0.2.0
        """

        if limit <= 0:
            raise ValueError(
                "In serve mode value of the limit cannot be less than or "
                "equal to zero. Otherwise, a parsing of providers will be "
                "endless"
            )

        self._server = Server(
            host=host,
            port=port,
            proxies=self._proxies,
            timeout=self._timeout,
            max_tries=kwargs.pop("max_tries", self._max_tries),
            loop=self._loop,
            **kwargs,
        )

        async def run_server():
            await self._server.start()
            asyncio.create_task(self.find(limit=limit, **kwargs))

        self._loop.run_until_complete(run_server())

    async def _load(self, data, check=True):
        """Looking for proxies in the passed data.

        Transform the passed data from [raw string | file-like object | list]
        to set {(host, port), ...}: {('192.168.0.1', '80'), }
        """
        log.debug("Load proxies from the raw data")
        if isinstance(data, io.TextIOWrapper):
            data = data.read()
        if isinstance(data, str):
            # Extract bracketed v6 entries first, then mask their spans
            # in the input before running the v4 line regex. Without
            # masking, an IPv4-mapped v6 entry like `[::ffff:1.2.3.4]:8080`
            # would also produce a phantom v4 entry `1.2.3.4:8080` from
            # the embedded literal. RFC 6874 zone IDs in brackets are
            # accepted by the regex; validation is via canonicalize_ip.
            v6_pairs = []
            for raw_v6, port in IPv6BracketedPortPattern.findall(data):
                canonical = canonicalize_ip(raw_v6)
                if canonical is not None:
                    v6_pairs.append((canonical, port))
            v4_input = IPv6BracketedPortPattern.sub(
                lambda m: " " * len(m.group(0)), data
            )
            v4_pairs = IPPortPatternLine.findall(v4_input)
            data = v4_pairs + v6_pairs
        proxies = set(data)
        for proxy in proxies:
            await self._handle(proxy, check=check)
        await self._on_check.join()
        self._done()

    async def _grab(self, types=None, check=False):
        async def _fetch(provider):
            # Providers scrape third-party HTML and break all the time — a changed
            # layout, a 502, a rate limit. One of them must never be able to abort
            # the whole pass: that would starve the pool of every other provider's
            # proxies too.
            #
            # The handling lives here rather than at the `as_completed` site
            # because only here is the provider still in scope. Previously the log
            # read "Provider failed: IndexError('list index out of range')" with no
            # way to tell which of ~30 sources had the broken parser.
            try:
                # A snapshot, not the provider's live set. `get_proxies()` returns
                # `Provider._proxies` itself, and the provider keeps adding to it
                # from its own concurrent page fetches — iterating it directly
                # raised "Set changed size during iteration", which killed proxy
                # discovery for the whole client until restart.
                #
                # The timeout is not decoration: in `forever` mode the pass length
                # sets how fast the pool refills, so one source that hangs on a
                # dead socket delays every other provider's proxies behind it.
                proxies = list(
                    await asyncio.wait_for(
                        provider.get_proxies(), timeout=self._provider_timeout
                    )
                )
                self.stats.note_provider(str(provider), len(proxies))
                return provider, proxies
            except asyncio.TimeoutError:
                log.warning(
                    f"{provider} exceeded {self._provider_timeout}s, skipping it"
                )
                self.stats.note_provider(str(provider), 0, timed_out=True)
                return provider, []
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                # Teardown noise ("Session is closed", a resolver that is already
                # gone) says nothing about the provider — it means we are shutting
                # down. Keep it at debug so a real provider breakage stays visible
                # in a normal-level log.
                text = str(exc)
                is_teardown = isinstance(exc, (RuntimeError, AttributeError)) and (
                    "closed" in text or "getaddrinfo" in text or "Event loop" in text
                )
                if is_teardown or "Connector is closed" in text:
                    log.debug(f"{provider} stopped during shutdown: {exc!r}")
                else:
                    log.warning(f"{provider} failed, skipping it: {exc!r}")
                    self.stats.note_provider(str(provider), 0, failed=True)
                return provider, []

        def _get_tasks(by=None):
            by = self._max_concurrent_providers if by is None else by
            providers = [
                pr
                for pr in self._providers
                if not types or not pr.proto or bool(pr.proto & types.keys())
            ]
            while providers:
                tasks = [asyncio.create_task(_fetch(pr)) for pr in providers[:by]]
                del providers[:by]
                self._track(*tasks)
                yield tasks

        log.debug("Start grabbing proxies")
        while True:
            try:
                for tasks in _get_tasks():
                    for task in asyncio.as_completed(tasks):
                        # `_fetch` already contained any provider-level failure and
                        # returned an empty list.
                        provider, proxies = await task
                        for proxy in proxies:
                            await self._handle(proxy, check=check, source=str(provider))
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                # A pass may still fail in a way no provider owns — a bug here, a
                # resolver that went away. In `forever` mode that used to end
                # discovery permanently: the exception unwound through `find()`,
                # the caller logged it once, and the pool then sat empty for the
                # rest of the process's life while the app logged "pool exhausted"
                # every 30 seconds. A long-lived broker must survive its own bad
                # pass and try again.
                if not (self._server or self._forever):
                    raise
                log.exception(f"Grab cycle failed, retrying next pass: {exc!r}")
            self.stats.note_pass_complete()
            log.info(f"Проход завершён — {self.stats.summary(self._proxies.qsize())}")
            if self._server or self._forever:
                log.debug(f"fall asleep for {self._grab_pause} seconds")
                await asyncio.sleep(self._grab_pause)
                log.debug("awaked")
                # Providers republish the same lists; without this the pool would
                # keep rejecting known-dead entries and never take a fresh one.
                self.unique_proxies.clear()
                self._source_of.clear()
            else:
                break
        await self._on_check.join()
        self._done()

    async def _handle(self, proxy, check=False, source=None):
        self.stats.note_candidate()
        host, port = proxy[0], proxy[1]
        if check and self._dead.is_known_dead(host, port):
            self.stats.note_known_dead()
            return
        try:
            proxy = await Proxy.create(
                *proxy,
                timeout=self._timeout,
                resolver=self._resolver,
                verify_ssl=self._verify_ssl,
                loop=self._loop,
            )
        except (ResolveError, ValueError):
            return

        if not self._is_unique(proxy):
            return
        if source:
            # Кто первым принёс этот адрес. Провайдер, приносящий только
            # повторы за другими, бесполезен, и без атрибуции это не видно.
            self._source_of[(proxy.host, proxy.port)] = source
            self.stats.note_provider_unique(source)
        if not self._geo_passed(proxy):
            return

        if check:
            await self._push_to_check(proxy)
        else:
            self._push_to_result(proxy)

    def _is_unique(self, proxy):
        if (proxy.host, proxy.port) not in self.unique_proxies:
            self.unique_proxies[(proxy.host, proxy.port)] = proxy
            return True
        else:
            self.stats.note_duplicate()
            return False

    def _geo_passed(self, proxy):
        if self._countries and (proxy.geo.code not in self._countries):
            proxy.log("Location of proxy is outside the given countries list")
            self.stats.note_geo_rejected()
            return False
        else:
            return True

    async def _push_to_check(self, proxy):
        def _task_done(proxy, f):
            self._on_check.task_done()
            if not self._on_check.empty():
                self._on_check.get_nowait()
            try:
                self.stats.note_checked()
                if not f.result():
                    self._dead.remember(proxy.host, proxy.port)
                if f.result():
                    # proxy is working and its types is equal to the requested
                    self.stats.note_passed(
                        getattr(proxy.geo, "code", None),
                        source=self._source_of.get((proxy.host, proxy.port)),
                    )
                    self._push_to_result(proxy)
            except asyncio.CancelledError:
                pass

        if self._server and not self._proxies.empty() and self._limit <= 0:
            log.debug(f"pause. proxies: {self._proxies.qsize()}; limit: {self._limit}")
            await self._proxies.join()
            log.debug(f"unpause. proxies: {self._proxies.qsize()}")

        await self._on_check.put(None)
        task = asyncio.create_task(self._checker.check(proxy))
        task.add_done_callback(partial(_task_done, proxy))
        self._track(task)

    def _push_to_result(self, proxy):
        log.debug(f"push to result: {proxy!r}")
        if proxy is not None and self._pool_store.enabled:
            self._verified[(proxy.host, proxy.port)] = proxy
        self._proxies.put_nowait(proxy)
        self._update_limit()

    # ------------------------------------------------------------------ #
    # Pool persistence                                                     #
    # ------------------------------------------------------------------ #

    def _start_pool_autosave(self):
        """Persist the working set periodically, not only on a clean stop.

        `stop()` does not run when the process is killed, and a long run that
        ends with `kill` would otherwise throw away everything it had verified —
        which is exactly the case persistence exists for.
        """
        if not self._pool_store.enabled or self._pool_save_task is not None:
            return
        if not self._pool_save_interval:
            return

        async def _autosave():
            while True:
                await asyncio.sleep(self._pool_save_interval)
                self.save_pool()

        self._pool_save_task = asyncio.create_task(_autosave())
        self._track(self._pool_save_task)

    def pool_stats(self) -> dict:
        """Срез воронки «кандидат → проверка → пул» вместе с размером очереди.

        Отдаёт словарь, а не печатает: вызывающему может понадобиться отдать
        это в метрики, в тест или в собственный лог. Строку для лога даёт
        `Broker.stats.summary()`.
        """
        return self.stats.as_dict(pool_size=self._proxies.qsize())

    def save_pool(self) -> int:
        """Write the proxies verified during this run. Returns entries stored."""
        if not self._pool_store.enabled or not self._verified:
            return 0
        return self._pool_store.save(list(self._verified.values()))

    def _update_limit(self):
        self._limit -= 1
        if self._limit == 0 and not self._server:
            self._done()

    def stop(self):
        """Stop all tasks, and the local proxy server if it's running."""
        self._done()
        if self._server:
            self._server.stop()
            self._server = None
        # Clean up signal handler to prevent memory leak
        if self._signal_handler_registered and self._loop:
            try:
                self._loop.remove_signal_handler(signal.SIGINT)
                self._signal_handler_registered = False
            except (NotImplementedError, ValueError):
                # NotImplementedError on Windows, ValueError if handler wasn't set
                pass
        log.info("Stop!")

    def _track(self, *tasks):
        """Remember tasks so stop() can cancel them, without retaining them forever.

        The done-callback is what keeps the set bounded: a finished task drops out on
        its own, so the set holds only work that is still in flight.
        """
        for task in tasks:
            self._all_tasks.add(task)
            task.add_done_callback(self._all_tasks.discard)

    def _done(self):
        log.debug("called done")
        # Persist before cancelling tasks: `_done()` is the one hook that runs on
        # every ending — `limit` reached, `stop()`, SIGINT — whereas `stop()` is
        # not called by the CLI at all. Saving in `stop()` alone meant a `find`
        # run that hit its limit wrote nothing, and the autosave task was
        # cancelled below before its first tick.
        self.save_pool()
        # Итог воронки в конце — по нему видно, на какой ступени терялись
        # прокси, без чтения всего лога.
        log.info(f"Итог — {self.stats.summary(self._proxies.qsize())}")
        while self._all_tasks:
            task = self._all_tasks.pop()
            if not task.done():
                task.cancel()
        self._push_to_result(None)
        log.info(f"Done! Total found proxies: {len(self.unique_proxies)}")

    def show_stats(self, verbose=False, **kwargs):
        """Show statistics on the found proxies.

        Useful for debugging, but you can also use if you're interested.

        :param verbose: Flag indicating whether to print verbose stats

        .. deprecated:: 0.2.0
            Use :attr:`verbose` instead of :attr:`full`.
        """
        if kwargs:
            verbose = True
            warnings.warn(
                "`full` in `show_stats` is deprecated, use `verbose` instead.",
                DeprecationWarning,
                stacklevel=2,
            )

        found_proxies = self.unique_proxies.values()
        num_working_proxies = len([p for p in found_proxies if p.is_working])

        if not found_proxies:
            print("Proxy not found")
            return

        errors = Counter()
        for p in found_proxies:
            errors.update(p.stat["errors"])

        proxies_by_type = {
            "SOCKS5": [],
            "SOCKS4": [],
            "HTTPS": [],
            "HTTP": [],
            "CONNECT:80": [],
            "CONNECT:25": [],
        }

        stat = {
            "Wrong country": [],
            "Wrong protocol/anonymity lvl": [],
            "Connection success": [],
            "Connection timeout": [],
            "Connection failed": [],
        }

        for p in found_proxies:
            msgs = " ".join([x[1] for x in p.get_log()])
            full_log = [p]
            for proto in p.types:
                proxies_by_type[proto].append(p)
            if "Location of proxy" in msgs:
                stat["Wrong country"].append(p)
            elif "Connection: success" in msgs:
                if "Protocol or the level" in msgs:
                    stat["Wrong protocol/anonymity lvl"].append(p)
                stat["Connection success"].append(p)
                if not verbose:
                    continue
                events_by_ngtr = defaultdict(list)
                for ngtr, event, runtime in p.get_log():
                    events_by_ngtr[ngtr].append((event, runtime))
                for ngtr, events in sorted(
                    events_by_ngtr.items(), key=lambda item: item[0]
                ):
                    full_log.append(f"\t{ngtr}")
                    for event, runtime in events:
                        if event.startswith("Initial connection"):
                            full_log.append("\t\t-------------------")
                        else:
                            full_log.append(f"\t\t{event:<66} Runtime: {runtime:.2f}")
                for row in full_log:
                    print(row)
            elif "Connection: failed" in msgs:
                stat["Connection failed"].append(p)
            else:
                stat["Connection timeout"].append(p)
        if verbose:
            print("Stats:")
            pprint(stat)

        print(f"The number of working proxies: {num_working_proxies}")
        for proto, proxies in proxies_by_type.items():
            print(f"{proto} ({len(proxies)}): {proxies}")
        print("Errors:", errors)


def _update_types(types):
    _types = {}
    if not types:
        return _types
    elif isinstance(types, dict):
        return types
    for tp in types:
        lvl = None
        if isinstance(tp, (list, tuple, set)):
            tp, lvl = tp[0], tp[1]
            if isinstance(lvl, str):
                lvl = lvl.split()
        _types[tp] = lvl
    return _types
