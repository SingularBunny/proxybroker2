"""Тесты воронки пула.

Метрика, которая врёт, хуже её отсутствия: по ней принимают решение «источники
сломались» вместо «фильтр слишком узкий», и разбор уходит не туда. Именно это и
произошло на семнадцатичасовом прогоне — единственным сигналом были 868 строк
«pool exhausted», ни одна из которых не отвечала, где теряются прокси.
"""

import asyncio

import pytest

from proxybroker import Broker, Proxy
from proxybroker.stats import PoolStats


def _proxy(host="10.0.0.1", port=8080, country="RU"):
    p = Proxy(host, port)
    p._geo = p._geo._replace(code=country)
    return p


class TestВоронка:
    def test_ступени_считаются_отдельно(self):
        s = PoolStats()
        for _ in range(10):
            s.note_candidate()
        s.note_duplicate()
        s.note_geo_rejected()
        s.note_checked()
        s.note_passed("RU")

        срез = s.as_dict()
        assert срез["candidates"] == 10
        assert срез["duplicates"] == 1
        assert срез["geo_rejected"] == 1
        assert срез["checked"] == 1
        assert срез["passed"] == 1

    def test_доля_прошедших(self):
        s = PoolStats()
        for _ in range(4):
            s.note_checked()
        s.note_passed()
        assert s.pass_rate == pytest.approx(25.0)

    def test_без_проверок_доля_неизвестна_а_не_ноль(self):
        """«Ноль процентов» и «нечего измерять» — разные состояния.

        Если бы пустая статистика отдавала 0%, разбор начался бы с вывода
        «все прокси мёртвые», хотя проверка ещё не начиналась.
        """
        assert PoolStats().pass_rate is None

    def test_страны_прошедших(self):
        s = PoolStats()
        s.note_passed("RU")
        s.note_passed("RU")
        s.note_passed("DE")
        assert s.as_dict()["countries"] == {"RU": 2, "DE": 1}

    def test_проход_отделён_от_накопленного(self):
        """Накопленное за сутки скрывает деградацию последнего часа."""
        s = PoolStats()
        for _ in range(100):
            s.note_candidate()
        s.note_pass_complete()
        for _ in range(3):
            s.note_candidate()
        s.note_pass_complete()

        срез = s.as_dict()
        assert срез["candidates"] == 103, "накопительный счётчик считает всё"
        assert срез["last_pass"]["candidates"] == 3, "последний проход — только свой вклад"
        assert срез["passes"] == 2


class TestСтрокаДляЛога:
    def test_называет_ступень_на_которой_теряются(self):
        """Главный сценарий: пул пуст, потому что фильтр по стране режет всё.

        Без разбивки это выглядит как «источники сломались», и разбор уходит
        проверять провайдеров вместо настроек.
        """
        s = PoolStats()
        for _ in range(1000):
            s.note_candidate()
            s.note_geo_rejected()

        строка = s.summary(pool_size=0)
        assert "не та страна 1000" in строка
        assert "пул 0" in строка

    def test_пустая_статистика_не_врёт_процентами(self):
        строка = PoolStats().summary()
        assert "%" not in строка

    def test_упоминает_сохранённый_пул(self):
        s = PoolStats()
        s.note_from_store(12)
        assert "из сохранённого пула 12" in s.summary()


class TestИнтеграцияСБрокером:
    @pytest.mark.asyncio
    async def test_дубликаты_учитываются(self):
        broker = Broker(timeout=0.1, max_tries=1, providers=[], stop_broker_on_sigint=False)
        broker._is_unique(_proxy("10.0.0.1"))
        broker._is_unique(_proxy("10.0.0.1"))  # тот же адрес

        assert broker.stats.duplicates == 1
        broker.stop()

    @pytest.mark.asyncio
    async def test_отбраковка_по_стране_учитывается(self):
        broker = Broker(timeout=0.1, max_tries=1, providers=[], stop_broker_on_sigint=False)
        broker._countries = ["RU"]

        assert broker._geo_passed(_proxy(country="RU"))
        assert not broker._geo_passed(_proxy(country="DE"))

        assert broker.stats.geo_rejected == 1
        broker.stop()

    @pytest.mark.asyncio
    async def test_pool_stats_включает_размер_очереди(self):
        broker = Broker(timeout=0.1, max_tries=1, providers=[], stop_broker_on_sigint=False)
        broker._proxies.put_nowait(_proxy())

        assert broker.pool_stats()["pool_size"] == 1
        broker.stop()

    @pytest.mark.asyncio
    async def test_учёт_не_роняет_проход(self):
        """Наблюдаемость не имеет права стоить прогона."""
        broker = Broker(timeout=0.1, max_tries=1, providers=[], stop_broker_on_sigint=False)

        class _Провайдер:
            proto = set()

            async def get_proxies(self):
                return [("10.0.0.1", "8080"), ("10.0.0.1", "8080")]

        broker._providers = [_Провайдер()]
        await broker._grab(types={"HTTP": None}, check=False)

        assert broker.stats.candidates == 2
        assert broker.stats.passes == 1
        broker.stop()


class TestВкладПровайдеров:
    """Без атрибуции нельзя ответить, какой источник заменить.

    38 провайдеров создают вид разнообразия, но большинство живёт на GitHub и
    его CDN. Понять, кто из них реально приносит живые прокси, а кто только
    повторяет чужое, можно лишь измерив вклад каждого.
    """

    def test_вклад_разложен_по_ступеням(self):
        s = PoolStats()
        s.note_provider("A", 100)
        for _ in range(40):
            s.note_provider_unique("A")
        s.note_passed("RU", source="A")

        вклад = s.as_dict()["providers"]["A"]
        assert вклад == {
            "yielded": 100, "unique": 40, "passed": 1, "failures": 0, "timeouts": 0
        }

    def test_повторы_за_другими_видны(self):
        """Источник, отдающий только чужие адреса, бесполезен при любом объёме."""
        s = PoolStats()
        s.note_provider("зеркало", 500)  # ни одного note_provider_unique

        assert s.as_dict()["providers"]["зеркало"]["unique"] == 0

    def test_бесполезные_по_порогу(self):
        s = PoolStats()
        s.note_provider("много_но_мёртвые", 500)
        s.note_provider("мало_и_рано_судить", 3)
        s.note_provider("рабочий", 500)
        s.note_passed(source="рабочий")

        assert s.useless_providers() == ["много_но_мёртвые"]

    def test_порог_защищает_от_поспешного_вывода(self):
        """«Ноль из трёх» и «ноль из тысячи» — разные утверждения."""
        s = PoolStats()
        s.note_provider("новичок", 3)
        assert s.useless_providers(min_yielded=50) == []

    def test_сбои_и_таймауты_считаются_отдельно(self):
        """Сломанный парсер и зависший сокет чинятся по-разному."""
        s = PoolStats()
        s.note_provider("медленный", 0, timed_out=True)
        s.note_provider("сломанный", 0, failed=True)

        assert s.providers["медленный"]["timeouts"] == 1
        assert s.providers["медленный"]["failures"] == 0
        assert s.providers["сломанный"]["failures"] == 1
        assert s.providers["сломанный"]["timeouts"] == 0


class TestБюджетПровайдера:
    @pytest.mark.asyncio
    async def test_зависший_источник_не_держит_проход(self):
        """В режиме forever длина прохода задаёт скорость пополнения пула.

        Один источник, повисший на мёртвом сокете, задерживал прокси всех
        остальных за собой.
        """
        broker = Broker(
            timeout=0.1, max_tries=1, providers=[],
            stop_broker_on_sigint=False, provider_timeout=1,
        )

        class _Зависший:
            proto = set()

            def __repr__(self):
                return "<Provider завис.test>"

            async def get_proxies(self):
                await asyncio.sleep(3600)

        class _Быстрый:
            proto = set()

            async def get_proxies(self):
                return [("127.0.0.1", "8080")]

        broker._providers = [_Зависший(), _Быстрый()]
        собрано = []

        async def _handle(proxy, check=False, source=None):
            собрано.append(proxy)

        broker._handle = _handle

        await asyncio.wait_for(
            broker._grab(types={"HTTP": None}, check=False), timeout=10
        )
        broker.stop()

        assert ("127.0.0.1", "8080") in собрано, "быстрый источник не должен ждать"
        assert broker.stats.providers["<Provider завис.test>"]["timeouts"] == 1

    @pytest.mark.asyncio
    async def test_атрибуция_доходит_до_статистики(self):
        broker = Broker(timeout=0.1, max_tries=1, providers=[], stop_broker_on_sigint=False)

        class _Источник:
            proto = set()

            def __repr__(self):
                return "<Provider списки.test>"

            async def get_proxies(self):
                return [("127.0.0.1", "8080"), ("127.0.0.2", "3128")]

        broker._providers = [_Источник()]
        await broker._grab(types={"HTTP": None}, check=False)
        broker.stop()

        вклад = broker.stats.providers["<Provider списки.test>"]
        assert вклад["yielded"] == 2
        assert вклад["unique"] == 2
