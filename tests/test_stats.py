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
