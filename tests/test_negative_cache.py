"""Тесты негативного кэша.

Кэш, который помнит слишком долго, выглядит снаружи как «провайдеры перестали
отдавать прокси»: пул сохнет, в логе тишина, и разбор уходит проверять
источники. Поэтому половина тестов здесь — про то, что кэш вовремя забывает.
"""

import pytest

from proxybroker import Broker
from proxybroker.negative_cache import NegativeCache


class TestПамять:
    def test_помнит_недавнюю_неудачу(self):
        c = NegativeCache(ttl=60)
        c.remember("10.0.0.1", 8080, now=100.0)
        assert c.is_known_dead("10.0.0.1", 8080, now=110.0)

    def test_незнакомый_адрес_не_мёртв(self):
        assert not NegativeCache(ttl=60).is_known_dead("10.0.0.9", 80, now=100.0)

    def test_порт_различается(self):
        c = NegativeCache(ttl=60)
        c.remember("10.0.0.1", 8080, now=100.0)
        assert not c.is_known_dead("10.0.0.1", 3128, now=100.0)


class TestЗабывание:
    def test_запись_протухает(self):
        """Бесплатные прокси мерцают: мёртвый пять минут назад может отвечать."""
        c = NegativeCache(ttl=60)
        c.remember("10.0.0.1", 8080, now=100.0)
        assert not c.is_known_dead("10.0.0.1", 8080, now=200.0)

    def test_протухшая_запись_удаляется_а_не_копится(self):
        c = NegativeCache(ttl=60)
        c.remember("10.0.0.1", 8080, now=100.0)
        c.is_known_dead("10.0.0.1", 8080, now=200.0)
        assert len(c) == 0

    def test_нулевой_ttl_отключает_кэш(self):
        """Аварийный выключатель без правки кода."""
        c = NegativeCache(ttl=0)
        assert not c.enabled
        c.remember("10.0.0.1", 8080)
        assert not c.is_known_dead("10.0.0.1", 8080)
        assert len(c) == 0


class TestРазмерОграничен:
    """Третья коллекция, способная расти без предела, — после двух утечек.

    Словарь, пополняемый на 3500 записей в минуту, повторил бы историю, которая
    дважды стоила десятков гигабайт.
    """

    def test_вытесняется_старейшее(self):
        c = NegativeCache(ttl=3600, max_entries=3)
        for i in range(5):
            c.remember(f"10.0.0.{i}", 80, now=100.0 + i)

        assert len(c) == 3
        assert not c.is_known_dead("10.0.0.0", 80, now=101.0), "старейшее вытеснено"
        assert c.is_known_dead("10.0.0.4", 80, now=105.0)

    def test_повтор_обновляет_свежесть_а_не_плодит(self):
        c = NegativeCache(ttl=3600, max_entries=10)
        c.remember("10.0.0.1", 80, now=100.0)
        c.remember("10.0.0.1", 80, now=200.0)
        assert len(c) == 1

    def test_потолок_не_может_быть_нулевым(self):
        """max_entries=0 означал бы кэш, который ничего не помнит и всё считает."""
        c = NegativeCache(ttl=3600, max_entries=0)
        c.remember("10.0.0.1", 80, now=100.0)
        assert len(c) == 1


class TestМусорныеДанные:
    def test_нечисловой_порт_означает_не_знаю(self):
        """Кэш зовётся до `Proxy.create`, где порт ещё сырой из списка.

        Мусор должен означать «не знаю», а не падение: отбраковкой занимается
        `Proxy.create`, а не кэш.
        """
        c = NegativeCache(ttl=60)
        c.remember("10.0.0.1", "не порт")
        assert not c.is_known_dead("10.0.0.1", "не порт")
        assert len(c) == 0

    def test_строковый_порт_работает(self):
        c = NegativeCache(ttl=60)
        c.remember("10.0.0.1", "8080", now=100.0)
        assert c.is_known_dead("10.0.0.1", 8080, now=100.0)


class TestИнтеграцияСБрокером:
    @pytest.mark.asyncio
    async def test_пропуски_видны_в_воронке(self):
        """Слишком агрессивный кэш неотличим от «источники сломались».

        Без отдельной ступени в воронке диагностика опять свелась бы к
        догадкам — ровно то, от чего уходили в PB8.
        """
        broker = Broker(timeout=0.1, max_tries=1, providers=[], stop_broker_on_sigint=False)
        broker._dead.remember("10.0.0.1", 8080)

        await broker._handle(("10.0.0.1", "8080", ()), check=True)

        assert broker.stats.known_dead == 1
        assert "мёртвые из кэша 1" in broker.stats.summary()
        broker.stop()

    @pytest.mark.asyncio
    async def test_без_проверки_кэш_не_вмешивается(self):
        """`grab` отдаёт адреса без проверки — фильтровать их по ней неправильно."""
        broker = Broker(timeout=0.1, max_tries=1, providers=[], stop_broker_on_sigint=False)
        broker._dead.remember("10.0.0.1", 8080)

        await broker._handle(("10.0.0.1", "8080", ()), check=False)

        assert broker.stats.known_dead == 0
        broker.stop()
