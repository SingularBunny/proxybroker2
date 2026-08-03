"""Помнить, что адрес только что не прошёл проверку.

Зачем
-----
``unique_proxies`` очищается между проходами, и это правильно: провайдеры
переопубликовывают те же списки, и без сброса пул перестал бы принимать
переопубликованный, но живой адрес. Платой идёт перепроверка одних и тех же
заведомо мёртвых адресов каждый цикл.

Масштаб платы виден по замеру: за проход приходит порядка 3800 кандидатов, из
которых проверку проходят около 340. Остальные 3500 проверяются заново каждые
60 секунд, занимая слоты проверки, которые могли бы достаться новым адресам.

Три вещи, без которых это стало бы хуже, чем ничего
--------------------------------------------------
**TTL обязателен, и короткий.** Бесплатные прокси мерцают: адрес, мёртвый пять
минут назад, вполне может отвечать сейчас. Кэш без срока превратил бы разовую
неудачу в пожизненный бан и медленно осушил бы пул.

**Размер ограничен.** Словарь, растущий на 3500 записей каждую минуту, — это
третья утечка того же семейства, что уже дважды стоили нам десятков гигабайт
(см. ``docs/memory.md``). Вытесняется самое старое.

**Пропуски видны.** Слишком агрессивный кэш выглядит снаружи ровно как
«провайдеры перестали отдавать прокси». Счётчик пропущенных попадает в ту же
воронку, что и остальные ступени, иначе диагностика опять сведётся к догадкам.
"""

from __future__ import annotations

import time
from collections import OrderedDict
from typing import Optional, Tuple

#: Сколько адрес считается мёртвым. Меньше паузы между проходами не имеет
#: смысла — адрес всё равно вернётся на следующем круге; больше часа опасно,
#: потому что бесплатные прокси за это время успевают ожить.
DEFAULT_TTL = 900

#: Потолок записей. 20 000 адресов — это несколько проходов при типичных
#: 3500 кандидатах, чего достаточно, чтобы срезать повторы, и мало настолько,
#: что о памяти можно не думать.
DEFAULT_MAX_ENTRIES = 20_000


class NegativeCache:
    """Адреса, недавно не прошедшие проверку.

    :param ttl: сколько секунд помнить неудачу.
    :param max_entries: потолок записей; при переполнении вытесняется старейшая.

    ``ttl=0`` полностью отключает кэш — на случай, если поведение окажется
    вредным для чьего-то набора источников и это нужно будет выключить без
    правки кода.
    """

    def __init__(
        self, ttl: int = DEFAULT_TTL, max_entries: int = DEFAULT_MAX_ENTRIES
    ) -> None:
        self._ttl = ttl
        self._max_entries = max(1, max_entries)
        self._entries: "OrderedDict[Tuple[str, int], float]" = OrderedDict()

    @property
    def enabled(self) -> bool:
        return self._ttl > 0

    def __len__(self) -> int:
        return len(self._entries)

    @staticmethod
    def _key(host: str, port) -> Optional[Tuple[str, int]]:
        """Ключ или ``None``, если порт не число.

        Кэш вызывается до `Proxy.create`, где порт ещё сырой из списка
        провайдера. Мусорное значение должно означать «не знаю», а не падение:
        отбраковкой мусора занимается `Proxy.create`, а не кэш.
        """
        try:
            return (host, int(port))
        except (TypeError, ValueError):
            return None

    def remember(self, host: str, port: int, now: Optional[float] = None) -> None:
        """Запомнить, что адрес не прошёл проверку."""
        if not self.enabled:
            return
        ключ = self._key(host, port)
        if ключ is None:
            return
        self._entries.pop(ключ, None)  # переносим в конец: запись свежая
        self._entries[ключ] = now if now is not None else time.monotonic()
        while len(self._entries) > self._max_entries:
            self._entries.popitem(last=False)

    def is_known_dead(self, host: str, port: int, now: Optional[float] = None) -> bool:
        """Проверялся ли адрес недавно и неудачно.

        Просроченная запись удаляется сразу: иначе она заняла бы место до
        вытеснения, а вытеснение идёт по возрасту вставки, а не по сроку.
        """
        if not self.enabled:
            return False
        ключ = self._key(host, port)
        if ключ is None:
            return False
        отмечен = self._entries.get(ключ)
        if отмечен is None:
            return False
        текущее = now if now is not None else time.monotonic()
        if текущее - отмечен > self._ttl:
            del self._entries[ключ]
            return False
        return True

    def clear(self) -> None:
        self._entries.clear()
