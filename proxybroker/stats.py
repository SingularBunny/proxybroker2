"""Что происходит с пулом, наружу.

Зачем
-----
Во время семнадцатичасового прогона единственным сигналом были строки в логе,
и то косвенные: 868 раз «proxy pool exhausted». Ни размера пула, ни скорости
пополнения, ни доли прошедших проверку видно не было, а `wait_for_proxies`
умеет ждать только количество. Разбор занял часы там, где хватило бы одной
строки «за проход: 40 896 кандидатов, 52 прошли, отбраковано по стране 40 800».

Что считается
-------------
Воронка от кандидата до готового прокси. Каждая ступень отвечает на свой
вопрос при разборе:

* ``candidates`` — сколько адресов дали провайдеры. Ноль означает, что
  сломались источники, а не проверка.
* ``duplicates`` — сколько отброшено как уже виденные в этом проходе.
* ``geo_rejected`` — сколько не прошло фильтр по стране. Если почти всё
  отсеивается здесь, пул пуст не потому, что прокси мёртвые, а потому что
  фильтр слишком узкий: ровно этот случай выглядел как поломка.
* ``checked`` / ``passed`` — сколько дошло до проверки и сколько её прошло.
  Доля показывает, живы ли источники: у бесплатных списков нормой считаются
  единицы процентов.
* ``from_store`` — сколько кандидатов пришло из сохранённого пула, а не от
  провайдеров.

Счётчики накопительные за время жизни брокера, кроме ``per_pass``, который
показывает последний завершённый проход — по нему видно деградацию во времени.
"""

from __future__ import annotations

import time
from collections import Counter
from typing import Any, Dict, Optional


class PoolStats:
    """Счётчики воронки: кандидат → проверка → пул.

    Устроено как обычные целые числа без блокировок: брокер однопоточный, всё
    происходит в одном цикле событий, и накладные расходы на учёт не должны
    ощущаться на пути каждого прокси.
    """

    def __init__(self) -> None:
        self.started_at = time.monotonic()
        self.candidates = 0
        self.duplicates = 0
        self.geo_rejected = 0
        self.checked = 0
        self.passed = 0
        self.from_store = 0
        self.passes = 0
        #: Страны прошедших проверку — показывает, откуда реально берутся прокси.
        self.countries: Counter = Counter()
        #: Снимок счётчиков на конец предыдущего прохода.
        self._pass_baseline: Dict[str, int] = {}
        self._last_pass: Dict[str, int] = {}

    # ------------------------------------------------------------------ #

    def note_candidate(self) -> None:
        self.candidates += 1

    def note_duplicate(self) -> None:
        self.duplicates += 1

    def note_geo_rejected(self) -> None:
        self.geo_rejected += 1

    def note_checked(self) -> None:
        self.checked += 1

    def note_passed(self, country: Optional[str] = None) -> None:
        self.passed += 1
        if country:
            self.countries[country] += 1

    def note_from_store(self, count: int) -> None:
        self.from_store += count

    def note_pass_complete(self) -> None:
        """Закрыть проход и запомнить его вклад отдельно от накопленного."""
        self.passes += 1
        текущие = self._snapshot()
        self._last_pass = {
            ключ: текущие[ключ] - self._pass_baseline.get(ключ, 0)
            for ключ in текущие
        }
        self._pass_baseline = текущие

    # ------------------------------------------------------------------ #

    def _snapshot(self) -> Dict[str, int]:
        return {
            "candidates": self.candidates,
            "duplicates": self.duplicates,
            "geo_rejected": self.geo_rejected,
            "checked": self.checked,
            "passed": self.passed,
        }

    @property
    def pass_rate(self) -> Optional[float]:
        """Доля проверенных, которые прошли, в процентах.

        ``None``, а не ноль, когда проверок ещё не было: «ноль процентов» и
        «нечего измерять» — разные состояния, и путать их при разборе дорого.
        """
        if not self.checked:
            return None
        return self.passed / self.checked * 100

    @property
    def uptime(self) -> float:
        return time.monotonic() - self.started_at

    def as_dict(self, pool_size: Optional[int] = None) -> Dict[str, Any]:
        """Машиночитаемый срез — для метрик, тестов и отладки."""
        данные: Dict[str, Any] = {
            **self._snapshot(),
            "from_store": self.from_store,
            "passes": self.passes,
            "pass_rate": self.pass_rate,
            "uptime": round(self.uptime, 1),
            "countries": dict(self.countries.most_common()),
            "last_pass": dict(self._last_pass),
        }
        if pool_size is not None:
            данные["pool_size"] = pool_size
        return данные

    def summary(self, pool_size: Optional[int] = None) -> str:
        """Одна строка для лога.

        Формат подобран так, чтобы по ней сразу читалось, на какой ступени
        воронки теряются прокси, — а не только итог.
        """
        части = [f"пул {pool_size}"] if pool_size is not None else []
        части.append(f"кандидатов {self.candidates}")
        if self.duplicates:
            части.append(f"повторов {self.duplicates}")
        if self.geo_rejected:
            части.append(f"не та страна {self.geo_rejected}")
        доля = self.pass_rate
        части.append(
            f"проверено {self.checked}, прошли {self.passed}"
            + (f" ({доля:.1f}%)" if доля is not None else "")
        )
        if self.from_store:
            части.append(f"из сохранённого пула {self.from_store}")
        if self.countries:
            топ = ", ".join(f"{к}:{n}" for к, n in self.countries.most_common(5))
            части.append(f"страны {топ}")
        части.append(f"проходов {self.passes}")
        return " | ".join(части)
