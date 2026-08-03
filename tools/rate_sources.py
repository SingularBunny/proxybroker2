"""Оценить источники прокси по единственному значимому критерию — числу живых.

Зачем
-----
Список источников легко пополнять по репутации и невозможно так проверить.
Замер вклада (PB9) показал, что из 38 провайдеров живые прокси дают шесть, а
один даёт 68% пула, — и узнать это можно было только измерением.

Этот скрипт прогоняет произвольный набор URL через настоящую проверку брокера и
печатает, сколько каждый отдал, сколько из этого новых адресов и сколько в
итоге прошло судей. Отдал много и не дал ни одного живого — источник не нужен,
сколько бы адресов он ни публиковал.

    python -m tools.rate_sources --seconds 200
    python -m tools.rate_sources --url https://example.com/list.txt --proto SOCKS5

Без ``--url`` берётся встроенный список кандидатов.
"""

from __future__ import annotations

import argparse
import asyncio
import logging

from proxybroker import Broker
from proxybroker.providers import Provider

#: Кандидаты на инфраструктуре, не связанной с GitHub и proxyscrape.
#: Проверено отдельно, что отвечают и содержат пары адрес:порт.
КАНДИДАТЫ = [
    ("https://openproxylist.xyz/socks5.txt", ("SOCKS5",)),
    ("https://openproxylist.xyz/socks4.txt", ("SOCKS4",)),
    ("https://openproxylist.xyz/http.txt", ("HTTP", "CONNECT:80")),
    ("https://spys.me/proxy.txt", ("HTTP", "CONNECT:80")),
]


async def оценить(источники, секунд: int) -> None:
    провайдеры = [Provider(url=u, proto=p) for u, p in источники]
    брокер = Broker(
        timeout=8,
        max_tries=1,
        provider_timeout=40,
        grab_pause=0,
        providers=провайдеры,
        stop_broker_on_sigint=False,
    )
    задача = asyncio.create_task(
        брокер.find(types=["HTTP", "SOCKS4", "SOCKS5"], limit=0, wait=True)
    )
    await asyncio.sleep(секунд)

    # Снимаем и печатаем ДО остановки: сама остановка может подвиснуть на
    # закрытии сессий провайдеров, и тогда результат замера пропадёт вместе с
    # процессом — то есть двести секунд ожидания впустую.
    срез = брокер.pool_stats()

    print(
        f"\nкандидатов {срез['candidates']}  "
        f"проверено {срез['checked']}  прошли {срез['passed']}\n"
    )
    print(f"{'источник':44} {'отдал':>7} {'новых':>7} {'живых':>7}")
    for имя, вклад in sorted(
        срез["providers"].items(), key=lambda kv: -kv[1]["passed"]
    ):
        коротко = имя.replace("<Provider ", "").replace(">", "")[:42]
        print(
            f"{коротко:44} {вклад['yielded']:>7} "
            f"{вклад['unique']:>7} {вклад['passed']:>7}"
        )
    страны = list(срез["countries"].items())[:8]
    if страны:
        print("\nстраны живых:", ", ".join(f"{к}:{n}" for к, n in страны))

    брокер.stop()
    задача.cancel()
    try:
        await asyncio.wait_for(задача, timeout=5)
    except BaseException:
        pass


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--seconds", type=int, default=200, help="сколько наблюдать")
    parser.add_argument("--url", action="append", help="URL источника; повторяемый")
    parser.add_argument(
        "--proto",
        default="HTTP,CONNECT:80",
        help="протоколы для --url через запятую",
    )
    args = parser.parse_args()

    logging.disable(logging.INFO)
    if args.url:
        прото = tuple(p.strip() for p in args.proto.split(",") if p.strip())
        источники = [(u, прото) for u in args.url]
    else:
        источники = КАНДИДАТЫ

    asyncio.run(оценить(источники, args.seconds))


if __name__ == "__main__":
    main()
