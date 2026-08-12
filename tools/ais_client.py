"""Имитатор АИС «СВХ» — тестовый клиент совместимого API v1.

Решение от 07.08.2026 (docs/uniserver-kyzylkia.md §3): реальную АИС на
этапе разработки и пилота не трогаем; проверки — этим клиентом. Он шлёт
запрос в точном формате контракта (docs/contracts/ais-api-v1.md) и
печатает запрос и ответ.

Примеры:
    uv run python -m tools.ais_client --vehicle 01KG777AAA
    uv run python -m tools.ais_client --operation taring --vehicle 01KG777AAA
    uv run python -m tools.ais_client --url http://vesy.gti.kg/api/v1/weigh ...

Только stdlib — клиент можно запускать где угодно без зависимостей.
"""

import argparse
import json
import sys
import urllib.error
import urllib.request


def main() -> None:
    parser = argparse.ArgumentParser(description="Имитатор запроса АИС «СВХ» (API v1)")
    parser.add_argument("--url", default="http://127.0.0.1:8080/api/v1/weigh")
    parser.add_argument("--operation", choices=["weighing", "taring"], default="weighing")
    parser.add_argument("--vehicle", default=None, help="номер ТС (для тары/нетто)")
    parser.add_argument("--trailer", default=None, help="номер прицепа")
    # legacy-поля маршрутизации — значения Кызыл-Кыи по умолчанию.
    # autoscale — номер весов НА объекте (подтверждено Игорем 12.08.2026:
    # для одиночных весов АИС шлёт 1; «2» из ранней справки был ошибкой)
    parser.add_argument("--ip", default="192.168.158.20", help="legacy ip_address")
    parser.add_argument("--port", type=int, default=8087, help="legacy port")
    parser.add_argument("--autoscale", type=int, default=1, help="номер весов на объекте")
    parser.add_argument("--username", default="admin")
    parser.add_argument("--password", default="admin")
    parser.add_argument("--timeout", type=float, default=150.0, help="тайм-аут HTTP, с")
    args = parser.parse_args()

    payload: dict[str, object] = {
        "ip_address": args.ip,
        "port": args.port,
        "username": args.username,
        "password": args.password,
        "autoscale": args.autoscale,
        "operation": args.operation,
    }
    if args.vehicle:
        payload["vehicle_number"] = args.vehicle
    if args.trailer:
        payload["trailer_number"] = args.trailer

    shown = dict(payload)
    shown["password"] = "***"  # пароль в консоль не печатаем
    print("→ POST", args.url)
    print(json.dumps(shown, ensure_ascii=False, indent=2))

    request = urllib.request.Request(
        args.url,
        data=json.dumps(payload).encode(),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=args.timeout) as response:
            body = response.read().decode()
            status = response.status
    except urllib.error.HTTPError as exc:
        body = exc.read().decode()
        status = exc.code
    except OSError as exc:
        print(f"✗ центр недоступен: {exc}", file=sys.stderr)
        sys.exit(2)

    print(f"← HTTP {status}")
    try:
        parsed = json.loads(body)
        print(json.dumps(parsed, ensure_ascii=False, indent=2))
        if parsed.get("code") != "OK":
            sys.exit(1)
    except json.JSONDecodeError:
        print(body)
        sys.exit(1)


if __name__ == "__main__":
    main()
