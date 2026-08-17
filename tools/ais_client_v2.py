"""Имитатор АИС «СВХ» для нативного контракта v2 (docs/contracts/ais-api-v2.md).

Шлёт запросы в точном формате контракта и печатает запрос и ответ —
проверки центра без реальной АИС и образец для разработчиков АИС.

Примеры (токен — из --token или переменной окружения AIS_TOKEN):
    uv run python -m tools.ais_client_v2 weigh --ais-ref WEI000094176 --ais-object 0014 \\
        --vehicle 01KG777AAA --trailer 01KG500AB --operator "Акимов Н. Б."
    uv run python -m tools.ais_client_v2 weigh --taring --ais-ref TAR000012206 --ais-object 0014 \\
        --vehicle 01KG777AAA --trailer 01KG500AB --operator "Акимов Н. Б."
    uv run python -m tools.ais_client_v2 get 0d9b4d3e-8c0f-4b41-9f4c-2a6f1e3b7c55
    uv run python -m tools.ais_client_v2 find --ais-ref WEI000094176
    uv run python -m tools.ais_client_v2 list --from 2026-08-14T00:00:00+06:00 --unlinked
    uv run python -m tools.ais_client_v2 link 0d9b4d3e-… --ais-ref WEI000094200

Только stdlib — клиент можно запускать где угодно без зависимостей.
Коды выхода: 0 — исход OK (или запрос выполнен), 1 — отказ/ошибка ответа,
2 — центр недоступен.
"""

import argparse
import json
import os
import sys
import urllib.error
import urllib.parse
import urllib.request
from typing import Any

DEFAULT_BASE = "http://127.0.0.1:8080"


def _request(
    method: str, url: str, token: str, *, payload: dict[str, Any] | None, timeout: float
) -> tuple[int, str]:
    data = json.dumps(payload).encode() if payload is not None else None
    headers = {"Authorization": f"Bearer {token}"}
    if data is not None:
        headers["Content-Type"] = "application/json"
    request = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            return int(response.status), response.read().decode()
    except urllib.error.HTTPError as exc:
        return int(exc.code), exc.read().decode()
    except OSError as exc:
        print(f"✗ центр недоступен: {exc}", file=sys.stderr)
        sys.exit(2)


def _show(method: str, url: str, payload: dict[str, Any] | None, status: int, body: str) -> Any:
    print("→", method, url)
    if payload is not None:
        print(json.dumps(payload, ensure_ascii=False, indent=2))
    print(f"← HTTP {status}")
    try:
        parsed = json.loads(body)
    except json.JSONDecodeError:
        print(body)
        return None
    print(json.dumps(parsed, ensure_ascii=False, indent=2))
    return parsed


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Имитатор АИС «СВХ» — нативный API v2")
    parser.add_argument("--base", default=DEFAULT_BASE, help="базовый адрес центра")
    parser.add_argument(
        "--token",
        default=os.environ.get("AIS_TOKEN", ""),
        help="сервисный токен (или env AIS_TOKEN)",
    )
    parser.add_argument(
        "--timeout", type=float, default=150.0, help="тайм-аут HTTP, с (команда ждёт весы)"
    )
    sub = parser.add_subparsers(dest="command", required=True)

    weigh = sub.add_parser("weigh", help="команда взвешивания/тарирования")
    weigh.add_argument("--ais-ref", required=True, help="номер документа АИС: WEI… или TAR…")
    weigh.add_argument("--ais-object", required=True, help="спец. идентификатор СВХ (0014)")
    weigh.add_argument("--scale-no", type=int, default=1, help="номер весов на объекте")
    weigh.add_argument("--taring", action="store_true", help="тарирование вместо взвешивания")
    weigh.add_argument("--vehicle", required=True, help="номер головы")
    weigh.add_argument("--trailer", default=None, help="номер прицепа")
    weigh.add_argument("--operator", required=True, help="ФИО оператора АИС")

    get = sub.add_parser("get", help="документ операции по id (uuid)")
    get.add_argument("weighing_id")

    find = sub.add_parser("find", help="документ по номеру АИС")
    find.add_argument("--ais-ref", required=True)

    listing = sub.add_parser("list", help="список за период")
    listing.add_argument("--from", dest="moment_from", default=None, help="ISO 8601 с поясом")
    listing.add_argument("--to", dest="moment_to", default=None)
    listing.add_argument("--ais-object", default=None)
    listing.add_argument("--scale-no", type=int, default=None)
    listing.add_argument("--operation", choices=["weighing", "taring"], default=None)
    listing.add_argument("--source", choices=["ais", "local_offline"], default=None)
    listing.add_argument("--unlinked", action="store_true", help="только без номера АИС")
    listing.add_argument("--page", type=int, default=1)
    listing.add_argument("--per-page", type=int, default=50)

    link = sub.add_parser("link", help="сообщить номер документа АИС офлайн-операции")
    link.add_argument("weighing_id")
    link.add_argument("--ais-ref", required=True)

    args = parser.parse_args(argv)
    if not args.token:
        parser.error("нужен сервисный токен: --token или переменная окружения AIS_TOKEN")
    base = args.base.rstrip("/")

    if args.command == "weigh":
        payload: dict[str, Any] = {
            "ais_ref": args.ais_ref,
            "ais_object": args.ais_object,
            "scale_no": args.scale_no,
            "operation": "taring" if args.taring else "weighing",
            "vehicle_number": args.vehicle,
            "operator": args.operator,
        }
        if args.trailer:
            payload["trailer_number"] = args.trailer
        url = f"{base}/api/v2/weighings"
        status, body = _request("POST", url, args.token, payload=payload, timeout=args.timeout)
        parsed = _show("POST", url, payload, status, body)
        ok = status == 200 and isinstance(parsed, dict) and parsed.get("code") == "OK"
        sys.exit(0 if ok else 1)

    if args.command == "get":
        url = f"{base}/api/v2/weighings/{args.weighing_id}"
    elif args.command == "find":
        url = f"{base}/api/v2/weighings?" + urllib.parse.urlencode({"ais_ref": args.ais_ref})
    elif args.command == "list":
        params = {
            "from": args.moment_from,
            "to": args.moment_to,
            "ais_object": args.ais_object,
            "scale_no": args.scale_no,
            "operation": args.operation,
            "source": args.source,
            "unlinked": "true" if args.unlinked else None,
            "page": args.page,
            "per_page": args.per_page,
        }
        query = urllib.parse.urlencode({k: v for k, v in params.items() if v is not None})
        url = f"{base}/api/v2/weighings?{query}"
    else:  # link
        url = f"{base}/api/v2/weighings/{args.weighing_id}/ais_ref"
        payload = {"ais_ref": args.ais_ref}
        status, body = _request("POST", url, args.token, payload=payload, timeout=args.timeout)
        _show("POST", url, payload, status, body)
        sys.exit(0 if status == 200 else 1)

    status, body = _request("GET", url, args.token, payload=None, timeout=args.timeout)
    _show("GET", url, None, status, body)
    sys.exit(0 if status == 200 else 1)


if __name__ == "__main__":
    main()
