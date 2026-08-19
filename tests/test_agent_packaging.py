"""Сборка агента (PyInstaller): то, что нельзя проверить без Windows-раннера,
закрепляем статически по spec-файлу.

Боевой урок Аламедина (19.08.2026): pyserial подгружает обработчики URL
динамически (``serial_for_url`` → ``importlib``), и замороженный exe без
явных hiddenimports не знал протокол ``socket://`` — а именно так агент
читает поток индикатора из TCP-ретрансляции UniServer, когда com0com на
Windows 11 не грузится (ошибка подписи драйвера 577).
"""

import re
from pathlib import Path

import serial

SPEC = Path(__file__).resolve().parents[1] / "packaging" / "agent" / "ves-agent.spec"

REQUIRED_HIDDEN_IMPORTS = {
    "serial.urlhandler",
    "serial.urlhandler.protocol_socket",  # socket://host:port — TCP-ретрансляция / эмулятор
    "serial.urlhandler.protocol_loop",
    "serial.urlhandler.protocol_rfc2217",
    "uvicorn.protocols.websockets.websockets_impl",
}


def _hidden_imports() -> set[str]:
    text = SPEC.read_text(encoding="utf-8")
    block = re.search(r"hiddenimports=\[(.*?)\]", text, re.S)
    assert block is not None, "в spec нет hiddenimports"
    return set(re.findall(r'"([^"]+)"', block.group(1)))


def test_spec_lists_dynamic_imports() -> None:
    missing = REQUIRED_HIDDEN_IMPORTS - _hidden_imports()
    assert not missing, f"в ves-agent.spec не перечислены динамические импорты: {sorted(missing)}"


def test_socket_url_handler_resolves() -> None:
    # тот же вызов, что в драйвере cas22 (_open_port): URL должен разбираться,
    # порт при этом не открываем
    port = serial.serial_for_url("socket://127.0.0.1:1", do_not_open=True)
    assert port.__class__.__module__ == "serial.urlhandler.protocol_socket"
