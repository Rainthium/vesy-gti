"""Драйвер xk3190 (0.4.27): кадр VESAR, но цифры в целых кг и дискрета 20.

Проверяется: разбор кадра с делителем 1, квантование к 20 кг, умолчания
класса против vesar, переопределение из конфига и сквозной проход
кадра Кокчо-Коза через цикл чтения драйвера.
"""

import threading

import pytest
from pydantic import ValidationError

from agent.config import ScaleSection
from agent.drivers import create_driver
from agent.drivers.vesar import PacketAssembler, VesarDriver, parse_packet, quantize
from agent.drivers.xk3190 import Xk3190Driver
from shared.enums import ScaleStatus

# буфер «Менеджера сервера весов АВТО» на Кокчо-Козе 03.09.2026 (пустые весы)
KOKCHO_EMPTY = bytes.fromhex("02 2B 30 30 30 30 30 30 30 31 42 03")


def frame(sign: bytes, digits: str) -> bytes:
    """Собрать кадр с верной контрольной суммой (XOR знака и цифр)."""
    body = sign + digits.encode("ascii")
    checksum = 0
    for byte in body:
        checksum ^= byte
    return b"\x02" + body + f"{checksum:02X}".encode("ascii") + b"\x03"


class TestParseWithDivisor:
    def test_kokcho_buffer_is_zero_and_checksum_matches(self) -> None:
        assert frame(b"+", "0000000") == KOKCHO_EMPTY
        packet = parse_packet(KOKCHO_EMPTY, divisor=1.0)
        assert packet is not None and packet.raw_weight_kg == 0.0

    def test_digits_are_whole_kilograms_with_divisor_one(self) -> None:
        packet = parse_packet(frame(b"+", "0037000"), divisor=1.0)
        assert packet is not None and packet.raw_weight_kg == 37000.0

    def test_default_divisor_stays_tenths_for_vesar(self) -> None:
        packet = parse_packet(frame(b"+", "0037000"))
        assert packet is not None and packet.raw_weight_kg == 3700.0

    def test_negative_sign_keeps_divisor(self) -> None:
        packet = parse_packet(frame(b"-", "0000080"), divisor=1.0)
        assert packet is not None and packet.raw_weight_kg == -80.0

    def test_assembler_passes_divisor_through(self) -> None:
        assembler = PacketAssembler(divisor=1.0)
        packets = assembler.feed(b"\xff" + frame(b"+", "0000080") + KOKCHO_EMPTY)
        assert [p.raw_weight_kg for p in packets] == [80.0, 0.0]


class TestQuantizeDiscrete20:
    @pytest.mark.parametrize(
        ("raw", "expected"),
        [(37010.0, 37000.0), (37000.0, 37000.0), (19.9, 0.0), (20.0, 20.0), (-85.0, -80.0)],
    )
    def test_truncates_to_twenty(self, raw: float, expected: float) -> None:
        assert quantize(raw, 20.0) == expected

    def test_default_discrete_is_ten_for_vesar(self) -> None:
        assert quantize(85.0) == 80.0


class TestDefaultsAndRegistry:
    def test_xk3190_defaults(self) -> None:
        driver = create_driver("xk3190", "loop://", baudrate=1200)
        assert isinstance(driver, Xk3190Driver)
        assert driver.weight_divisor == 1.0
        assert driver.discrete_kg == 20.0
        assert driver.baudrate == 1200

    def test_vesar_defaults_unchanged(self) -> None:
        driver = create_driver("vesar", "loop://", baudrate=9600)
        assert type(driver) is VesarDriver
        assert driver.weight_divisor == 10.0
        assert driver.discrete_kg == 10.0

    def test_config_overrides_class_defaults(self) -> None:
        driver = create_driver(
            "xk3190", "loop://", baudrate=9600, weight_divisor=10, discrete_kg=10
        )
        assert isinstance(driver, Xk3190Driver)
        assert driver.weight_divisor == 10.0
        assert driver.discrete_kg == 10.0

    def test_cas22_rejects_frame_options(self) -> None:
        with pytest.raises(ValueError, match="cas22"):
            create_driver("cas22", "loop://", baudrate=9600, discrete_kg=20)


class TestScaleSection:
    def test_accepts_xk3190_with_options(self) -> None:
        section = ScaleSection(driver="xk3190", port="COM7", baudrate=1200, discrete_kg=20)
        assert section.driver == "xk3190"
        assert section.weight_divisor is None
        assert section.discrete_kg == 20.0

    def test_options_must_be_positive(self) -> None:
        with pytest.raises(ValidationError):
            ScaleSection(driver="xk3190", port="COM7", discrete_kg=0)

    def test_unknown_driver_rejected(self) -> None:
        with pytest.raises(ValidationError):
            ScaleSection(driver="xk3191", port="COM7")  # type: ignore[arg-type]


class _FakePort:
    """Порт, отдающий кадры по одному и гасящий цикл, когда они кончились."""

    def __init__(self, frames: list[bytes], stop_event: threading.Event) -> None:
        self._frames = list(frames)
        self._stop_event = stop_event

    def read(self, _size: int) -> bytes:
        if self._frames:
            return self._frames.pop(0)
        self._stop_event.set()
        return b""


def test_read_loop_reports_whole_kilograms_quantized_to_twenty() -> None:
    driver = Xk3190Driver("loop://", baudrate=1200)
    stop_event = threading.Event()
    port = _FakePort([KOKCHO_EMPTY, frame(b"+", "0037010")], stop_event)
    driver._read_loop(port, stop_event)  # type: ignore[arg-type]
    state = driver.state
    assert state.status is ScaleStatus.OK
    assert state.weight_kg == 37000.0
