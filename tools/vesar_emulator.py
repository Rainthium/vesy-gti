"""Эмулятор весового индикатора с протоколом «VESAR» (СВХ «Кара-Суу»).

Назначение — разработка и тесты драйвера без железа (правило проекта:
к реальным весам не подключаемся). Генерирует непрерывный поток
12-байтовых пакетов, как настоящий индикатор (docs/protocols/vesar.md,
живые дампы 20.08.2026), и воспроизводит нештатные режимы: качание веса,
обрыв потока, мусор, битую контрольную сумму, отрицательный вес.

Два способа использования (как у tools/cas22_emulator.py):

1. Библиотекой в тестах — `PacketBuilder` и генераторы сценариев::

       builder = PacketBuilder()
       stream = b"".join(step.payload for step in full_cycle(builder))

2. TCP-сервером для отладки драйвера вручную::

       uv run python -m tools.vesar_emulator --port 4001 --scenario demo

   Драйвер подключается через pyserial-URL: ``socket://localhost:4001``.

Формат пакета (12 байт): STX 0x02, знак '+'/'-', масса 7 цифр ASCII
(младший разряд — десятые кг), контрольная сумма XOR байтов [1..8]
двумя ASCII-hex цифрами, ETX 0x03. Флага стабильности в пакете НЕТ —
драйвер вычисляет её программно по амплитуде за окно успокоения,
поэтому «нестабильность» здесь — это колебания значений, а не флаг.
"""

import argparse
import asyncio
import random
from collections.abc import Callable, Iterable, Iterator
from dataclasses import dataclass

PACKET_LEN = 12
STX = b"\x02"
ETX = b"\x03"

# реальный индикатор шлёт ~12,5 пакетов/с (дамп 20.08.2026)
DEFAULT_RATE = 12.5
# внутренний шаг показаний индикатора: переходные значения живого дампа
# шли с шагом 5,0 кг (при дискрете табло 10 кг)
INTERNAL_STEP_KG = 5.0


class PacketBuilder:
    """Собирает 12-байтовые пакеты в формате индикатора «VESAR»."""

    def __init__(self, step_kg: float = INTERNAL_STEP_KG) -> None:
        self.step_kg = step_kg

    def round_weight(self, weight_kg: float) -> float:
        """Привести вес к внутреннему шагу показаний индикатора."""
        return round(weight_kg / self.step_kg) * self.step_kg

    def build(self, weight_kg: float, *, bad_checksum: bool = False) -> bytes:
        """Собрать один пакет; ``bad_checksum`` — испортить КС (тест отбраковки)."""
        value = self.round_weight(weight_kg)
        raw = round(abs(value) * 10)  # младший разряд — десятые кг
        digits = str(raw).rjust(7, "0").encode("ascii")
        if len(digits) != 7:
            raise ValueError(f"масса не помещается в 7 цифр: {weight_kg!r}")
        sign = b"-" if value < 0 else b"+"
        checksum = 0
        for byte in sign + digits:
            checksum ^= byte
        if bad_checksum:
            checksum ^= 0xFF
        packet = STX + sign + digits + f"{checksum:02X}".encode("ascii") + ETX
        assert len(packet) == PACKET_LEN
        return packet


@dataclass(frozen=True)
class Step:
    """Шаг сценария: что отправить и сколько ждать. Пустой payload — тишина."""

    payload: bytes
    delay_s: float


def _packets(payloads: Iterable[bytes], rate: float) -> Iterator[Step]:
    """Обернуть последовательность пакетов в шаги с равномерной задержкой."""
    delay = 1.0 / rate
    for payload in payloads:
        yield Step(payload, delay)


# --- базовые сценарии ---


def empty_scale(
    builder: PacketBuilder, duration_s: float = 3.0, rate: float = DEFAULT_RATE
) -> Iterator[Step]:
    """Пустые весы: ровный ноль (драйвер сочтёт стабильным сам)."""
    count = max(1, int(duration_s * rate))
    return _packets((builder.build(0) for _ in range(count)), rate)


def drive_on(
    builder: PacketBuilder,
    target_kg: float,
    duration_s: float = 3.0,
    rate: float = DEFAULT_RATE,
) -> Iterator[Step]:
    """Заезд АТС: вес растёт от нуля до цели (амплитуда даёт нестабильность)."""
    count = max(2, int(duration_s * rate))
    return _packets(
        (builder.build(target_kg * i / (count - 1)) for i in range(count)),
        rate,
    )


def stabilizing(
    builder: PacketBuilder,
    around_kg: float,
    duration_s: float = 2.0,
    rate: float = DEFAULT_RATE,
    amplitude_kg: float = 60.0,
    seed: int = 0,
) -> Iterator[Step]:
    """Затухающие колебания вокруг цели (АТС качается на платформе).

    Амплитуда по умолчанию заведомо больше порога успокоения драйвера
    (3 дискреты = 30 кг) — начало фазы гарантированно нестабильно.
    """
    rng = random.Random(seed)
    count = max(2, int(duration_s * rate))

    def payloads() -> Iterator[bytes]:
        for i in range(count):
            damping = 1.0 - i / count
            jitter = rng.uniform(-amplitude_kg, amplitude_kg) * damping
            yield builder.build(around_kg + jitter)

    return _packets(payloads(), rate)


def stable_weight(
    builder: PacketBuilder,
    weight_kg: float,
    duration_s: float = 3.0,
    rate: float = DEFAULT_RATE,
) -> Iterator[Step]:
    """Ровный вес — через окно успокоения драйвер сочтёт его стабильным."""
    count = max(1, int(duration_s * rate))
    return _packets((builder.build(weight_kg) for _ in range(count)), rate)


def drive_off(
    builder: PacketBuilder,
    from_kg: float,
    duration_s: float = 2.0,
    rate: float = DEFAULT_RATE,
) -> Iterator[Step]:
    """Съезд АТС: вес падает до нуля."""
    count = max(2, int(duration_s * rate))
    return _packets(
        (builder.build(from_kg * (1 - i / (count - 1))) for i in range(count)),
        rate,
    )


def stream_break(duration_s: float = 4.0) -> Iterator[Step]:
    """Обрыв потока: тишина дольше таймаута драйвера (3 с)."""
    yield Step(b"", duration_s)


def garbage(n_bytes: int = 40, seed: int = 0, rate: float = DEFAULT_RATE) -> Iterator[Step]:
    """Мусор в потоке (наводки, обрывки) — тест ресинхронизации.

    Внутрь вложены ложный STX и обрывок настоящего пакета — разбор
    обязан отбросить их по ETX и контрольной сумме.
    """
    rng = random.Random(seed)
    junk = bytes(rng.randrange(256) for _ in range(n_bytes))
    junk = junk[: n_bytes // 2] + STX + b"+00" + junk[n_bytes // 2 :]
    chunk = 16
    return _packets((junk[i : i + chunk] for i in range(0, len(junk), chunk)), rate)


def bad_checksum(
    builder: PacketBuilder,
    weight_kg: float,
    duration_s: float = 2.0,
    rate: float = DEFAULT_RATE,
) -> Iterator[Step]:
    """Пакеты с битой контрольной суммой — драйвер обязан их отбросить."""
    count = max(1, int(duration_s * rate))
    return _packets((builder.build(weight_kg, bad_checksum=True) for _ in range(count)), rate)


def negative_weight(
    builder: PacketBuilder,
    weight_kg: float = -30.0,
    duration_s: float = 2.0,
    rate: float = DEFAULT_RATE,
) -> Iterator[Step]:
    """Отрицательный вес (весы «ушли в минус» после съезда без обнуления)."""
    count = max(1, int(duration_s * rate))
    return _packets((builder.build(weight_kg) for _ in range(count)), rate)


# --- составные сценарии ---


def full_cycle(
    builder: PacketBuilder,
    target_kg: float = 12500.0,
    rate: float = DEFAULT_RATE,
    seed: int = 0,
) -> Iterator[Step]:
    """Полный цикл: пусто → заезд → стабилизация → ровный вес → съезд."""
    yield from empty_scale(builder, 2.0, rate)
    yield from drive_on(builder, target_kg, 3.0, rate)
    yield from stabilizing(builder, target_kg, 2.0, rate, seed=seed)
    yield from stable_weight(builder, target_kg, 3.0, rate)
    yield from drive_off(builder, target_kg, 2.0, rate)
    yield from empty_scale(builder, 2.0, rate)


def demo(
    builder: PacketBuilder,
    target_kg: float = 12500.0,
    rate: float = DEFAULT_RATE,
    seed: int = 0,
) -> Iterator[Step]:
    """Все режимы подряд: цикл, мусор, битая КС, обрыв, минус."""
    yield from full_cycle(builder, target_kg, rate, seed)
    yield from garbage(seed=seed, rate=rate)
    yield from full_cycle(builder, target_kg * 2, rate, seed + 1)
    yield from bad_checksum(builder, target_kg, rate=rate)
    yield from stream_break()
    yield from negative_weight(builder, rate=rate)
    yield from empty_scale(builder, 2.0, rate)


ScenarioFactory = Callable[[], Iterator[Step]]


def make_scenario(
    name: str,
    builder: PacketBuilder,
    target_kg: float,
    rate: float,
    seed: int = 0,
) -> ScenarioFactory:
    """Фабрика сценария по имени (для CLI и тестов)."""
    scenarios: dict[str, ScenarioFactory] = {
        "empty": lambda: empty_scale(builder, 3.0, rate),
        "cycle": lambda: full_cycle(builder, target_kg, rate, seed),
        "unstable": lambda: stabilizing(builder, target_kg, 10.0, rate, seed=seed),
        "dropout": lambda: iter((*stable_weight(builder, target_kg, 2.0, rate), *stream_break())),
        "garbage": lambda: iter(
            (*garbage(seed=seed, rate=rate), *stable_weight(builder, target_kg, 2.0, rate))
        ),
        "badsum": lambda: bad_checksum(builder, target_kg, 3.0, rate),
        "negative": lambda: negative_weight(builder, -30.0, 3.0, rate),
        "demo": lambda: demo(builder, target_kg, rate, seed),
    }
    if name not in scenarios:
        raise ValueError(f"неизвестный сценарий: {name} (есть: {', '.join(sorted(scenarios))})")
    return scenarios[name]


# --- TCP-сервер (устройство — как в tools/cas22_emulator.py) ---


async def serve(
    host: str,
    port: int,
    scenario_factory: ScenarioFactory,
    *,
    loop_forever: bool = True,
) -> None:
    """Отдавать поток эмулятора каждому подключившемуся клиенту."""
    stopping = asyncio.Event()

    async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        peer = writer.get_extra_info("peername")
        print(f"Клиент подключился: {peer}")
        try:
            while not stopping.is_set():
                for step in scenario_factory():
                    if stopping.is_set():
                        return
                    if step.payload:
                        writer.write(step.payload)
                        await writer.drain()
                    await asyncio.sleep(step.delay_s)
                if not loop_forever:
                    break
        except (ConnectionResetError, BrokenPipeError):
            pass
        finally:
            print(f"Клиент отключился: {peer}")
            writer.close()

    server = await asyncio.start_server(handle, host, port)
    print(f"Эмулятор VESAR слушает {host}:{port} (pyserial: socket://{host}:{port})")
    try:
        await server.serve_forever()
    finally:
        # порядок важен: сперва флаг (обработчики выйдут за один шаг),
        # затем wait_closed — он ждёт завершения обработчиков
        stopping.set()
        server.close()
        await server.wait_closed()


def main() -> None:
    parser = argparse.ArgumentParser(description="Эмулятор весового индикатора VESAR")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=4001)
    parser.add_argument("--rate", type=float, default=DEFAULT_RATE, help="пакетов в секунду")
    parser.add_argument("--weight", type=float, default=12500.0, help="целевой вес, кг")
    parser.add_argument("--seed", type=int, default=0, help="зерно генератора колебаний/мусора")
    parser.add_argument(
        "--scenario",
        default="demo",
        choices=[
            "empty",
            "cycle",
            "unstable",
            "dropout",
            "garbage",
            "badsum",
            "negative",
            "demo",
        ],
    )
    args = parser.parse_args()

    builder = PacketBuilder()
    factory = make_scenario(args.scenario, builder, args.weight, args.rate, args.seed)
    try:
        asyncio.run(serve(args.host, args.port, factory))
    except KeyboardInterrupt:
        print("\nОстановлен.")


if __name__ == "__main__":
    main()
