"""Эмулятор весового индикатора CAS CI-серии, протокол «CAS 22 byte».

Назначение — разработка и тесты драйвера без железа (правило проекта:
к реальным весам не подключаемся). Генерирует непрерывный поток
22-байтовых пакетов, как настоящий индикатор (docs/protocols/cas22.md),
и воспроизводит нештатные режимы: нестабильность, обрыв потока, мусор
в потоке, отрицательный вес, перегруз.

Два способа использования:

1. Библиотекой в тестах — `PacketBuilder` и генераторы сценариев дают
   детерминированные последовательности шагов без ввода-вывода::

       builder = PacketBuilder()
       stream = b"".join(step.payload for step in full_cycle(builder))

2. TCP-сервером для отладки драйвера вручную::

       uv run python -m tools.cas22_emulator --port 4001 --scenario demo

   Драйвер подключается через pyserial-URL: ``socket://localhost:4001``.

Формат пакета (22 байта): флаг ST/US/OL, ',', режим GS/NT, ',', знак,
',', масса 8 символов ASCII, единица «kg », CR LF. Без контрольной суммы.
"""

import argparse
import asyncio
import random
from collections.abc import Callable, Iterable, Iterator
from dataclasses import dataclass

# Флаги состояния (байты 0-1)
FLAG_STABLE = b"ST"
FLAG_UNSTABLE = b"US"
FLAG_OVERLOAD = b"OL"

# Режимы (байты 3-4)
MODE_GROSS = b"GS"
MODE_NET = b"NT"

PACKET_LEN = 22
DEFAULT_RATE = 8.0  # пакетов в секунду (реальный индикатор шлёт чаще 10/с)
DEFAULT_DISCRET = 10  # дискретность весов, кг (значение с Кызыл-Кыи)


class PacketBuilder:
    """Собирает 22-байтовые пакеты в формате индикатора CAS CI-серии."""

    def __init__(self, discret: int = DEFAULT_DISCRET) -> None:
        self.discret = discret

    def round_weight(self, weight_kg: float) -> int:
        """Привести вес к дискретности весов (как делает сам индикатор)."""
        return round(weight_kg / self.discret) * self.discret

    def build(
        self,
        weight_kg: float,
        *,
        stable: bool = True,
        overload: bool = False,
        mode: bytes = MODE_GROSS,
        space_in_mass: bool = False,
    ) -> bytes:
        """Собрать один пакет.

        ``space_in_mass`` — воспроизвести причуду реального индикатора:
        пробел внутри числа (из-за него падал первый прототип разбора).
        """
        flag = FLAG_OVERLOAD if overload else FLAG_STABLE if stable else FLAG_UNSTABLE

        value = self.round_weight(weight_kg)
        sign = b" -" if value < 0 else b"  "
        digits = str(abs(value))
        if space_in_mass and len(digits) > 3:
            # пробел-разделитель тысяч внутри поля массы: "   1 460"
            digits = f"{digits[:-3]} {digits[-3:]}"
        mass = digits.rjust(8).encode("ascii")
        if len(mass) != 8:  # вес шире 8 символов физически невозможен, но проверим
            raise ValueError(f"поле массы не помещается в 8 байт: {digits!r}")

        packet = flag + b"," + mode + b"," + sign + b"," + mass + b"kg " + b"\r\n"
        assert len(packet) == PACKET_LEN
        return packet


@dataclass(frozen=True)
class Step:
    """Шаг сценария: что отправить в поток и сколько ждать после отправки.

    Пустой ``payload`` — тишина (обрыв потока на ``delay_s`` секунд).
    """

    payload: bytes
    delay_s: float


def _packets(payloads: Iterable[bytes], rate: float) -> Iterator[Step]:
    """Обернуть последовательность пакетов в шаги с равномерной задержкой."""
    delay = 1.0 / rate
    for payload in payloads:
        yield Step(payload, delay)


# --- базовые сценарии (docs/protocols/cas22.md, раздел «Эмулятор») ---


def empty_scale(
    builder: PacketBuilder, duration_s: float = 3.0, rate: float = DEFAULT_RATE
) -> Iterator[Step]:
    """Пустые весы: стабильный ноль."""
    count = max(1, int(duration_s * rate))
    return _packets((builder.build(0, stable=True) for _ in range(count)), rate)


def drive_on(
    builder: PacketBuilder,
    target_kg: float,
    duration_s: float = 3.0,
    rate: float = DEFAULT_RATE,
) -> Iterator[Step]:
    """Заезд АТС: вес растёт от нуля до цели, флаг нестабильности."""
    count = max(2, int(duration_s * rate))
    return _packets(
        (builder.build(target_kg * i / (count - 1), stable=False) for i in range(count)),
        rate,
    )


def stabilizing(
    builder: PacketBuilder,
    around_kg: float,
    duration_s: float = 2.0,
    rate: float = DEFAULT_RATE,
    amplitude_kg: float = 30.0,
    seed: int = 0,
) -> Iterator[Step]:
    """Затухающие колебания вокруг цели (АТС качается на платформе), US."""
    rng = random.Random(seed)
    count = max(2, int(duration_s * rate))

    def payloads() -> Iterator[bytes]:
        for i in range(count):
            damping = 1.0 - i / count  # амплитуда затухает к концу
            jitter = rng.uniform(-amplitude_kg, amplitude_kg) * damping
            yield builder.build(around_kg + jitter, stable=False)

    return _packets(payloads(), rate)


def stable_weight(
    builder: PacketBuilder,
    weight_kg: float,
    duration_s: float = 3.0,
    rate: float = DEFAULT_RATE,
) -> Iterator[Step]:
    """Стабильный вес (ST) — фаза, в которой драйвер фиксирует взвешивание."""
    count = max(1, int(duration_s * rate))
    return _packets((builder.build(weight_kg, stable=True) for _ in range(count)), rate)


def drive_off(
    builder: PacketBuilder,
    from_kg: float,
    duration_s: float = 2.0,
    rate: float = DEFAULT_RATE,
) -> Iterator[Step]:
    """Съезд АТС: вес падает до нуля, флаг нестабильности."""
    count = max(2, int(duration_s * rate))
    return _packets(
        (builder.build(from_kg * (1 - i / (count - 1)), stable=False) for i in range(count)),
        rate,
    )


def stream_break(duration_s: float = 4.0) -> Iterator[Step]:
    """Обрыв потока: тишина дольше таймаута драйвера (3 с)."""
    yield Step(b"", duration_s)


def garbage(n_bytes: int = 40, seed: int = 0, rate: float = DEFAULT_RATE) -> Iterator[Step]:
    """Мусор в потоке (наводки, обрывки пакетов) — тест ресинхронизации.

    Мусор нарезан кусками и может содержать CR LF и обрывки настоящих
    пакетов — драйвер обязан отбросить его по проверке байтов синхронизации.
    """
    rng = random.Random(seed)
    junk = bytes(rng.randrange(256) for _ in range(n_bytes))
    # вставим ложный конец пакета, чтобы проверка не свелась к поиску CRLF
    junk = junk[: n_bytes // 2] + b"\r\n" + junk[n_bytes // 2 :]
    chunk = 16
    return _packets((junk[i : i + chunk] for i in range(0, len(junk), chunk)), rate)


def overload(
    builder: PacketBuilder, duration_s: float = 2.0, rate: float = DEFAULT_RATE
) -> Iterator[Step]:
    """Перегруз (OL): вес использовать нельзя."""
    count = max(1, int(duration_s * rate))
    return _packets((builder.build(99990, overload=True) for _ in range(count)), rate)


def negative_weight(
    builder: PacketBuilder,
    weight_kg: float = -30.0,
    duration_s: float = 2.0,
    rate: float = DEFAULT_RATE,
) -> Iterator[Step]:
    """Отрицательный вес (весы «ушли в минус» после съезда без обнуления)."""
    count = max(1, int(duration_s * rate))
    return _packets((builder.build(weight_kg, stable=True) for _ in range(count)), rate)


# --- составные сценарии ---


def full_cycle(
    builder: PacketBuilder,
    target_kg: float = 12500.0,
    rate: float = DEFAULT_RATE,
    seed: int = 0,
) -> Iterator[Step]:
    """Полный цикл взвешивания: пусто → заезд → стабилизация → фиксация → съезд."""
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
    """Демонстрация всех режимов подряд: цикл, мусор, обрыв, минус, перегруз."""
    yield from full_cycle(builder, target_kg, rate, seed)
    yield from garbage(seed=seed, rate=rate)
    yield from full_cycle(builder, target_kg * 2, rate, seed + 1)
    yield from stream_break()
    yield from negative_weight(builder, rate=rate)
    yield from overload(builder, rate=rate)
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
        "overload": lambda: overload(builder, 3.0, rate),
        "negative": lambda: negative_weight(builder, -30.0, 3.0, rate),
        "demo": lambda: demo(builder, target_kg, rate, seed),
    }
    if name not in scenarios:
        raise ValueError(f"неизвестный сценарий: {name} (есть: {', '.join(sorted(scenarios))})")
    return scenarios[name]


# --- TCP-сервер ---


async def serve(
    host: str,
    port: int,
    scenario_factory: ScenarioFactory,
    *,
    loop_forever: bool = True,
) -> None:
    """Отдавать поток эмулятора каждому подключившемуся клиенту.

    Сценарий проигрывается с начала для каждого клиента; по окончании —
    повторяется (непрерывная передача, как у настоящего индикатора).
    """

    async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        peer = writer.get_extra_info("peername")
        print(f"Клиент подключился: {peer}")
        try:
            while True:
                for step in scenario_factory():
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
    print(f"Эмулятор CAS 22 byte слушает {host}:{port} (pyserial: socket://{host}:{port})")
    async with server:
        await server.serve_forever()


def main() -> None:
    parser = argparse.ArgumentParser(description="Эмулятор весового индикатора CAS 22 byte")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=4001)
    parser.add_argument("--rate", type=float, default=DEFAULT_RATE, help="пакетов в секунду")
    parser.add_argument("--weight", type=float, default=12500.0, help="целевой вес, кг")
    parser.add_argument("--discret", type=int, default=DEFAULT_DISCRET, help="дискретность, кг")
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
            "overload",
            "negative",
            "demo",
        ],
    )
    args = parser.parse_args()

    builder = PacketBuilder(discret=args.discret)
    factory = make_scenario(args.scenario, builder, args.weight, args.rate, args.seed)
    try:
        asyncio.run(serve(args.host, args.port, factory))
    except KeyboardInterrupt:
        print("\nОстановлен.")


if __name__ == "__main__":
    main()
