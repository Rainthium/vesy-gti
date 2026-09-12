"""Перенос действующих тарирований из выгрузки АИС «СВХ» (решение Игоря 12.09.2026).

До переключения объекта на весовую систему АИС проводила тарирования через
UniServer, и их массы живут только в АИС. Без переноса после переключения
каждой сцепке пришлось бы тарироваться заново (правило №4: тара действует
3 месяца) — на Канте это около двух тысяч сцепок. Перенос разовый, из
выгрузки «Тарирования» АИС, инструментом ``tools.import_ais_tares``;
кнопки в панели нет (решение Игоря: дальше тарирования проводятся только в
системе). Исключение из решения 07.08.2026 «единственный источник тары —
тарирования в системе» — только для переключаемых объектов, ручного ввода
тары по-прежнему нет.

Что переносится: строки не старше 3 месяцев, с номером ТС, не помеченные
«не учитывать при взвешивании», не тяжелее лимита тары и ещё не известные
центру. «Известность» решает номер документа ``TAR…``: у операций по
контракту v2 он закреплён за записью журнала; операции без номера (офлайн,
пока АИС не сообщила номер) узнаются по объекту, номеру ТС, массе и
времени. Перенесённая запись получает ``source = imported`` и номер TAR
(origin ``import``), поэтому повтор переноса её не задваивает — свежую
выгрузку можно загрузить ещё раз после переключения объекта, войдут только
новые строки.

Здесь — чистая логика: разбор файла и вердикты; БД — в ``center.db.repo``.
"""

from __future__ import annotations

import csv
import io
from collections import Counter, defaultdict
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta, timezone
from enum import StrEnum

from shared.enums import WeighingSource
from shared.tare import DEFAULT_MAX_TARE_KG, three_months_before

# колонки выгрузки «Тарирования» АИС «СВХ» (разделитель «;», UTF-8 с BOM)
COL_ENTRY = "Номер въезда"
COL_TARE_REF = "Порядковый номер тарирования"
COL_VEHICLE = "Гос.номер АТС"
COL_TRAILER = "Гос.номер прицепа"
COL_STATUS = "Статус тарирования"
COL_MASSA = "Тара"
COL_UNIT = "Единица измерения"
COL_OBJECT = "Наименование объекта"
COL_OPERATOR = "ФИО оператора"
COL_TARED_AT = "Дата и время тарирования"
COL_IGNORED = "Не учитывать при взвешивании"
REQUIRED_COLUMNS = (
    COL_TARE_REF,
    COL_VEHICLE,
    COL_TRAILER,
    COL_STATUS,
    COL_MASSA,
    COL_UNIT,
    COL_OBJECT,
    COL_TARED_AT,
)
STATUS_COMPLETED = "Завершенное взвешивание"
UNIT_KG = "кг"
# время в выгрузке — бишкекское без пояса (проверено по общим с центром
# номерам TAR 12.09.2026); смещение, а не zoneinfo — как в shared/tare.py
BISHKEK = timezone(timedelta(hours=6))
_DATE_FORMATS = ("%d.%m.%Y, %H:%M:%S", "%d.%m.%Y, %H:%M", "%d.%m.%Y %H:%M:%S", "%d.%m.%Y %H:%M")

# сопоставление операции без номера TAR с журналом центра: тот же объект и
# номер ТС, масса в пределах округления АИС, время в пределах получаса
# (часы АИС и центра расходятся на минуты)
MATCH_MASSA_KG = 20.0
MATCH_WINDOW = timedelta(minutes=30)


class TareImportError(ValueError):
    """Файл не похож на выгрузку тарирований АИС «СВХ»."""


@dataclass(frozen=True)
class AisTaring:
    """Строка выгрузки после разбора; номера нормализованы как в API v2
    (strip/upper, пустой прицеп — None)."""

    tare_ref: str  # TAR000013623
    entry_ref: str | None  # SVH000887648 — номер въезда
    vehicle_number: str  # '' — без номера ТС
    trailer_number: str | None
    massa: float  # кг
    object_name: str  # объект в терминах АИС: «Кант», «Аламедин СВХ»
    operator: str | None
    tared_at: datetime  # aware, UTC
    completed: bool  # статус «Завершенное взвешивание»
    ignored: bool  # «Не учитывать при взвешивании»

    @property
    def pair(self) -> tuple[str, str]:
        """Ключ сцепки как в реестре тар ('' — без прицепа)."""
        return self.vehicle_number, self.trailer_number or ""


@dataclass(frozen=True)
class OurTaring:
    """Состоявшееся тарирование журнала центра — для сопоставления без TAR."""

    site_code: str
    vehicle_number: str
    massa: float
    weighed_at: datetime


@dataclass(frozen=True)
class ImportTarget:
    """Куда ложится объект выгрузки: объект центра и его весы."""

    site_code: str
    scale_id: int


class Verdict(StrEnum):
    """Судьба строки выгрузки."""

    IMPORT = "import"
    TOO_OLD = "too_old"
    ALREADY_OURS = "already_ours"
    ALREADY_IMPORTED = "already_imported"
    MATCHED_OURS = "matched_ours"
    NOT_COMPLETED = "not_completed"
    IGNORED = "ignored"
    NO_VEHICLE = "no_vehicle"
    TOO_HEAVY = "too_heavy"
    UNKNOWN_OBJECT = "unknown_object"


VERDICT_LABELS: dict[Verdict, str] = {
    Verdict.IMPORT: "к переносу",
    Verdict.TOO_OLD: "старше 3 месяцев",
    Verdict.ALREADY_OURS: "уже в журнале центра (по номеру TAR)",
    Verdict.ALREADY_IMPORTED: "уже перенесено раньше",
    Verdict.MATCHED_OURS: "уже в журнале центра (совпали объект, ТС, масса и время)",
    Verdict.NOT_COMPLETED: "незавершённое тарирование",
    Verdict.IGNORED: "помечено «не учитывать при взвешивании»",
    Verdict.NO_VEHICLE: "без номера ТС",
    Verdict.TOO_HEAVY: "тара тяжелее лимита",
    Verdict.UNKNOWN_OBJECT: "объект не заведён в центре",
}


@dataclass(frozen=True)
class PlanItem:
    taring: AisTaring
    verdict: Verdict
    scale_id: int | None  # весы центра (None — объект не сопоставлен)


def _normalize(value: str | None) -> str:
    return (value or "").strip().upper()


def _parse_moment(raw: str) -> datetime:
    text = " ".join(raw.split())
    for fmt in _DATE_FORMATS:
        try:
            return datetime.strptime(text, fmt).replace(tzinfo=BISHKEK).astimezone(UTC)
        except ValueError:
            continue
    raise ValueError(f"дата «{raw}» не разобрана")


def _parse_massa(raw: str) -> float:
    text = raw.strip().replace(" ", "").replace("\u00a0", "").replace(",", ".")
    massa = float(text)
    if massa <= 0:
        raise ValueError(f"масса {raw} не положительная")
    return massa


def parse_ais_tarings(text: str) -> tuple[list[AisTaring], list[str]]:
    """Разобрать выгрузку; вернуть строки и сообщения о нечитаемых строках.

    Заголовок обязателен (иначе ``TareImportError`` — файл не тот); отдельная
    кривая строка перенос не останавливает — она попадает в отчёт.
    """
    # restkey: лишние «;» в строке уходят в список под этим ключом и ниже
    # отбрасываются вместе с недостающими полями (None), а не роняют разбор
    reader = csv.DictReader(io.StringIO(text.lstrip("\ufeff")), delimiter=";", restkey="_extra")
    header = [name.strip() for name in reader.fieldnames or []]
    missing = [name for name in REQUIRED_COLUMNS if name not in header]
    if missing:
        raise TareImportError(
            "в файле нет колонок выгрузки тарирований АИС «СВХ»: " + ", ".join(missing)
        )
    tarings: list[AisTaring] = []
    problems: list[str] = []
    for number, raw in enumerate(reader, start=2):
        row = {
            key.strip(): value.strip()
            for key, value in raw.items()
            if isinstance(key, str) and isinstance(value, str)
        }
        tare_ref = row.get(COL_TARE_REF, "")
        try:
            if not tare_ref:
                raise ValueError("нет номера тарирования")
            unit = row.get(COL_UNIT, "")
            if unit and unit.lower() != UNIT_KG:
                raise ValueError(f"единица измерения «{unit}» — ожидались кг")
            tarings.append(
                AisTaring(
                    tare_ref=tare_ref,
                    entry_ref=row.get(COL_ENTRY) or None,
                    vehicle_number=_normalize(row.get(COL_VEHICLE)),
                    trailer_number=_normalize(row.get(COL_TRAILER)) or None,
                    massa=_parse_massa(row.get(COL_MASSA, "")),
                    object_name=row.get(COL_OBJECT, ""),
                    operator=row.get(COL_OPERATOR) or None,
                    tared_at=_parse_moment(row.get(COL_TARED_AT, "")),
                    completed=row.get(COL_STATUS, "") == STATUS_COMPLETED,
                    ignored=row.get(COL_IGNORED, "").lower() == "true",
                )
            )
        except ValueError as exc:
            problems.append(f"строка {number} ({tare_ref or 'без номера'}): {exc}")
    return tarings, problems


def _matches_ours(
    taring: AisTaring, site_code: str, ours: Mapping[tuple[str, str], list[OurTaring]]
) -> bool:
    for candidate in ours.get((site_code, taring.vehicle_number), ()):
        if (
            abs(candidate.massa - taring.massa) <= MATCH_MASSA_KG
            and abs(candidate.weighed_at - taring.tared_at) <= MATCH_WINDOW
        ):
            return True
    return False


def plan_import(
    tarings: Iterable[AisTaring],
    *,
    now: datetime,
    targets: Mapping[str, ImportTarget],
    known_refs: Mapping[str, WeighingSource],
    our_tarings: Iterable[OurTaring] = (),
    max_tare_kg: float = DEFAULT_MAX_TARE_KG,
) -> list[PlanItem]:
    """Вынести вердикт каждой строке выгрузки (порядок строк сохраняется).

    ``targets`` — объект АИС → весы центра; ``known_refs`` — номера TAR,
    уже закреплённые за записями журнала, с источником записи;
    ``our_tarings`` — тарирования журнала для сопоставления без номера;
    ``max_tare_kg`` — лимит тары (0 — без лимита).
    """
    threshold = three_months_before(now)
    ours: dict[tuple[str, str], list[OurTaring]] = defaultdict(list)
    for our in our_tarings:
        ours[(our.site_code, our.vehicle_number)].append(our)
    plan: list[PlanItem] = []
    for taring in tarings:
        target = targets.get(taring.object_name)
        verdict = _verdict(
            taring,
            target,
            threshold=threshold,
            known_refs=known_refs,
            ours=ours,
            max_tare_kg=max_tare_kg,
        )
        plan.append(
            PlanItem(
                taring=taring,
                verdict=verdict,
                scale_id=target.scale_id if target is not None else None,
            )
        )
    return plan


def _verdict(
    taring: AisTaring,
    target: ImportTarget | None,
    *,
    threshold: datetime,
    known_refs: Mapping[str, WeighingSource],
    ours: Mapping[tuple[str, str], list[OurTaring]],
    max_tare_kg: float,
) -> Verdict:
    """Первая сработавшая причина не переносить строку — либо IMPORT."""
    if target is None:
        return Verdict.UNKNOWN_OBJECT
    if taring.tared_at < threshold:
        return Verdict.TOO_OLD
    known = known_refs.get(taring.tare_ref)
    if known is not None:
        return (
            Verdict.ALREADY_IMPORTED if known is WeighingSource.IMPORTED else Verdict.ALREADY_OURS
        )
    if taring.vehicle_number and _matches_ours(taring, target.site_code, ours):
        return Verdict.MATCHED_OURS
    if not taring.completed:
        return Verdict.NOT_COMPLETED
    if taring.ignored:
        return Verdict.IGNORED
    if not taring.vehicle_number:
        return Verdict.NO_VEHICLE
    if max_tare_kg > 0 and taring.massa > max_tare_kg:
        return Verdict.TOO_HEAVY
    return Verdict.IMPORT


def summarize(plan: Iterable[PlanItem]) -> dict[Verdict, int]:
    """Число строк по вердиктам (в порядке объявления Verdict, нули опущены)."""
    counts = Counter(item.verdict for item in plan)
    return {verdict: counts[verdict] for verdict in Verdict if counts[verdict]}


def summarize_by_object(plan: Iterable[PlanItem]) -> dict[str, dict[Verdict, int]]:
    """То же в разрезе объектов выгрузки."""
    by_object: dict[str, Counter[Verdict]] = defaultdict(Counter)
    for item in plan:
        by_object[item.taring.object_name][item.verdict] += 1
    return {
        name: {verdict: counts[verdict] for verdict in Verdict if counts[verdict]}
        for name, counts in by_object.items()
    }


def pairs_to_import(plan: Iterable[PlanItem]) -> int:
    """Сколько сцепок получит тару: строк может быть больше (реестр хранит последнюю)."""
    return len({item.taring.pair for item in plan if item.verdict is Verdict.IMPORT})
