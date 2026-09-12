"""Разовый перенос тарирований из выгрузки АИС «СВХ» в журнал и реестр тар центра.

Решение Игоря 12.09.2026 (см. ``center/tare_import.py``): при переключении
объекта на весовую систему действующие тары, проведённые через UniServer,
живут только в АИС — переносим их из выгрузки «Тарирования» АИС, чтобы
сцепкам не тарироваться заново. Кнопки в панели нет — инструмент запускается
в контейнере центра (deploy/README.md):

    docker cp tares.csv ves-center-app-1:/tmp/tares.csv
    docker compose exec -T app uv run --no-sync python -m tools.import_ais_tares /tmp/tares.csv
    docker compose exec -T app uv run --no-sync python -m tools.import_ais_tares \\
        /tmp/tares.csv --apply

Без ``--apply`` — только проверка: вердикт каждой строке и сводка. С
``--apply`` — перенос одной транзакцией, запись в аудит; повтор с той же
или более свежей выгрузкой безопасен (уже перенесённое узнаётся по номеру
TAR). Реестр тар уходит агентам при следующем тарировании на любом объекте
либо при переподключении агента (полный снимок при hello) — отдельной
рассылки инструмент не делает, он вне процесса центра.
"""

from __future__ import annotations

import argparse
import csv
import sys
from collections.abc import Iterable, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import TextIO

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session, sessionmaker

from center.db import repo
from center.db.models import AuditLog, Scale, Site
from center.db.session import make_engine, make_session_factory
from center.tare_import import (
    BISHKEK,
    VERDICT_LABELS,
    ImportTarget,
    PlanItem,
    TareImportError,
    Verdict,
    pairs_to_import,
    parse_ais_tarings,
    plan_import,
    summarize,
    summarize_by_object,
)
from shared.tare import DEFAULT_MAX_TARE_KG, three_months_before

ACTOR = "tools.import_ais_tares"

# объекты выгрузки АИС «СВХ» → код объекта центра и весы (None — единственные
# весы объекта). У Канта весов двое: тарируют на «Весы №2 (тарирование)»
AIS_OBJECTS: dict[str, tuple[str, str | None]] = {
    "Кант": ("kant", "Весы №2 (тарирование)"),
    "Аламедин СВХ": ("alamedin", None),
    "Алтын Логистик": ("altyn-logistik", None),
    "Кокчо-Коз СВХ": ("kokcho-koz", None),
    "Кара-Суу": ("kara-suu", None),
    "Кызыл-Кыя": ("kyzyl-kyia", None),
    "Джалал-Абад ПЗТК": ("jalal-abad", None),
    "Кербен ВЗТК": ("kerben", None),
}


class TargetError(ValueError):
    """Объект выгрузки не сопоставлен с весами центра однозначно."""


def parse_object_option(raw: str) -> tuple[str, tuple[str, str | None]]:
    """``--object "Кант=kant/Весы №2 (тарирование)"`` → («Кант», («kant», «Весы №2…»))."""
    name, sep, target = raw.partition("=")
    if not sep or not name.strip() or not target.strip():
        raise argparse.ArgumentTypeError(
            f"ожидалось ОБЪЕКТ=код_объекта[/имя весов], получено: {raw!r}"
        )
    code, _, scale_name = target.partition("/")
    return name.strip(), (code.strip(), scale_name.strip() or None)


def resolve_targets(
    session: Session, objects: dict[str, tuple[str, str | None]]
) -> tuple[dict[str, ImportTarget], list[str]]:
    """Объекты выгрузки → весы центра; вторым — объекты, которых в центре нет."""
    targets: dict[str, ImportTarget] = {}
    absent: list[str] = []
    for ais_name, (site_code, scale_name) in objects.items():
        site = session.execute(select(Site).where(Site.code == site_code)).scalar_one_or_none()
        if site is None:
            absent.append(f"{ais_name} → объект «{site_code}» не заведён")
            continue
        scales = list(
            session.execute(
                select(Scale).where(Scale.site_id == site.id).order_by(Scale.id)
            ).scalars()
        )
        if scale_name is not None:
            chosen = [scale for scale in scales if scale.name == scale_name]
            if not chosen:
                raise TargetError(
                    f"{ais_name}: у объекта «{site_code}» нет весов «{scale_name}» "
                    f"(есть: {', '.join(scale.name for scale in scales) or 'нет'})"
                )
            scale = chosen[0]
        elif len(scales) == 1:
            scale = scales[0]
        elif not scales:
            absent.append(f"{ais_name} → у объекта «{site_code}» нет весов")
            continue
        else:
            raise TargetError(
                f"{ais_name}: у объекта «{site_code}» несколько весов — укажите имя: "
                + ", ".join(scale.name for scale in scales)
            )
        targets[ais_name] = ImportTarget(site_code=site_code, scale_id=scale.id)
    return targets, absent


def _print_summary(
    plan: Sequence[PlanItem],
    problems: Sequence[str],
    targets: dict[str, ImportTarget],
    absent: Sequence[str],
    out: TextIO,
) -> None:
    print("Сопоставление объектов:", file=out)
    for name, target in targets.items():
        print(f"  {name} → {target.site_code}, весы id={target.scale_id}", file=out)
    for line in absent:
        print(f"  {line} — строки объекта не переносятся", file=out)
    print(f"\nСтрок в выгрузке: {len(plan) + len(problems)}", file=out)
    for verdict, count in summarize(plan).items():
        print(f"  {count:6d}  {VERDICT_LABELS[verdict]}", file=out)
    if problems:
        print(f"  {len(problems):6d}  не разобраны:", file=out)
        for line in problems[:20]:
            print(f"           {line}", file=out)
        if len(problems) > 20:
            print(f"           … и ещё {len(problems) - 20}", file=out)
    print("\nПо объектам выгрузки:", file=out)
    for name, counts in summarize_by_object(plan).items():
        parts = ", ".join(
            f"{VERDICT_LABELS[verdict]}: {count}" for verdict, count in counts.items()
        )
        print(f"  {name}: {parts}", file=out)
    to_import = [item for item in plan if item.verdict is Verdict.IMPORT]
    if to_import:
        massas = sorted(item.taring.massa for item in to_import)
        first = min(item.taring.tared_at for item in to_import)
        last = max(item.taring.tared_at for item in to_import)
        print(
            f"\nК переносу: {len(to_import)} строк, сцепок {pairs_to_import(plan)}; "
            f"тара от {massas[0]:.0f} до {massas[-1]:.0f} кг "
            f"(медиана {massas[len(massas) // 2]:.0f}); "
            f"с {first:%d.%m.%Y} по {last:%d.%m.%Y}",
            file=out,
        )
    else:
        print("\nПереносить нечего.", file=out)


def _write_report(path: Path, plan: Iterable[PlanItem], problems: Sequence[str]) -> None:
    """Построчный отчёт (CSV с «;», как выгрузка) — что и почему."""
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.writer(handle, delimiter=";")
        writer.writerow(
            ["Номер TAR", "Объект", "Номер ТС", "Прицеп", "Тара, кг", "Дата (Бишкек)", "Вердикт"]
        )
        for item in plan:
            taring = item.taring
            writer.writerow(
                [
                    taring.tare_ref,
                    taring.object_name,
                    taring.vehicle_number,
                    taring.trailer_number or "",
                    f"{taring.massa:.0f}",
                    f"{taring.tared_at.astimezone(BISHKEK):%d.%m.%Y %H:%M}",
                    VERDICT_LABELS[item.verdict],
                ]
            )
        for line in problems:
            writer.writerow(["", "", "", "", "", "", f"не разобрана: {line}"])


def apply_plan(session: Session, plan: Sequence[PlanItem], *, source_file: str) -> int:
    """Перенести строки с вердиктом IMPORT одной транзакцией; вернуть число записей."""
    imported_at = datetime.now(UTC)
    saved = 0
    by_object: dict[str, int] = {}
    for item in plan:
        if item.verdict is not Verdict.IMPORT or item.scale_id is None:
            continue
        row = repo.save_imported_taring(
            session, item.scale_id, item.taring, imported_at=imported_at
        )
        if row is not None:
            saved += 1
            by_object[item.taring.object_name] = by_object.get(item.taring.object_name, 0) + 1
    session.add(
        AuditLog(
            actor=ACTOR,
            action="tares_import",
            details={
                "file": source_file,
                "imported": saved,
                "by_object": by_object,
                "verdicts": {verdict.value: count for verdict, count in summarize(plan).items()},
            },
        )
    )
    session.commit()
    return saved


def main(
    argv: Sequence[str] | None = None,
    *,
    session_factory: sessionmaker[Session] | None = None,
    out: TextIO = sys.stdout,
) -> int:
    parser = argparse.ArgumentParser(
        description="Перенос тарирований из выгрузки АИС «СВХ» в центр (по умолчанию — проверка)"
    )
    parser.add_argument("file", type=Path, help="выгрузка «Тарирования» АИС (CSV, «;», UTF-8)")
    parser.add_argument(
        "--apply", action="store_true", help="выполнить перенос (иначе только отчёт)"
    )
    parser.add_argument(
        "--max-tare-kg",
        type=float,
        default=DEFAULT_MAX_TARE_KG,
        help=(
            f"тары тяжелее не переносятся (по умолчанию {DEFAULT_MAX_TARE_KG:.0f}; 0 — без лимита)"
        ),
    )
    parser.add_argument(
        "--object",
        action="append",
        type=parse_object_option,
        default=[],
        metavar="ОБЪЕКТ=код[/весы]",
        help="сопоставление объекта выгрузки с объектом центра (дополняет встроенную таблицу)",
    )
    parser.add_argument("--report", type=Path, help="куда записать построчный отчёт (CSV)")
    args = parser.parse_args(argv)

    try:
        text = args.file.read_text(encoding="utf-8-sig")
    except OSError as exc:
        print(f"файл не прочитан: {exc}", file=out)
        return 2
    try:
        tarings, problems = parse_ais_tarings(text)
    except TareImportError as exc:
        print(str(exc), file=out)
        return 2

    objects = dict(AIS_OBJECTS)
    objects.update(dict(args.object))
    factory = session_factory or make_session_factory(make_engine())
    now = datetime.now(UTC)
    with factory() as session:
        try:
            targets, absent = resolve_targets(session, objects)
        except TargetError as exc:
            print(str(exc), file=out)
            return 2
        known = repo.known_ais_refs(session, (taring.tare_ref for taring in tarings))
        ours = repo.tarings_since(session, three_months_before(now))
        plan = plan_import(
            tarings,
            now=now,
            targets=targets,
            known_refs=known,
            our_tarings=ours,
            max_tare_kg=args.max_tare_kg,
        )
        _print_summary(plan, problems, targets, absent, out)
        if args.report is not None:
            _write_report(args.report, plan, problems)
            print(f"Построчный отчёт: {args.report}", file=out)
        if not args.apply:
            print("\nПроверка без записи. Для переноса добавьте --apply.", file=out)
            return 0
        try:
            saved = apply_plan(session, plan, source_file=args.file.name)
        except IntegrityError as exc:
            # журнал изменился между планом и записью (агент прислал тарирование с
            # тем же номером TAR): ничего не записано — повторный запуск пересчитает
            session.rollback()
            print(
                f"перенос не записан: журнал изменился во время переноса ({exc.orig}) — "
                "повторите запуск",
                file=out,
            )
            return 1
    print(
        f"\nПеренесено записей: {saved}. Реестр тар обновлён; агенты получат снимок при "
        "следующем тарировании на любом объекте или при переподключении.",
        file=out,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
