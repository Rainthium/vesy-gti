"""Тесты shared/card.py — общей печатной формы весовой карточки.

Номер и форматы обязаны совпадать при печати одной и той же записи
из агента и из центра — вся логика собрана в build_card и проверяется
здесь, а маршруты обеих сторон лишь передают свои данные.
"""

from datetime import UTC, date, datetime, timedelta, timezone

from shared.card import (
    build_card,
    card_number,
    fmt_dt,
    fmt_kg,
    logo_data_uri,
    verification_text,
)
from shared.enums import Operation
from shared.messages import VerificationInfo

WEIGHED_AT = datetime(2026, 8, 13, 4, 23, 1, tzinfo=UTC)  # 10:23:01 по Бишкеку


class TestCardNumber:
    def test_weighing_prefix_and_bishkek_time(self) -> None:
        """Взвешивание: префикс ВЕС, время бишкекское (+6 от UTC)."""
        assert card_number(Operation.WEIGHING, WEIGHED_AT) == "ВЕС-20260813-102301"

    def test_taring_prefix(self) -> None:
        assert card_number(Operation.TARING, WEIGHED_AT) == "ТАР-20260813-102301"

    def test_naive_treated_as_utc(self) -> None:
        """Naive-время (как в SQLite агента) трактуется как UTC."""
        naive = WEIGHED_AT.replace(tzinfo=None)
        assert card_number(Operation.WEIGHING, naive) == "ВЕС-20260813-102301"

    def test_date_rollover_across_midnight(self) -> None:
        """+6 часов переносит номер на следующие сутки."""
        moment = datetime(2026, 8, 13, 20, 30, tzinfo=UTC)
        assert card_number(Operation.WEIGHING, moment) == "ВЕС-20260814-023000"

    def test_other_timezone_converted(self) -> None:
        moment = datetime(2026, 8, 13, 6, 23, 1, tzinfo=timezone(timedelta(hours=2)))
        assert card_number(Operation.WEIGHING, moment) == "ВЕС-20260813-102301"


class TestFormats:
    def test_fmt_dt_bishkek(self) -> None:
        assert fmt_dt(WEIGHED_AT) == "13.08.2026 10:23:01"

    def test_fmt_kg_thousands_separator(self) -> None:
        assert fmt_kg(42850.0) == "42 850"

    def test_fmt_kg_none_is_dash(self) -> None:
        assert fmt_kg(None) == "—"


class TestVerificationText:
    def test_full_line_like_ais_act(self) -> None:
        """Полная строка — как в акте АИС."""
        info = VerificationInfo(
            number="№3961", verified_on=date(2026, 2, 26), valid_until=date(2027, 2, 26)
        )
        assert verification_text(info) == "№3961 от 26.02.2026 (срок до 26.02.2027)"

    def test_number_only(self) -> None:
        assert verification_text(VerificationInfo(number="№3961")) == "№3961"

    def test_number_and_date_without_deadline(self) -> None:
        info = VerificationInfo(number="№3961", verified_on=date(2026, 2, 26))
        assert verification_text(info) == "№3961 от 26.02.2026"

    def test_none_and_empty_number(self) -> None:
        assert verification_text(None) is None
        assert verification_text(VerificationInfo(number="")) is None


class TestLogo:
    def test_logo_is_embedded_jpeg(self) -> None:
        """Герб встроен data:URI — печатной странице не нужна статика."""
        uri = logo_data_uri()
        assert uri.startswith("data:image/jpeg;base64,")
        assert len(uri) > 1000


def _weighing_card(**overrides: object) -> dict[str, object]:
    fields: dict[str, object] = {
        "operation": Operation.WEIGHING,
        "weighed_at": WEIGHED_AT,
        "site_name": "СВХ «Кызыл-Кыя»",
        "scale_name": "Весы SCS-80",
        "vehicle_number": "P18035",
        "trailer_number": "P6802",
        "massa": 42850.0,
        "tare_value": 14820.0,
        "netto": 28030.0,
        "tared_at": datetime(2026, 7, 8, 2, 25, tzinfo=UTC),
        "operator": "Акимов Нурлан Боронбаевич",
        "verification": VerificationInfo(number="№3961"),
        "photo_front_url": None,
        "photo_rear_url": None,
        "photos_note": None,
        "record_uuid": "0" * 32,
    }
    fields.update(overrides)
    return build_card(**fields)  # type: ignore[arg-type]


class TestBuildCard:
    def test_weighing_fields(self) -> None:
        """Взвешивание: все три веса заполнены, обе даты на месте."""
        card = _weighing_card()
        assert card["number"] == "ВЕС-20260813-102301"
        assert card["is_weighing"] is True
        assert card["operation_label"] == "Взвешивание"
        assert card["weighed_at_text"] == "13.08.2026 10:23:01"
        assert card["tared_at_text"] == "08.07.2026 08:25:00"
        assert card["gross_text"] == "42 850"
        assert card["tare_text"] == "14 820"
        assert card["netto_text"] == "28 030"
        assert card["netto_note"] is None
        assert card["verification_text"] == "№3961"

    def test_taring_dashes(self) -> None:
        """Тарирование: масса идёт в ТАРУ, брутто и нетто — прочерки."""
        card = _weighing_card(
            operation=Operation.TARING,
            massa=14820.0,
            tare_value=None,
            netto=None,
            tared_at=None,
        )
        assert card["number"] == "ТАР-20260813-102301"
        assert card["is_weighing"] is False
        assert card["operation_label"] == "Тарирование"
        assert card["gross_text"] == "—"
        assert card["tare_text"] == "14 820"
        assert card["netto_text"] == "—"
        assert card["netto_note"] is None

    def test_weighing_without_tare(self) -> None:
        """Взвешивание без действующей тары: тара и нетто — прочерки."""
        card = _weighing_card(tare_value=None, netto=None, tared_at=None)
        assert card["tare_text"] == "—"
        assert card["netto_text"] == "—"
        assert card["tared_at_text"] is None
        assert card["netto_note"] == "Нетто не рассчитано: действующего тарирования сцепки не было."

    def test_photo_frames_always_two(self) -> None:
        """Рамки ПЕРЕД/ЗАД в карточке всегда, даже без доступных снимков
        (просьба Игоря 13.08.2026) — недоступный снимок = пустая рамка."""
        card = _weighing_card()
        photos = card["photos"]
        assert isinstance(photos, list) and len(photos) == 2
        assert [p["label"] for p in photos] == ["Фото 1 (перед)", "Фото 2 (зад)"]
        assert [p["url"] for p in photos] == [None, None]
        with_front = _weighing_card(photo_front_url="/photos/x/front.jpg")
        photos = with_front["photos"]
        assert isinstance(photos, list)
        assert photos[0]["url"] == "/photos/x/front.jpg"
        assert photos[1]["url"] is None


def _no_netto_card(**overrides: object) -> dict[str, object]:
    """Взвешивание без нетто — основа для проверок примечания «почему»."""
    fields: dict[str, object] = {"tare_value": None, "netto": None, "tared_at": None}
    fields.update(overrides)
    return _weighing_card(**fields)


class TestNettoNote:
    """Примечание «почему нет нетто» (просьба Игоря 14.08.2026).

    Показывается вместо снятой строки «Полная масса, кг:» только у успешных
    взвешиваний без нетто; для устаревшей тары — дата, время И МАССА.
    """

    def test_expired_tare_with_date_and_mass(self) -> None:
        card = _no_netto_card(
            latest_tared_at=datetime(2026, 3, 5, 8, 31, tzinfo=UTC),  # 14:31 по Бишкеку
            latest_tare_value=15300.0,
        )
        assert card["netto_note"] == (
            "Нетто не рассчитано: тарирование сцепки от 05.03.2026 14:31:00, "
            "тара 15 300 кг — устарело (тара действует 3 календарных месяца)."
        )

    def test_tare_newer_than_weighing_ignored(self) -> None:
        """Сцепку перетарировали ПОСЛЕ записи — чужая свежая дата на старую
        карту не попадает, печать одинакова в любой момент."""
        card = _no_netto_card(
            latest_tared_at=WEIGHED_AT + timedelta(days=3),
            latest_tare_value=15300.0,
        )
        assert card["netto_note"] == (
            "Нетто не рассчитано: действующего тарирования сцепки не было."
        )

    def test_boundary_matches_rule_four(self) -> None:
        """Граница «устарело» та же, что у подстановки (правило №4): ровно
        3 месяца — тара ещё действует, «устарело» писать нельзя (нетто нет
        по иной причине — например, реплика запоздала); микросекундой
        старше — уже устарело, дата и масса в примечании."""
        from shared.tare import three_months_before

        boundary = three_months_before(WEIGHED_AT)
        still_valid = _no_netto_card(latest_tared_at=boundary, latest_tare_value=15300.0)
        assert still_valid["netto_note"] == (
            "Нетто не рассчитано: действующего тарирования сцепки не было."
        )
        expired = _no_netto_card(
            latest_tared_at=boundary - timedelta(microseconds=1), latest_tare_value=15300.0
        )
        note = expired["netto_note"]
        assert isinstance(note, str) and "устарело" in note and "13.05.2026" in note

    def test_vehicle_number_missing(self) -> None:
        """АИС не передала номер — тару искать не по чему."""
        card = _no_netto_card(vehicle_number=None, trailer_number=None)
        assert card["netto_note"] == (
            "Нетто не рассчитано: номер транспортного средства не передан."
        )

    def test_error_record_has_no_note(self) -> None:
        """У записи с кодом ошибки нетто нет по другой причине — примечание
        о тарировании было бы враньём."""
        card = _no_netto_card(code_ok=False)
        assert card["netto_note"] is None

    def test_taring_and_ok_weighing_have_no_note(self) -> None:
        assert _weighing_card()["netto_note"] is None
        taring = _weighing_card(
            operation=Operation.TARING, massa=14820.0, tare_value=None, netto=None, tared_at=None
        )
        assert taring["netto_note"] is None

    def test_naive_dates_compared_as_utc(self) -> None:
        """Naive-даты обеих сторон трактуются как UTC — сравнение не падает."""
        card = _no_netto_card(
            weighed_at=WEIGHED_AT.replace(tzinfo=None),
            latest_tared_at=datetime(2026, 3, 5, 8, 31),
            latest_tare_value=15300.0,
        )
        note = card["netto_note"]
        assert isinstance(note, str) and "05.03.2026 14:31:00" in note
