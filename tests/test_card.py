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
        "photos": [],
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
        assert card["massa_text"] == "42 850"
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
        assert card["massa_text"] == "14 820"

    def test_weighing_without_tare(self) -> None:
        """Взвешивание без действующей тары: тара и нетто — прочерки."""
        card = _weighing_card(tare_value=None, netto=None, tared_at=None)
        assert card["tare_text"] == "—"
        assert card["netto_text"] == "—"
        assert card["tared_at_text"] is None
