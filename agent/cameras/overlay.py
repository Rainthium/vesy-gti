"""Оверлей на снимок камеры: имя камеры, дата/время, зафиксированный вес.

Как OSD UniServer (docs/uniserver-kyzylkia.md): часам камер не доверяем
(на Кызыл-Кые в кадре штамп «1970»), поэтому агент прожигает собственную
плашку в момент фиксации — однократно, ДО расчёта sha256 (правило №2:
после сохранения фото не пересжимается никогда, sha256 связан с записью).

Время на плашке — бишкекское (правило №4а: UTC+6, переводов нет).
Шрифт — DejaVu Sans Bold (подмножество: латиница + кириллица), лежит
в комплекте агента: на весовом ПК наличие системных шрифтов
не гарантировано. Лицензия — fonts/DEJAVU-LICENSE.txt.

Деградация безопасна: если кадр не декодируется или прожиг падает,
возвращаются исходные байты (фото важнее оверлея), проблема — в лог.
"""

import io
import logging
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta, timezone
from pathlib import Path

from PIL import Image, ImageDraw, ImageFont

from shared.enums import CameraRole

logger = logging.getLogger(__name__)

JPEG_QUALITY = 85  # правило №2: JPEG q85, родное разрешение

BISHKEK_TZ = timezone(timedelta(hours=6))  # Asia/Bishkek, переводов нет

_FONT_PATH = Path(__file__).parent / "fonts" / "DejaVuSans-Bold-subset.ttf"

CAMERA_LABELS = {
    CameraRole.FRONT: "Камера 1 · перед",
    CameraRole.REAR: "Камера 2 · зад",
}


@dataclass(frozen=True)
class OverlayInfo:
    """Что прожигаем: камера, момент фиксации (UTC), вес."""

    role: CameraRole
    moment: datetime
    weight_kg: float | None  # None — вес не зафиксирован (плашка без веса)


def _format_weight(weight_kg: float) -> str:
    """12500.0 → «12 500 кг» (узкий неразрывный пробел, как в интерфейсах)."""
    return f"{weight_kg:,.0f}".replace(",", " ") + " кг"


def _overlay_text(info: OverlayInfo) -> str:
    """Одна строка плашки: камера · дата время · вес."""
    moment = info.moment
    if moment.tzinfo is None:
        moment = moment.replace(tzinfo=UTC)
    stamp = moment.astimezone(BISHKEK_TZ).strftime("%d.%m.%Y %H:%M:%S")
    parts = [CAMERA_LABELS.get(info.role, str(info.role)), stamp]
    if info.weight_kg is not None:
        parts.append(_format_weight(info.weight_kg))
    return "  ·  ".join(parts)


def burn_overlay(jpeg: bytes, info: OverlayInfo) -> bytes:
    """Прожечь плашку в нижней части кадра; вернуть новый JPEG (q85).

    При любой ошибке (битый кадр, отсутствие шрифта) возвращаются
    ИСХОДНЫЕ байты — снимок не теряется.
    """
    try:
        return _burn(jpeg, info)
    except Exception:
        logger.exception("оверлей не прожёгся (%s) — снимок сохраняется без плашки", info.role)
        return jpeg


def _burn(jpeg: bytes, info: OverlayInfo) -> bytes:
    image: Image.Image = Image.open(io.BytesIO(jpeg))
    if image.mode != "RGB":
        image = image.convert("RGB")

    # размер плашки — от ширины кадра (2560 → шрифт ~46, миниатюры читаемы)
    font_size = max(14, image.width // 56)
    font = ImageFont.truetype(str(_FONT_PATH), font_size)
    text = _overlay_text(info)

    draw = ImageDraw.Draw(image, "RGBA")
    padding = max(4, font_size // 3)
    text_box = draw.textbbox((0, 0), text, font=font)
    strip_height = (text_box[3] - text_box[1]) + padding * 2
    top = image.height - strip_height
    # полупрозрачная тёмная полоса внизу — текст читается на любом кадре
    draw.rectangle((0, top, image.width, image.height), fill=(0, 0, 0, 150))
    draw.text(
        (padding, top + padding - text_box[1]),
        text,
        font=font,
        fill=(255, 255, 255, 255),
    )

    out = io.BytesIO()
    image.save(out, "JPEG", quality=JPEG_QUALITY)
    return out.getvalue()
