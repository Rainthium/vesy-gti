"""Тесты оверлея на снимках (agent/cameras/overlay.py).

Покрытие:
- текст плашки: камера, бишкекское время (+6 к UTC), вес с U+202F;
- прожиг: результат — валидный JPEG родного разрешения, байты меняются,
  низ кадра затемнён (полоса плашки);
- деградация: битые байты возвращаются как есть (фото важнее оверлея);
- store_shots прожигает до расчёта sha256 (хеш считается от плашки).
"""

import hashlib
import io
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

from PIL import Image

from agent.cameras.capture import CameraShot
from agent.cameras.overlay import OverlayInfo, _overlay_text, burn_overlay
from agent.weighing.shots import store_shots
from shared.enums import CameraRole

MOMENT = datetime(2026, 8, 8, 9, 30, 5, tzinfo=UTC)  # 15:30:05 в Бишкеке


def make_jpeg(color: str = "gray", size: tuple[int, int] = (320, 240)) -> bytes:
    buffer = io.BytesIO()
    Image.new("RGB", size, color).save(buffer, "JPEG", quality=90)
    return buffer.getvalue()


class TestOverlayText:
    def test_front_camera_bishkek_time_and_weight(self) -> None:
        """Плашка: имя камеры, время UTC+6, вес с узким пробелом U+202F."""
        text = _overlay_text(OverlayInfo(role=CameraRole.FRONT, moment=MOMENT, weight_kg=12500.0))
        assert text == "Камера 1 · перед  ·  08.08.2026 15:30:05  ·  12 500 кг"

    def test_rear_camera_without_weight(self) -> None:
        """Без зафиксированного веса плашка ограничивается камерой и временем."""
        text = _overlay_text(OverlayInfo(role=CameraRole.REAR, moment=MOMENT, weight_kg=None))
        assert text == "Камера 2 · зад  ·  08.08.2026 15:30:05"

    def test_naive_moment_treated_as_utc(self) -> None:
        """Naive-время трактуется как UTC (в БД время хранится в UTC)."""
        text = _overlay_text(
            OverlayInfo(role=CameraRole.FRONT, moment=MOMENT.replace(tzinfo=None), weight_kg=None)
        )
        assert "15:30:05" in text


class TestBurnOverlay:
    def test_result_is_valid_jpeg_same_size_different_bytes(self) -> None:
        original = make_jpeg()
        burned = burn_overlay(
            original, OverlayInfo(role=CameraRole.FRONT, moment=MOMENT, weight_kg=20000.0)
        )
        assert burned != original
        image = Image.open(io.BytesIO(burned))
        assert image.format == "JPEG"
        assert image.size == (320, 240)  # родное разрешение (правило №2)

    def test_bottom_strip_darkened(self) -> None:
        """Плашка — затемнённая полоса внизу: низ темнее исходного белого кадра."""
        burned = burn_overlay(
            make_jpeg("white"),
            OverlayInfo(role=CameraRole.FRONT, moment=MOMENT, weight_kg=1000.0),
        )
        image = Image.open(io.BytesIO(burned)).convert("L")
        top_pixel = image.getpixel((image.width // 2, 2))
        bottom_pixel = image.getpixel((image.width - 6, image.height - 3))
        assert isinstance(top_pixel, int) and isinstance(bottom_pixel, int)
        assert top_pixel > 200  # верх остался белым
        assert bottom_pixel < 160  # низ накрыт полупрозрачной полосой

    def test_garbage_bytes_returned_unchanged(self) -> None:
        """Битый кадр не декодируется — исходные байты сохраняются как есть."""
        garbage = b"\xff\xd8\xff\xe0not-a-real-jpeg\xff\xd9"
        assert (
            burn_overlay(garbage, OverlayInfo(role=CameraRole.REAR, moment=MOMENT, weight_kg=None))
            == garbage
        )


class TestStoreShotsBurnsBeforeHash:
    def test_sha256_matches_stored_overlaid_file(self, tmp_path: Path) -> None:
        """Оверлей прожигается ДО sha256: хеш и размер — от файла с плашкой."""
        original = make_jpeg("blue")
        record_uuid = uuid4()
        photos, errors = store_shots(
            tmp_path,
            record_uuid,
            MOMENT,
            [CameraShot(role=CameraRole.FRONT, jpeg=original, captured_at=MOMENT)],
            weight_kg=8000.0,
        )
        assert not errors
        stored = Path(photos[0].path).read_bytes()
        assert stored != original  # плашка прожжена
        assert photos[0].sha256 == hashlib.sha256(stored).hexdigest()
        assert photos[0].size_bytes == len(stored)
