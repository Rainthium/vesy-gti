"""Графики экрана «Отчёты» — серверный SVG без внешних библиотек.

Панель работает во внутренней сети без CDN, печатается и живёт в токенах
дизайн-системы ГТИ, поэтому графики собираются здесь строками SVG:
горизонтальные столбики «по объектам», столбики динамики по отрезкам
периода и линии по объектам. Цвета — из палитры ДС (панель и печать
одинаковы). Числа подписываются на самих столбиках, всплывающая
подсказка — стандартный ``<title>``.

Все функции возвращают готовую разметку ``<svg …>`` (в шаблоне — ``| safe``);
тексты экранируются.
"""

from __future__ import annotations

import math
from collections.abc import Sequence
from html import escape

PRIMARY = "#0E8A5B"
PRIMARY_DARK = "#0B5E43"
MUTED = "#6B7280"
FAINT = "#9AA1AB"
GRID = "#E2E6EA"
TEXT = "#1A1F2B"

# палитра линий «по объектам»: до 13 объектов ОАО, цвета различимы и на печати
PALETTE = (
    "#0E8A5B",
    "#2E97D4",
    "#E8A33D",
    "#D9434E",
    "#7C5CBF",
    "#1FA36F",
    "#B5780F",
    "#1F7FBB",
    "#C2333E",
    "#0B5E43",
    "#8B5E3C",
    "#5B6C8F",
    "#9AA1AB",
)

FONT = "font-family:Inter,-apple-system,'Segoe UI',sans-serif"


def fmt_number(value: float, *, decimals: int | None = None) -> str:
    """Число с пробелами между разрядами; дробные — с одним знаком (2,5)."""
    if decimals is None:
        decimals = 0 if abs(value) >= 10 or float(value).is_integer() else 1
    text = f"{value:,.{decimals}f}".replace(",", " ").replace(".", ",")
    return text


def nice_ceiling(value: float, ticks: int = 4) -> float:
    """Верх шкалы: «круглое» число не меньше value, делящееся на ticks."""
    if value <= 0:
        return float(ticks)
    raw_step = value / ticks
    magnitude = 10 ** math.floor(math.log10(raw_step))
    for factor in (1, 2, 2.5, 4, 5, 10):
        step = factor * magnitude
        if step * ticks >= value:
            return float(step * ticks)
    return float(10 * magnitude * ticks)


def _text(x: float, y: float, content: str, **attrs: str) -> str:
    extra = "".join(f' {k.replace("_", "-")}="{escape(v)}"' for k, v in attrs.items())
    return f'<text x="{x:.1f}" y="{y:.1f}"{extra}>{escape(content)}</text>'


def bar_chart_horizontal(
    rows: Sequence[tuple[str, float]],
    *,
    unit: str = "",
    decimals: int | None = None,
    color: str = PRIMARY,
    title: str = "",
) -> str:
    """Горизонтальные столбики: подпись слева, значение у конца столбика.

    ``rows`` — (подпись, значение) в порядке показа. Пустой список —
    подпись «нет данных».
    """
    row_h = 30
    label_w = 230
    value_w = 90
    width = 760
    top = 8
    height = top + max(1, len(rows)) * row_h + 8
    parts = [
        f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {width} {height}" '
        f'width="100%" role="img" aria-label="{escape(title or "график")}" '
        f'style="{FONT};display:block;max-width:100%;height:auto">'
    ]
    if title:
        parts.append(f"<title>{escape(title)}</title>")
    if not rows:
        parts.append(
            _text(
                width / 2,
                top + row_h / 2 + 4,
                "нет данных",
                fill=MUTED,
                font_size="13",
                text_anchor="middle",
            )
        )
        parts.append("</svg>")
        return "".join(parts)
    max_value = max((v for _, v in rows), default=0.0)
    scale_w = width - label_w - value_w
    for index, (label, value) in enumerate(rows):
        y = top + index * row_h
        bar_w = scale_w * (value / max_value) if max_value > 0 else 0
        text_value = fmt_number(value, decimals=decimals) + (f" {unit}" if unit else "")
        parts.append("<g>")
        parts.append(f"<title>{escape(label)}: {escape(text_value)}</title>")
        parts.append(
            _text(
                label_w - 12,
                y + row_h / 2 + 4,
                _shorten(label, 30),
                fill=TEXT,
                font_size="13",
                font_weight="600",
                text_anchor="end",
            )
        )
        parts.append(
            f'<rect x="{label_w}" y="{y + 6:.1f}" width="{max(bar_w, 2 if value > 0 else 0):.1f}" '
            f'height="{row_h - 12}" rx="4" fill="{color}"/>'
        )
        parts.append(
            _text(
                label_w + bar_w + 8,
                y + row_h / 2 + 4,
                text_value,
                fill=MUTED,
                font_size="12.5",
                font_weight="700",
            )
        )
        parts.append("</g>")
    parts.append("</svg>")
    return "".join(parts)


def _shorten(text: str, limit: int) -> str:
    return text if len(text) <= limit else text[: limit - 1] + "…"


def _grid_line(x1: float, x2: float, y: float) -> str:
    return f'<line x1="{x1}" y1="{y:.1f}" x2="{x2}" y2="{y:.1f}" stroke="{GRID}" stroke-width="1"/>'


def _axis_decimals(top: float, ticks: int, decimals: int | None) -> int:
    """Знаки после запятой у делений оси: целые деления — без дробной части."""
    if all(float(top * tick / ticks).is_integer() for tick in range(ticks + 1)):
        return 0
    return decimals if decimals is not None else 1


def _axis_labels(labels: Sequence[str], max_labels: int) -> set[int]:
    """Индексы подписей оси X, которые поместятся (равномерное прореживание)."""
    n = len(labels)
    if n <= max_labels:
        return set(range(n))
    step = math.ceil(n / max_labels)
    return {i for i in range(n) if i % step == 0}


def column_chart(
    labels: Sequence[str],
    values: Sequence[float],
    *,
    unit: str = "",
    decimals: int | None = None,
    color: str = PRIMARY,
    title: str = "",
    height: int = 220,
) -> str:
    """Вертикальные столбики по отрезкам периода (динамика одного показателя)."""
    width = 1000
    pad_l, pad_r, pad_t, pad_b = 56, 12, 14, 34
    plot_w = width - pad_l - pad_r
    plot_h = height - pad_t - pad_b
    n = max(1, len(labels))
    top = nice_ceiling(max(values, default=0.0))
    ticks = 4
    axis_decimals = _axis_decimals(top, ticks, decimals)
    parts = [
        f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {width} {height}" width="100%" '
        f'role="img" aria-label="{escape(title or "график")}" '
        f'style="{FONT};display:block;max-width:100%;height:auto">'
    ]
    if title:
        parts.append(f"<title>{escape(title)}</title>")
    # сетка и подписи оси Y
    for tick in range(ticks + 1):
        value = top * tick / ticks
        y = pad_t + plot_h - plot_h * tick / ticks
        parts.append(_grid_line(pad_l, width - pad_r, y))
        parts.append(
            _text(
                pad_l - 8,
                y + 4,
                fmt_number(value, decimals=axis_decimals),
                fill=FAINT,
                font_size="11.5",
                text_anchor="end",
            )
        )
    slot = plot_w / n
    bar_w = max(2.0, min(48.0, slot * 0.64))
    shown = _axis_labels(labels, 16)
    for index, (label, value) in enumerate(zip(labels, values, strict=True)):
        x = pad_l + slot * index + (slot - bar_w) / 2
        h = plot_h * (value / top) if top > 0 else 0
        y = pad_t + plot_h - h
        text_value = fmt_number(value, decimals=decimals) + (f" {unit}" if unit else "")
        parts.append("<g>")
        parts.append(f"<title>{escape(label)}: {escape(text_value)}</title>")
        bar_h = max(h, 1.5 if value > 0 else 0)
        parts.append(
            f'<rect x="{x:.1f}" y="{y:.1f}" width="{bar_w:.1f}" height="{bar_h:.1f}" '
            f'rx="3" fill="{color}"/>'
        )
        if value > 0 and n <= 40:
            parts.append(
                _text(
                    x + bar_w / 2,
                    y - 5,
                    fmt_number(value, decimals=decimals),
                    fill=MUTED,
                    font_size="11",
                    font_weight="700",
                    text_anchor="middle",
                )
            )
        if index in shown:
            parts.append(
                _text(
                    x + bar_w / 2,
                    height - pad_b + 18,
                    label,
                    fill=MUTED,
                    font_size="11.5",
                    text_anchor="middle",
                )
            )
        parts.append("</g>")
    parts.append("</svg>")
    return "".join(parts)


def line_chart(
    labels: Sequence[str],
    series: Sequence[tuple[str, Sequence[float]]],
    *,
    unit: str = "",
    decimals: int | None = None,
    title: str = "",
    height: int = 260,
) -> str:
    """Линии по объектам на общей шкале; легенда — под графиком."""
    width = 1000
    pad_l, pad_r, pad_t, pad_b = 56, 12, 14, 34
    legend_rows = math.ceil(len(series) / 4) if series else 0
    legend_h = legend_rows * 20 + (8 if legend_rows else 0)
    total_h = height + legend_h
    plot_w = width - pad_l - pad_r
    plot_h = height - pad_t - pad_b
    n = max(1, len(labels))
    top = nice_ceiling(max((max(values, default=0.0) for _, values in series), default=0.0))
    ticks = 4
    axis_decimals = _axis_decimals(top, ticks, decimals)
    parts = [
        f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {width} {total_h}" width="100%" '
        f'role="img" aria-label="{escape(title or "график")}" '
        f'style="{FONT};display:block;max-width:100%;height:auto">'
    ]
    if title:
        parts.append(f"<title>{escape(title)}</title>")
    for tick in range(ticks + 1):
        value = top * tick / ticks
        y = pad_t + plot_h - plot_h * tick / ticks
        parts.append(_grid_line(pad_l, width - pad_r, y))
        parts.append(
            _text(
                pad_l - 8,
                y + 4,
                fmt_number(value, decimals=axis_decimals),
                fill=FAINT,
                font_size="11.5",
                text_anchor="end",
            )
        )
    slot = plot_w / n
    shown = _axis_labels(labels, 16)
    for index, label in enumerate(labels):
        if index in shown:
            x = pad_l + slot * index + slot / 2
            parts.append(
                _text(
                    x,
                    height - pad_b + 18,
                    label,
                    fill=MUTED,
                    font_size="11.5",
                    text_anchor="middle",
                )
            )
    for s_index, (name, values) in enumerate(series):
        color = PALETTE[s_index % len(PALETTE)]
        points = []
        for index, value in enumerate(values):
            x = pad_l + slot * index + slot / 2
            y = pad_t + plot_h - (plot_h * value / top if top > 0 else 0)
            points.append((x, y, value))
        path = " ".join(
            f"{'M' if i == 0 else 'L'}{x:.1f},{y:.1f}" for i, (x, y, _) in enumerate(points)
        )
        parts.append("<g>")
        parts.append(f"<title>{escape(name)}</title>")
        if len(points) > 1:
            parts.append(
                f'<path d="{path}" fill="none" stroke="{color}" stroke-width="2.2" '
                'stroke-linejoin="round" stroke-linecap="round"/>'
            )
        for x, y, value in points:
            if value > 0 or len(points) == 1:
                parts.append(f'<circle cx="{x:.1f}" cy="{y:.1f}" r="3.2" fill="{color}"/>')
        parts.append("</g>")
        # легенда
        col, row = s_index % 4, s_index // 4
        lx = pad_l + col * (plot_w / 4)
        ly = height + 6 + row * 20
        parts.append(
            f'<rect x="{lx:.1f}" y="{ly:.1f}" width="14" height="14" rx="3" fill="{color}"/>'
        )
        parts.append(
            _text(
                lx + 20, ly + 11, _shorten(name, 34), fill=TEXT, font_size="12", font_weight="600"
            )
        )
    if not series:
        parts.append(
            _text(
                width / 2,
                pad_t + plot_h / 2,
                "нет данных",
                fill=MUTED,
                font_size="13",
                text_anchor="middle",
            )
        )
    parts.append("</svg>")
    return "".join(parts)
