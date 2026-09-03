"""Ручная операция при живой связи (0.4.28) не пересекается с командой АИС.

Пока автомат выполняет команду АИС, ручная фиксация отказывает и кнопка
неактивна — иначе на одну стоянку легли бы две записи (ais + local_offline).
"""

from pathlib import Path

import pytest

from agent.drivers.base import ScaleState
from agent.sync.storage import AgentStorage
from agent.weighing.manual import ManualFlowError, ManualOperationFlow
from shared.enums import Operation, ScaleStatus


def _flow(tmp_path: Path, *, busy: bool) -> ManualOperationFlow:
    return ManualOperationFlow(
        scale_state=lambda: ScaleState(status=ScaleStatus.OK, weight_kg=5000.0, stable=True),
        manual_allowed=lambda: True,
        storage=AgentStorage(":memory:"),
        cameras=[],
        photos_dir=tmp_path,
        busy=lambda: busy,
    )


def test_busy_runner_blocks_manual_capture(tmp_path: Path) -> None:
    flow = _flow(tmp_path, busy=True)
    assert flow.ready() is False
    with pytest.raises(ManualFlowError, match="АИС"):
        flow.prepare(
            Operation.WEIGHING, vehicle_number="01KG777AAA", trailer_number=None, operator="op"
        )


def test_idle_runner_lets_manual_capture_through(tmp_path: Path) -> None:
    flow = _flow(tmp_path, busy=False)
    assert flow.ready() is True
    preview = flow.prepare(
        Operation.WEIGHING, vehicle_number="01KG777AAA", trailer_number=None, operator="op"
    )
    assert preview.record.massa == 5000.0


def test_default_busy_is_false(tmp_path: Path) -> None:
    """Без колбэка (dev-стенд, старые сборки тестов) поведение прежнее."""
    flow = ManualOperationFlow(
        scale_state=lambda: ScaleState(status=ScaleStatus.OK, weight_kg=5000.0, stable=True),
        manual_allowed=lambda: True,
        storage=AgentStorage(":memory:"),
        cameras=[],
        photos_dir=tmp_path,
    )
    assert flow.ready() is True
