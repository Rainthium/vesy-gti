"""Тесты моделей сообщений агент↔центр и перечислений."""

import json
from datetime import UTC, datetime
from uuid import uuid4

import pytest
from pydantic import ValidationError

from shared import (
    CameraRole,
    EquipmentStatus,
    ErrorCode,
    Heartbeat,
    Hello,
    OfflineSync,
    Operation,
    ScaleStatus,
    WeighingRecord,
    WeighingSource,
    WeighRequest,
    parse_agent_message,
    parse_center_message,
)
from shared.messages import AgentOperatorInfo, OperatorsReport


class TestEnums:
    """Строковые значения перечислений — часть внешних контрактов."""

    def test_error_codes_match_contract(self) -> None:
        # Точные строки из architecture §4.1 — менять нельзя
        expected = {
            "OK",
            "ERR_AGENT_OFFLINE",
            "ERR_SCALE_OFFLINE",
            "ERR_NOT_ZERO",
            "ERR_VEHICLE_TIMEOUT",
            "ERR_UNSTABLE",
            "ERR_CAMERA",
            "ERR_BUSY",
            "ERR_INTERNAL",
        }
        assert {code.value for code in ErrorCode} == expected

    def test_operation_values(self) -> None:
        assert Operation.WEIGHING.value == "weighing"
        assert Operation.TARING.value == "taring"

    def test_source_values(self) -> None:
        # source=local_offline — правило проекта №3
        assert WeighingSource.LOCAL_OFFLINE.value == "local_offline"
        assert WeighingSource.AIS.value == "ais"

    def test_enums_serialize_as_plain_strings(self) -> None:
        # В JSON перечисление должно уходить голой строкой, не объектом
        assert json.dumps(Operation.WEIGHING) == '"weighing"'


class TestAgentMessages:
    """Разбор сообщений агент → центр по дискриминатору type."""

    def _equipment(self) -> EquipmentStatus:
        return EquipmentStatus(
            scale_status=ScaleStatus.OK,
            last_packet_at=datetime.now(UTC),
            current_weight=1460.0,
            stable=True,
        )

    def test_hello_roundtrip(self) -> None:
        msg = Hello(
            agent_id="kyzyl-kyia-1", version="0.1.0", driver="cas22", equipment=self._equipment()
        )
        parsed = parse_agent_message(msg.model_dump_json())
        assert isinstance(parsed, Hello)
        assert parsed.agent_id == "kyzyl-kyia-1"
        assert parsed.protocol_version == 1

    def test_heartbeat_roundtrip(self) -> None:
        msg = Heartbeat(agent_id="a1", sent_at=datetime.now(UTC), equipment=self._equipment())
        parsed = parse_agent_message(msg.model_dump_json())
        assert isinstance(parsed, Heartbeat)
        assert parsed.equipment.current_weight == 1460.0

    def test_offline_sync_roundtrip(self) -> None:
        record = WeighingRecord(
            uuid=uuid4(),
            operation=Operation.TARING,
            code=ErrorCode.OK,
            massa=12500.0,
            stable=True,
            weighed_at=datetime.now(UTC),
            vehicle_number="01KG123ABC",
            source=WeighingSource.LOCAL_OFFLINE,
            operator="operator1",
        )
        msg = OfflineSync(agent_id="a1", records=[record])
        parsed = parse_agent_message(msg.model_dump_json())
        assert isinstance(parsed, OfflineSync)
        assert parsed.records[0].source is WeighingSource.LOCAL_OFFLINE

    def test_operators_report_roundtrip_without_password_hashes(self) -> None:
        """operators_report — сообщение агента; хеша пароля в записи нет
        по построению модели (правило №7: секреты не гоняем без нужды)."""
        msg = OperatorsReport(
            agent_id="a1",
            records=[AgentOperatorInfo(login="local.op", from_center=False)],
        )
        parsed = parse_agent_message(msg.model_dump_json())
        assert isinstance(parsed, OperatorsReport)
        assert parsed.records[0].from_center is False
        assert parsed.records[0].is_active is True
        assert "pw_hash" not in AgentOperatorInfo.model_fields

    def test_unknown_type_rejected(self) -> None:
        with pytest.raises(ValidationError):
            parse_agent_message('{"type": "self_destruct"}')

    def test_center_message_not_valid_as_agent_message(self) -> None:
        # weigh_request идёт только центр → агент
        msg = WeighRequest(request_id=uuid4(), operation=Operation.WEIGHING)
        with pytest.raises(ValidationError):
            parse_agent_message(msg.model_dump_json())


class TestCenterMessages:
    """Разбор сообщений центр → агент."""

    def test_weigh_request_roundtrip(self) -> None:
        msg = WeighRequest(
            request_id=uuid4(),
            operation=Operation.WEIGHING,
            vehicle_number="01KG123ABC",
        )
        parsed = parse_center_message(msg.model_dump_json())
        assert isinstance(parsed, WeighRequest)
        assert parsed.operation is Operation.WEIGHING
        assert parsed.timeout_s is None  # тайм-аут по умолчанию — из конфига агента

    def test_camera_roles(self) -> None:
        assert {role.value for role in CameraRole} == {"front", "rear"}


class TestAisRefAndTareHint:
    """Агент 0.4.17: ais_ref в команде и записи, подсказка тары в команде —
    поля необязательные, старые сообщения без них разбираются как раньше."""

    def test_defaults_when_absent(self) -> None:
        from uuid import uuid4

        from shared.messages import WeighingRecord, WeighRequest, parse_center_message

        request = parse_center_message(
            WeighRequest(request_id=uuid4(), operation=Operation.WEIGHING).model_dump_json()
        )
        assert isinstance(request, WeighRequest)
        assert request.ais_ref is None and request.tare is None and request.tare_resolved is False
        record = WeighingRecord.model_validate(
            {"uuid": str(uuid4()), "operation": "weighing", "code": "OK", "source": "ais"}
        )
        assert record.ais_ref is None

    def test_roundtrip_with_hint(self) -> None:
        from datetime import UTC, datetime
        from uuid import uuid4

        from shared.messages import TareRecord, WeighRequest, parse_center_message

        hint = TareRecord(
            vehicle_number="01KG777AAA",
            trailer_number=None,
            tare_value=15300.0,
            tared_at=datetime(2026, 6, 12, 4, 21, tzinfo=UTC),
            weighing_uuid=uuid4(),
        )
        request = WeighRequest(
            request_id=uuid4(),
            operation=Operation.WEIGHING,
            ais_ref="WEI000094176",
            tare=hint,
            tare_resolved=True,
        )
        parsed = parse_center_message(request.model_dump_json())
        assert isinstance(parsed, WeighRequest)
        assert parsed.ais_ref == "WEI000094176"
        assert parsed.tare_resolved is True and parsed.tare == hint
