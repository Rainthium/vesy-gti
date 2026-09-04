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
from shared.messages import (
    AgentOperatorInfo,
    CycleSettings,
    OperatorsReport,
    PhotoCleanupRequest,
    PhotoCleanupResponse,
    ScaleSettingsPayload,
    UpdateStatus,
    supports_photo_cleanup,
)


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
            # лимит тары (контракт v2 4.4, дополнение 04.09.2026)
            "ERR_TARE_TOO_HEAVY",
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

    def test_update_status_stages(self) -> None:
        """Стадии автообновления (0.4.19): старый агент шлёт без stage —
        разбирается как started; новые стадии несут running_version."""
        legacy = parse_agent_message(
            '{"type": "update_status", "agent_id": "a", "version": "0.4.19", "ok": true}'
        )
        assert isinstance(legacy, UpdateStatus)
        assert legacy.stage == "started" and legacy.running_version is None
        rolled = UpdateStatus(
            agent_id="a",
            version="0.4.20",
            ok=False,
            error="откат на 0.4.19: нет связи с центром",
            stage="rolled_back",
            running_version="0.4.19",
        )
        parsed = parse_agent_message(rolled.model_dump_json())
        assert isinstance(parsed, UpdateStatus)
        assert parsed.stage == "rolled_back" and parsed.running_version == "0.4.19"
        with pytest.raises(ValidationError):
            UpdateStatus(agent_id="a", version="1", ok=True, stage="verified")  # type: ignore[arg-type]


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


class TestPhotoCleanupMessages:
    """Уборка локальных фото по команде центра (агент 0.4.25, решение Игоря
    02.09.2026): команда идёт только центр → агент, отчёт — только обратно."""

    def test_request_is_center_message_only(self) -> None:
        msg = PhotoCleanupRequest(request_id=uuid4())
        parsed = parse_center_message(msg.model_dump_json())
        assert isinstance(parsed, PhotoCleanupRequest)
        assert parsed.request_id == msg.request_id
        with pytest.raises(ValidationError):
            parse_agent_message(msg.model_dump_json())

    def test_response_roundtrip_and_defaults(self) -> None:
        msg = PhotoCleanupResponse(
            request_id=uuid4(),
            agent_id="a1",
            removed_files=12,
            freed_bytes=3_500_000,
            disk_free_mb=40_960,
        )
        parsed = parse_agent_message(msg.model_dump_json())
        assert isinstance(parsed, PhotoCleanupResponse)
        assert (parsed.removed_files, parsed.freed_bytes, parsed.disk_free_mb) == (
            12,
            3_500_000,
            40_960,
        )
        assert parsed.error is None
        bare = parse_agent_message(
            PhotoCleanupResponse(request_id=uuid4(), agent_id="a1").model_dump_json()
        )
        assert isinstance(bare, PhotoCleanupResponse)
        assert (bare.removed_files, bare.freed_bytes, bare.disk_free_mb) == (0, 0, None)
        with pytest.raises(ValidationError):
            parse_center_message(msg.model_dump_json())

    def test_retention_days_in_settings_payload(self) -> None:
        """0 — «не убирать» (это тоже управление), None — локальный конфиг;
        неизвестные поля будущих версий не ломают разбор старым агентом."""
        assert ScaleSettingsPayload(photo_retention_days=0).photo_retention_days == 0
        assert ScaleSettingsPayload().photo_retention_days is None
        future = ScaleSettingsPayload.model_validate_json(
            '{"photo_retention_days": 7, "unknown_future_field": 1}'
        )
        assert future.photo_retention_days == 7
        with pytest.raises(ValidationError):
            ScaleSettingsPayload(photo_retention_days=-1)
        with pytest.raises(ValidationError):
            ScaleSettingsPayload(photo_retention_days=3651)

    @pytest.mark.parametrize(
        ("version", "expected"),
        [("0.4.24", False), ("0.4.25", True), ("0.5.0", True), (None, False), ("мусор", False)],
    )
    def test_supports_photo_cleanup(self, version: str | None, expected: bool) -> None:
        assert supports_photo_cleanup(version) is expected


class TestManualAllowedField:
    """Ручной режим при связи с центром (0.4.28): None — не задано, bool — команда."""

    def test_default_is_none_and_bool_round_trips(self) -> None:
        assert ScaleSettingsPayload().manual_allowed is None
        assert ScaleSettingsPayload(manual_allowed=True).manual_allowed is True
        parsed = ScaleSettingsPayload.model_validate_json('{"manual_allowed": false}')
        assert parsed.manual_allowed is False


class TestMaxTareField:
    """CycleSettings.max_tare_kg (агент 0.4.29, решение Игоря 04.09.2026)."""

    def _cycle(self, **overrides: object) -> CycleSettings:
        fields: dict[str, object] = {
            "zero_threshold_kg": 200.0,
            "vehicle_threshold_kg": 500.0,
            "zero_timeout_s": 10.0,
            "vehicle_timeout_s": 90.0,
            "stable_duration_s": 5.0,
            "stable_timeout_s": 30.0,
            "no_data_timeout_s": 5.0,
        }
        fields.update(overrides)
        return CycleSettings.model_validate(fields)

    def test_default_for_old_snapshots(self) -> None:
        """Снимок настроек без поля (сохранён до 0.4.29) — лимит 25 т."""
        assert self._cycle().max_tare_kg == 25000.0

    def test_zero_allowed_negative_rejected(self) -> None:
        assert self._cycle(max_tare_kg=0).max_tare_kg == 0.0
        with pytest.raises(ValidationError):
            self._cycle(max_tare_kg=-1)

    def test_travels_in_scale_settings_payload(self) -> None:
        """Поле едет агенту в снимке настроек и переживает JSON."""
        payload = ScaleSettingsPayload(cycle=self._cycle(max_tare_kg=30000.0))
        restored = ScaleSettingsPayload.model_validate_json(payload.model_dump_json())
        assert restored.cycle is not None and restored.cycle.max_tare_kg == 30000.0
