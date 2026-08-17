"""Схемы нативного API v2 для АИС «СВХ» (docs/contracts/ais-api-v2.md, 1.0).

Валидация тела команды и обратного вызова. Ошибки валидации API отдаёт
своим телом ``{code: ERR_VALIDATION, message, details}`` (раздел 4.3
контракта), поэтому разбор здесь ручной, а не через FastAPI-422.
"""

import re
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator, model_validator

from shared.enums import Operation

# номер документа АИС: префикс операции + 9 цифр (WEI000094176 / TAR000012206)
AIS_REF_RE = re.compile(r"^(WEI|TAR)\d{9}$")
AIS_REF_PREFIX = {Operation.WEIGHING: "WEI", Operation.TARING: "TAR"}
# «Специальный идентификатор СВХ» — строка с ведущими нулями («0014»);
# формально допускаем буквы/дефис на случай других справочников
AIS_OBJECT_RE = re.compile(r"^[A-Za-z0-9-]{1,16}$")

VEHICLE_MAX = 32
OPERATOR_MAX = 200


def normalize_vehicle(value: str | None) -> str | None:
    """Номер ТС: верхний регистр, без пробелов по краям; пусто → None."""
    return (value or "").strip().upper() or None


def check_ais_ref(ais_ref: str, operation: Operation) -> str | None:
    """Текст ошибки, если номер документа АИС не подходит операции; иначе None."""
    if not AIS_REF_RE.match(ais_ref):
        return "ais_ref: ожидается номер документа АИС вида WEI000094176 / TAR000012206"
    if not ais_ref.startswith(AIS_REF_PREFIX[operation]):
        return (
            f"ais_ref: префикс номера не соответствует операции "
            f"({operation.value} → {AIS_REF_PREFIX[operation]}…)"
        )
    return None


class WeighV2Request(BaseModel):
    """Команда взвешивания/тарирования (контракт v2, раздел 4.2)."""

    model_config = ConfigDict(extra="ignore")

    ais_ref: str = Field(min_length=4, max_length=32)
    ais_object: str = Field(min_length=1, max_length=16)
    scale_no: int = Field(default=1, ge=1, le=999)
    operation: Operation
    vehicle_number: str = Field(min_length=1, max_length=VEHICLE_MAX)
    trailer_number: str | None = Field(default=None, max_length=VEHICLE_MAX)
    operator: str = Field(min_length=1, max_length=OPERATOR_MAX)

    @field_validator("ais_ref", "ais_object", "vehicle_number", "operator", mode="before")
    @classmethod
    def _strip(cls, value: object) -> object:
        return value.strip() if isinstance(value, str) else value

    @field_validator("trailer_number", mode="before")
    @classmethod
    def _empty_trailer_is_none(cls, value: object) -> object:
        if isinstance(value, str) and not value.strip():
            return None
        return value

    @field_validator("ais_object")
    @classmethod
    def _ais_object_format(cls, value: str) -> str:
        if not AIS_OBJECT_RE.match(value):
            raise ValueError("ожидается идентификатор СВХ из справочника АИС (например, 0014)")
        return value

    @field_validator("operator")
    @classmethod
    def _collapse_operator(cls, value: str) -> str:
        collapsed = " ".join(value.split())
        if not collapsed:
            raise ValueError("ФИО оператора не может быть пустым")
        return collapsed

    @model_validator(mode="after")
    def _ais_ref_matches_operation(self) -> "WeighV2Request":
        error = check_ais_ref(self.ais_ref, self.operation)
        if error is not None:
            raise ValueError(error)
        return self

    @property
    def vehicle(self) -> str:
        return normalize_vehicle(self.vehicle_number) or ""

    @property
    def trailer(self) -> str | None:
        return normalize_vehicle(self.trailer_number)


class AisRefLink(BaseModel):
    """Тело обратного вызова: номер документа АИС для офлайн-операции (7.5)."""

    model_config = ConfigDict(extra="ignore")

    ais_ref: str = Field(min_length=4, max_length=32)

    @field_validator("ais_ref", mode="before")
    @classmethod
    def _strip(cls, value: object) -> object:
        return value.strip() if isinstance(value, str) else value


def validation_details(exc: ValidationError) -> list[dict[str, Any]]:
    """Ошибки pydantic → список ``{field, error}`` для тела 422 (без внутренностей)."""
    details = []
    for err in exc.errors():
        location = ".".join(str(part) for part in err.get("loc", ()) if part != "__root__")
        message = str(err.get("msg", ""))
        # pydantic оборачивает ValueError валидаторов в "Value error, <текст>"
        message = message.removeprefix("Value error, ")
        details.append({"field": location or None, "error": message})
    return details
