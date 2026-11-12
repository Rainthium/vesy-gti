"""Интеграция с таможенной системой (Таргет-центр ГТС).

Контракт — docs/contracts/customs-events-v1.md (версия 1.0 от 11.11.2026),
план реализации — docs/customs-integration-plan.md. Поток независим от АИС
«СВХ»: свой outbox (``customs_events``), свой документ, свой брокер.
"""
