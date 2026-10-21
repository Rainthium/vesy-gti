"""Пауза по учётке при неудачных входах в панель (второй рубеж после nginx).

Правило (решение 21.10.2026): после ``max_failures`` неудачных попыток под
одним логином за ``window_s`` секунд этот логин не принимается ``pause_s``
секунд — даже с верным паролем. Счётчик сбрасывается удачным входом;
неудачи старше окна в счёт не идут. Попытки во время паузы отвергаются до
проверки пароля (PBKDF2 на 200 000 итераций — дорого) и в счётчик не
попадают.

Учёт — по логину, не по адресу: все внешние пользователи приходят с адреса
корпоративного прокси, а как он передаёт адреса — вне нашего контроля
(``limit_req`` в nginx считает по ``X-Forwarded-For``, см. deploy/README
§8а). Несуществующие логины считаются так же, как существующие, чтобы по
поведению формы нельзя было узнать, есть ли учётка.

Состояние — в памяти процесса: центр работает одним воркером uvicorn,
таблицы в БД для этого не нужны; рестарт центра счётчики обнуляет — это
приемлемо. Часы инъецируются (``now`` → секунды, монотонные) ради тестов.
"""

import time
from collections import deque
from collections.abc import Callable
from dataclasses import dataclass

# жёсткий предел числа отслеживаемых логинов: перебор случайных логинов не
# должен растить память процесса; при переполнении сначала чистим
# «остывшие» записи, если не помогло — выбрасываем самые старые
_MAX_TRACKED_LOGINS = 10_000
# ключ усекается — длинный «логин» с формы не должен занимать память
_MAX_KEY_LEN = 200


@dataclass(frozen=True)
class ThrottleRule:
    """Параметры паузы: сколько неудач за какое окно и на сколько пауза."""

    max_failures: int = 10
    window_s: float = 600.0
    pause_s: float = 60.0


class LoginThrottle:
    """Счётчик неудачных входов по логину с паузой.

    Не потокобезопасен: вызывается из обработчиков одного цикла событий.
    Проверка паузы и учёт неудачи разделены проверкой пароля (``await``),
    поэтому пачка ОДНОВРЕМЕННЫХ попыток под одним логином проходит
    проверку вся, и пауза наступает после неё; последовательные попытки
    режутся точно по порогу. Одновременные режет nginx (``limit_req``,
    deploy/README §8а) — этот модуль рассчитан на последовательный перебор.
    """

    def __init__(
        self,
        rule: ThrottleRule | None = None,
        *,
        now: Callable[[], float] = time.monotonic,
    ) -> None:
        self.rule = rule or ThrottleRule()
        self._now = now
        # моменты неудач по логину — только те, что ещё внутри окна
        self._failures: dict[str, deque[float]] = {}
        # момент окончания паузы по логину
        self._paused_until: dict[str, float] = {}

    @staticmethod
    def key(login: str) -> str:
        """Ключ учёта — логин в том виде, в каком его проверяет verify_user."""
        return login.strip()[:_MAX_KEY_LEN]

    def retry_after(self, login: str) -> float:
        """Сколько секунд логину ещё ждать; 0 — попытку можно проверять."""
        key = self.key(login)
        until = self._paused_until.get(key)
        if until is None:
            return 0.0
        remaining = until - self._now()
        if remaining <= 0:
            del self._paused_until[key]
            return 0.0
        return remaining

    def record_failure(self, login: str) -> float:
        """Учесть неудачную попытку; вернуть длину паузы, если она началась
        этой попыткой, иначе 0."""
        key = self.key(login)
        now = self._now()
        if key not in self._failures and len(self._failures) >= _MAX_TRACKED_LOGINS:
            self._sweep(now)
            self._evict_oldest()
        stamps = self._failures.setdefault(key, deque())
        self._drop_expired(stamps, now)
        stamps.append(now)
        if len(stamps) < self.rule.max_failures:
            return 0.0
        self._paused_until[key] = now + self.rule.pause_s
        return self.rule.pause_s

    def failures(self, login: str) -> int:
        """Число неудач логина внутри окна (для логов и тестов)."""
        stamps = self._failures.get(self.key(login))
        if not stamps:
            return 0
        self._drop_expired(stamps, self._now())
        return len(stamps)

    def reset(self, login: str) -> None:
        """Удачный вход: забыть неудачи и паузу логина."""
        key = self.key(login)
        self._failures.pop(key, None)
        self._paused_until.pop(key, None)

    def _drop_expired(self, stamps: deque[float], now: float) -> None:
        horizon = now - self.rule.window_s
        while stamps and stamps[0] <= horizon:
            stamps.popleft()

    def _sweep(self, now: float) -> None:
        """Убрать логины без неудач в окне и истёкшие паузы."""
        for key in list(self._failures):
            self._drop_expired(self._failures[key], now)
            if not self._failures[key]:
                del self._failures[key]
        for key in list(self._paused_until):
            if self._paused_until[key] <= now:
                del self._paused_until[key]

    def _evict_oldest(self) -> None:
        """Если чистка не помогла — выбросить самые давние записи (порядок
        вставки dict), чтобы предел держался и при переборе быстрее окна."""
        for table in (self._failures, self._paused_until):
            while len(table) >= _MAX_TRACKED_LOGINS:
                del table[next(iter(table))]
