"""Тесты паузы по учётке при неудачных входах в панель (center/web/login_throttle).

Чистая логика без БД и HTTP: окно, порог, пауза и её истечение, сброс
удачным входом, независимость логинов, ключ учёта, предел памяти.
Маршрут входа с паузой проверяется в tests/test_center_panel.py.
"""

from center.web.login_throttle import LoginThrottle, ThrottleRule


class Clock:
    def __init__(self) -> None:
        self.t = 1_000.0

    def now(self) -> float:
        return self.t

    def advance(self, seconds: float) -> None:
        self.t += seconds


def _make(rule: ThrottleRule | None = None) -> tuple[LoginThrottle, Clock]:
    clock = Clock()
    return LoginThrottle(rule, now=clock.now), clock


def _fail(throttle: LoginThrottle, login: str, times: int) -> float:
    """Учесть ``times`` неудач; вернуть паузу, начатую последней из них."""
    pause = 0.0
    for _ in range(times):
        pause = throttle.record_failure(login)
    return pause


class TestThreshold:
    def test_below_threshold_allowed(self) -> None:
        throttle, _ = _make()
        assert _fail(throttle, "dispatcher", 9) == 0.0
        assert throttle.retry_after("dispatcher") == 0.0
        assert throttle.failures("dispatcher") == 9

    def test_tenth_failure_starts_pause(self) -> None:
        throttle, _ = _make()
        assert _fail(throttle, "dispatcher", 10) == 60.0
        assert throttle.retry_after("dispatcher") == 60.0

    def test_pause_counts_down_and_expires(self) -> None:
        throttle, clock = _make()
        _fail(throttle, "dispatcher", 10)
        clock.advance(59)
        assert throttle.retry_after("dispatcher") == 1.0
        clock.advance(1)
        assert throttle.retry_after("dispatcher") == 0.0
        assert throttle.retry_after("dispatcher") == 0.0, "истёкшая пауза не возвращается"

    def test_failure_after_pause_within_window_pauses_again(self) -> None:
        """Десять неудач ещё в окне: каждая следующая неудача после паузы —
        снова минута (перебор ограничен одной попыткой в минуту, пока не
        пройдёт 10 минут тишины)."""
        throttle, clock = _make()
        _fail(throttle, "dispatcher", 10)
        clock.advance(60)
        assert throttle.retry_after("dispatcher") == 0.0
        assert throttle.record_failure("dispatcher") == 60.0

    def test_custom_rule(self) -> None:
        throttle, clock = _make(ThrottleRule(max_failures=3, window_s=30, pause_s=5))
        assert _fail(throttle, "x", 2) == 0.0
        assert throttle.record_failure("x") == 5.0
        clock.advance(5)
        assert throttle.retry_after("x") == 0.0


class TestWindow:
    def test_failures_older_than_window_are_forgotten(self) -> None:
        """Девять неудач, 10 минут тишины, ещё девять — паузы нет."""
        throttle, clock = _make()
        _fail(throttle, "dispatcher", 9)
        clock.advance(600)
        assert throttle.failures("dispatcher") == 0
        assert _fail(throttle, "dispatcher", 9) == 0.0
        assert throttle.retry_after("dispatcher") == 0.0

    def test_sliding_window(self) -> None:
        """Пять неудач, через 9 минут ещё пять — пауза (все десять в окне);
        но если первые пять вышли за окно, пятью свежими не наберётся."""
        throttle, clock = _make()
        _fail(throttle, "a", 5)
        clock.advance(540)
        assert _fail(throttle, "a", 5) == 60.0

        _fail(throttle, "b", 5)
        clock.advance(601)
        assert _fail(throttle, "b", 5) == 0.0


class TestReset:
    def test_success_resets_counter(self) -> None:
        throttle, _ = _make()
        _fail(throttle, "dispatcher", 9)
        throttle.reset("dispatcher")
        assert throttle.failures("dispatcher") == 0
        assert _fail(throttle, "dispatcher", 9) == 0.0

    def test_reset_lifts_pause(self) -> None:
        throttle, _ = _make()
        _fail(throttle, "dispatcher", 10)
        throttle.reset("dispatcher")
        assert throttle.retry_after("dispatcher") == 0.0

    def test_reset_unknown_login_is_noop(self) -> None:
        throttle, _ = _make()
        throttle.reset("nobody")
        assert throttle.retry_after("nobody") == 0.0


class TestIsolation:
    def test_logins_are_independent(self) -> None:
        throttle, _ = _make()
        _fail(throttle, "alice", 10)
        assert throttle.retry_after("alice") == 60.0
        assert throttle.retry_after("bob") == 0.0
        assert _fail(throttle, "bob", 9) == 0.0

    def test_key_is_stripped_login(self) -> None:
        """Ключ — логин без краёв, как его видит verify_user; регистр
        различается, как и в БД."""
        throttle, _ = _make()
        _fail(throttle, "  dispatcher ", 10)
        assert throttle.retry_after("dispatcher") == 60.0
        assert throttle.retry_after("Dispatcher") == 0.0

    def test_unknown_logins_counted_like_real_ones(self) -> None:
        """Модулю всё равно, есть ли учётка: пауза наступает одинаково."""
        throttle, _ = _make()
        assert _fail(throttle, "no-such-user", 10) == 60.0

    def test_tracked_logins_are_bounded(self) -> None:
        """Перебор случайных логинов не растит память: при переполнении
        вычищаются остывшие записи, а если все в окне — самые давние;
        действующая пауза при этом остаётся."""
        throttle, clock = _make()
        limit = 10_000
        _fail(throttle, "paused", 10)
        for i in range(limit - 1):
            throttle.record_failure(f"junk-{i}")
        assert len(throttle._failures) == limit
        clock.advance(30)  # пауза ещё действует, все неудачи — в окне
        throttle.record_failure("fresh-1")
        assert len(throttle._failures) == limit, "предел жёсткий"
        assert "paused" not in throttle._failures, "самая давняя запись выброшена"
        assert throttle.retry_after("paused") == 30.0, "но пауза ещё действует"
        clock.advance(600)  # всё остыло: и неудачи вне окна, и пауза истекла
        throttle.record_failure("fresh-2")
        assert set(throttle._failures) == {"fresh-2"}
        assert throttle._paused_until == {}

    def test_sweep_keeps_active_pause(self) -> None:
        """Чистка при переполнении забывает неудачи вне окна, но действующую
        паузу логина не снимает."""
        throttle, clock = _make(ThrottleRule(max_failures=2, window_s=10, pause_s=3600))
        _fail(throttle, "paused", 2)
        for i in range(10_000):
            throttle.record_failure(f"junk-{i}")
        clock.advance(100)
        throttle.record_failure("fresh")
        assert "paused" not in throttle._failures, "неудачи вне окна забыты"
        assert throttle.retry_after("paused") > 0, "но пауза ещё действует"
