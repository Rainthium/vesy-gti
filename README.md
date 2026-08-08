# Единая весовая система ОАО «ГТИ»

Система управления автомобильными весами на 13 объектах ОАО «Государственная таможенная Инфраструктура»: центральный сервер + агенты на весовых компьютерах. Принимает команды взвешивания от АИС «СВХ», хранит журнал взвешиваний и фото, работает автономно при потере связи. Заменяет UniServer AUTO.

## Документация

| Файл | Что это |
|---|---|
| `CLAUDE.md` | Инструкции для Claude Code: правила проекта, конвенции, рабочий цикл |
| `PROGRESS.md` | План и ход разработки по этапам, вопросы к владельцу проекта |
| `docs/architecture.md` | Архитектура системы (главный документ) |
| `docs/decisions.md` | Журнал архитектурных решений |
| `docs/protocols/cas22.md` | Протокол весового индикатора CAS (проверен на реальном объекте) |
| `docs/contracts/` | Контракты API (АИС «СВХ», агент↔центр) |
| `../design/` | Макеты интерфейсов (14 экранов, превью в `превью/`) |
| `../docs/` | Исходные материалы: реестр весов, дизайн-система ГТИ, задание на дизайн, выгрузка настроек UniServer |

## Структура

```
dev/
├── agent/      # служба весового ПК: drivers/ (cas22), weighing/ (автомат цикла,
│               # автоматический режим по командам центра, ручной офлайн-режим),
│               # cameras/, web/ (интерфейс оператора), sync/ (SQLite, WS-клиент, фото)
├── center/     # центральный сервер: db/ (модели+alembic), agents_ws/ (hub+WS),
│               # api_v1/ (совместимый АИС), photos/, web/ (панель диспетчера)
├── shared/     # модели сообщений, перечисления, правило тары, пароли
├── tools/      # cas22_emulator, ais_client (имитатор АИС), center_admin (CLI),
│               # dev_operator_ui и seed_demo_center (демо-стенды)
├── deploy/     # боевое развёртывание центра: Dockerfile, compose, nginx, README
└── docs/       # документация разработки
```

Стек: Python 3.12, FastAPI, PostgreSQL (центр), SQLite (агент), HTMX/Jinja2, docker-compose.

## Запуск в разработке

```bash
uv sync                                # окружение (менеджер — uv)
docker compose up -d postgres          # dev-БД PostgreSQL на localhost:5443 (ves/ves)
uv run alembic upgrade head            # миграции
uv run pytest -q                       # все тесты (нужен postgres)
uv run ruff format . && uv run ruff check . && uv run mypy .

# демо-стенды:
uv run python -m tools.seed_demo_center            # демо-данные центра (пустая БД; вход demo/demo1234)
uv run uvicorn center.app:create_app --factory --port 8080   # центр: /panel, /api/v1, /agents/ws
uv run python -m tools.dev_operator_ui   # интерфейс оператора агента (operator/operator; автономный режим по умолчанию, --online изображает связь с центром)
uv run python -m tools.ais_client --vehicle 01KG777AAA       # имитатор запроса АИС

# администрирование центра (пользователи/объекты/весы/токены агентов):
uv run python -m tools.center_admin --help

# настоящий агент целиком (конфиг TOML — образец agent/config.example.toml):
uv run python -m agent.main --config /путь/к/config.toml
uv run python -m agent.main --config /путь/к/config.toml add-operator --login a.osmonov
```

Конфигурация центра — env: `DATABASE_URL`, `PANEL_SECRET`, `V1_USERNAME/V1_PASSWORD`,
`AIS_PHOTO_TOKEN`, `AIS_ALLOWED_IPS`, `PHOTOS_DIR` (у всех есть dev-дефолты;
с `CENTER_ENV=production` центр требует задать секреты явно).

## Развёртывание (ВМ Ubuntu)

Боевой запуск центра — `deploy/README.md`: docker-compose (app + postgres +
nginx), секреты в `deploy/.env`, миграции контейнером `migrate`, защита
таблиц от TRUNCATE, TLS, бэкапы.

## Владелец проекта

Игорь Петрухин, ОАО «Государственная таможенная Инфраструктура».
