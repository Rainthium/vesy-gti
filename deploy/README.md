# Развёртывание центра на ВМ Ubuntu (vesy.gti.kg)

Центр — три контейнера docker-compose: `app` (FastAPI + uvicorn),
`postgres` (PostgreSQL 16), `nginx` (обратный прокси, единственный
порт наружу). Миграции применяет одноразовый контейнер `migrate`
при каждом запуске — `app` стартует только после их успеха.

Проверено на Ubuntu 22.04/24.04. Всё выполняется по SSH от пользователя
с правами sudo.

## 1. Docker

```bash
curl -fsSL https://get.docker.com | sudo sh
sudo usermod -aG docker $USER   # перелогиниться после этого
```

## 2. Код и секреты

```bash
git clone git@github.com:Rainthium/vesy-gti.git vesy-gti
cd vesy-gti/deploy
cp .env.example .env
```

Заполнить `deploy/.env` (файл в git не попадает — правило проекта №7):

- `POSTGRES_PASSWORD` — `openssl rand -hex 24`
- `PANEL_SECRET` — `openssl rand -hex 32`
- `AIS_PHOTO_TOKEN` — `openssl rand -hex 32`: сервисный токен АИС «СВХ» —
  и для команд нативного API v2 (`/api/v2/…`, контракт от 17.08.2026), и для
  фото (передать разработчикам АИС при подключении; до этого нужен только
  имитатору `tools/ais_client.py`)
- `V1_USERNAME`/`V1_PASSWORD` — оставить `admin`/`admin` (АИС «СВХ» шлёт
  именно их; менять только по согласованию с разработчиками АИС)
- `AIS_ALLOWED_IPS` — IP серверов АИС через запятую (боевой `192.168.140.150`,
  тестовый `192.168.6.116`; пусто — команды v2 и фото защищены только
  токеном). Те же адреса прописаны в `nginx.conf` (`location /api/v2/`)
- `TELEGRAM_BOT_TOKEN`/`TELEGRAM_CHAT_ID` — уведомления мониторинга
  (пусто — уведомления выключены, события копятся в журнале «События»
  панели). На пилоте: бот @vesy_gti_alerts_bot, КАНАЛ «Алерты весов
  ОАО "ГТИ"» (chat_id `-1004475491902`; бот — админ канала с правом
  постить). Канал вместо группы — сознательно: id канала стабилен,
  а группа при апгрейде в супергруппу МЕНЯЕТ chat_id (боевой урок
  14.08.2026 — sendMessage падал 400; причина теперь видна в логе).
  После смены chat_id в `.env` контейнер пересоздать:
  `docker compose up -d --force-recreate app` (restart env не перечитывает)

Центр запускается с `CENTER_ENV=production` и **откажется стартовать**,
если секреты не заданы или совпадают с dev-дефолтами (Telegram —
исключение: он необязательный).

## 3. Запуск

Сборка образа НА ВМ медленная — образ собирается на рабочей машине
и уезжает архивом по SSH (скрипт делает всё: сборка под linux/amd64,
rsync кода, передача образа, запуск):

```bash
./deploy/ship.sh vesy@192.168.140.70
```

Запасной вариант (сборка прямо на ВМ, если рабочей машины нет под рукой):

```bash
docker compose up -d --build
```

Проверка:

```bash
docker compose ps                                  # все healthy, migrate — exited(0)
curl -s http://localhost/healthz                   # {"status":"ok"}
docker compose logs app --tail 20
```

## 4. Защита таблиц от TRUNCATE (обязательно, один раз)

Строчные триггеры неизменяемости не ловят `TRUNCATE`, поэтому право
отзывается у рабочей роли (правило №2):

```bash
docker compose exec postgres psql -U ves -d ves -c 'REVOKE TRUNCATE ON TABLE weighings, weighing_photos FROM ves;'
```

## 5. Справочники и учётки

Всё через CLI администратора внутри контейнера (пароль спрашивается
интерактивно, токен агента печатается один раз — записать и вписать
в конфиг агента на весовом ПК):

```bash
docker compose exec app uv run python -m tools.center_admin create-user --login d.ivanov --full-name 'Иванов Д.' --role dispatcher
docker compose exec app uv run python -m tools.center_admin create-site --code kyzyl-kyia --name 'СВХ «Кызыл-Кыя»'
docker compose exec app uv run python -m tools.center_admin create-scale --site kyzyl-kyia --name 'Весы SCS-80' --driver cas22 --legacy-ip 192.168.158.20 --legacy-port 8087 --legacy-autoscale 2
docker compose exec app uv run python -m tools.center_admin create-agent --scale-id 1
docker compose exec app uv run python -m tools.center_admin list
```

`--legacy-ip/--legacy-port/--legacy-autoscale` — те адреса, которые АИС
сегодня шлёт в запросах v1 (маршрутизация на весы по ним, правило №1).

## 6. TLS (когда будет сертификат)

1. Положить `fullchain.pem` и `privkey.pem` в `deploy/certs/`.
2. В `nginx.conf` раскомментировать server-блок 443.
3. В `docker-compose.yml` открыть порт `443:443` и том `./certs`.
4. `docker compose up -d nginx`.

До сертификата всё ходит по 80 — для пилота внутри сети ГТИ допустимо,
для боевой работы с АИС включить TLS.

## 7. Резервные копии

БД (ежедневно, cron):

```bash
docker compose exec -T postgres pg_dump -U ves -Fc ves > /backup/ves-$(date +%F).dump
```

Фото — том `ves-center_photos`
(`/var/lib/docker/volumes/ves-center_photos/_data`), копировать
rsync'ом туда же, куда и дампы. Хранение фото — 5 лет (правило №2).

## 8. Обновление версии

```bash
# с рабочей машины (сборка локально, доставка образа по SSH):
git pull && ./deploy/ship.sh vesy@192.168.140.70   # migrate применит новые миграции сам
```

Если менялся `deploy/nginx.conf` — обязательно пересоздать nginx (rsync
подменяет инод, `nginx -s reload` перечитал бы старый файл):

```bash
ssh vesy@192.168.140.70 'cd ~/vesy-gti/deploy && docker compose up -d --force-recreate nginx'
```

Откат БД не предусмотрен (взвешивания неизменяемы) — откат версии
приложения: `git checkout <коммит>` и тот же `up -d --build`.

## 9. Автообновление агентов (без AnyDesk)

Центр раздаёт релизы агента из `~/vesy-gti/deploy/releases` (том
`/data/releases` контейнера, только чтение). Выкладка нового релиза
с рабочей машины:

```bash
# скачать архив релиза из GitHub (тег agent-vX.Y.Z) и положить на ВМ
curl -sL -o /tmp/ves-agent.zip https://github.com/Rainthium/vesy-gti/releases/download/agent-vX.Y.Z/ves-agent-X.Y.Z-win64.zip
ssh vesy@192.168.140.70 'mkdir -p ~/vesy-gti/deploy/releases'
scp /tmp/ves-agent.zip vesy@192.168.140.70:~/vesy-gti/deploy/releases/ves-agent-X.Y.Z-win64.zip
```

Дальше — в панели: «Объекты» → карточка весов → кнопка «Обновить до vX.Y.Z»
(видна администратору, когда версия агента отличается и агент в сети). Агент скачает
архив, сверит sha256, разложит app_new и перезапустит службу сам
(старая версия останется в app_old на весовом ПК для отката; лог —
C:\vesy-agent\logs\update.log). Успех виден по смене версии на дашборде
через ~минуту.

Если обновление не доехало, посмотреть журнал агента можно, не заходя на
объект: та же карточка весов → «Журнал агента» (центр запрашивает хвост
`logs\agent.log` по WS). Кнопка доступна администратору и работает с
агентами 0.4.5 и новее; каждый просмотр пишется в `audit_log`
(`action = agent_log_view`). Локальная копия того же хвоста — на самом
весовом ПК: интерфейс оператора → вкладка «Диагностика» (работает и без
связи с центром).

## 10. Роли в панели и видимость объектов

Администратор видит систему целиком; остальные пользователи — только тот
объект, к которому привязаны на экране «Пользователи» (пустая привязка =
видно всё, так заводятся диспетчеры головного офиса). Ограничение
считается по БД на каждый запрос, поэтому смена привязки, роли или
отключение учётки применяются немедленно, без перевхода. Проверять после
развёртывания стоит именно диспетчером объекта: под администратором
разграничение не видно.

Внизу экрана «Пользователи» — блок «Учётки на агентах»: что реально
лежит в базе каждого весового ПК (присылают агенты 0.4.14 и новее) —
реплика операторов центра плюс учётки, заведённые на месте вручную
(CLI add-operator); локальные помечены пилюлей «заведена на месте».
