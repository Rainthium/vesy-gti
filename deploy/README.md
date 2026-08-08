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
- `AIS_PHOTO_TOKEN` — `openssl rand -hex 32` (передать интеграторам АИС
  при переключении; до пилота нужен только имитатору `tools/ais_client.py`)
- `V1_USERNAME`/`V1_PASSWORD` — оставить `admin`/`admin` (АИС «СВХ» шлёт
  именно их; менять только по согласованию с разработчиками АИС)
- `AIS_ALLOWED_IPS` — IP серверов АИС через запятую (на пилоте можно
  оставить пустым — тогда фото защищены только токеном)

Центр запускается с `CENTER_ENV=production` и **откажется стартовать**,
если секреты не заданы или совпадают с dev-дефолтами.

## 3. Запуск

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
docker compose exec app uv run python -m tools.center_admin create-scale --site kyzyl-kyia --name 'Весы SCS-80' --driver cas22 --legacy-ip 192.168.150.185 --legacy-port 8087 --legacy-autoscale 2
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
cd vesy-gti && git pull
cd deploy && docker compose up -d --build   # migrate применит новые миграции сам
```

Откат БД не предусмотрен (взвешивания неизменяемы) — откат версии
приложения: `git checkout <коммит>` и тот же `up -d --build`.
