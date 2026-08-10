#!/usr/bin/env bash
# Доставка центра на ВМ БЕЗ сборки на ней (на ВМ сборка слишком долгая):
# образ собирается на рабочей машине под linux/amd64, уезжает архивом
# по SSH и загружается в докер ВМ; код деплой-каталога — rsync'ом.
#
#   ./deploy/ship.sh vesy@192.168.140.70
#
# Запускать из корня репозитория (dev/). Требуется настроенный SSH-ключ.
set -euo pipefail

HOST="${1:?использование: ./deploy/ship.sh user@host}"
IMAGE="ves-center:latest"

echo "==> Сборка образа под linux/amd64…"
docker build --platform linux/amd64 -f deploy/Dockerfile -t "$IMAGE" .

echo "==> Синхронизация кода (deploy/, миграции не нужны — они в образе)…"
rsync -az --delete \
    --exclude 'deploy/.env' --exclude 'deploy/certs' --exclude 'deploy/releases' \
    --exclude '.venv' --exclude '.git' --exclude '__pycache__' \
    --exclude '.pytest_cache' --exclude '.mypy_cache' --exclude '.ruff_cache' \
    --exclude 'photos_data' --exclude '.claude' \
    ./ "$HOST":~/vesy-gti/

echo "==> Передача образа (сжатый поток по SSH)…"
docker save "$IMAGE" | gzip | ssh "$HOST" 'gunzip | docker load'

echo "==> Запуск на ВМ (без сборки)…"
ssh "$HOST" 'cd ~/vesy-gti/deploy && docker compose up -d --no-build'

echo "==> Состояние:"
ssh "$HOST" 'cd ~/vesy-gti/deploy && docker compose ps'
