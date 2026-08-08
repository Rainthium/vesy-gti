# Образ центра весовой системы. Контекст сборки — корень репозитория:
#     docker build -f deploy/Dockerfile .
# (в боевом compose это уже прописано, руками собирать не нужно)
FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim

WORKDIR /app
ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy

# зависимости отдельным слоем — правки кода не пересобирают их
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev --no-install-project

COPY alembic.ini ./
COPY shared ./shared
COPY agent ./agent
COPY center ./center
COPY tools ./tools
RUN uv sync --frozen --no-dev

EXPOSE 8080
# --proxy-headers: реальный IP клиента из X-Forwarded-For (nginx);
# доверять можно любому прокси — порт приложения наружу не публикуется,
# до него дотягивается только nginx внутри сети compose
CMD ["uv", "run", "--no-sync", "uvicorn", "center.app:create_app", "--factory", \
     "--host", "0.0.0.0", "--port", "8080", \
     "--proxy-headers", "--forwarded-allow-ips", "*"]
