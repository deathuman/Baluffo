# syntax=docker/dockerfile:1.7

FROM node:25.8-slim AS container-frontend

WORKDIR /app

COPY package.json package-lock.json ./
RUN npm ci --ignore-scripts

COPY admin.html jobs.html saved.html ./
COPY frontend ./frontend
COPY probes ./probes
COPY scripts/build_container_frontend.mjs ./scripts/build_container_frontend.mjs
RUN npm run build:container-frontend -- --out-dir /container-frontend

FROM python:3.13-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    BALUFFO_DATA_DIR=/data \
    BALUFFO_CONTAINER_HOST=0.0.0.0 \
    BALUFFO_CONTAINER_PORT=8080 \
    PLAYWRIGHT_BROWSERS_PATH=/ms-playwright

WORKDIR /app

ARG BALUFFO_CONTAINER_REQUIRE_SYNC_CONFIG=false

COPY requirements-lock.txt ./
RUN python -m pip install --no-cache-dir --upgrade pip \
    && python -m pip install --no-cache-dir -r requirements-lock.txt \
    && python -m playwright install --with-deps chromium \
    && chmod -R a+rX /ms-playwright

COPY . .
COPY --from=container-frontend /container-frontend ./.container-frontend

RUN --mount=type=secret,id=BALUFFO_SYNC_BUILD_APP_ID,required=false \
    --mount=type=secret,id=BALUFFO_SYNC_BUILD_INSTALLATION_ID,required=false \
    --mount=type=secret,id=BALUFFO_SYNC_BUILD_REPO,required=false \
    --mount=type=secret,id=BALUFFO_SYNC_BUILD_PRIVATE_KEY_PEM,required=false \
    --mount=type=secret,id=BALUFFO_SYNC_BUILD_BRANCH,required=false \
    --mount=type=secret,id=BALUFFO_SYNC_BUILD_PATH,required=false \
    --mount=type=secret,id=BALUFFO_SYNC_BUILD_ALLOWED_REPO,required=false \
    --mount=type=secret,id=BALUFFO_SYNC_BUILD_ALLOWED_BRANCH,required=false \
    --mount=type=secret,id=BALUFFO_SYNC_BUILD_ALLOWED_PATH_PREFIX,required=false \
    --mount=type=secret,id=BALUFFO_SYNC_BUILD_KEY_DERIVATION,required=false \
    --mount=type=secret,id=BALUFFO_SYNC_BUILD_PASSPHRASE_ENV,required=false \
    --mount=type=secret,id=BALUFFO_SYNC_BUILD_EMBEDDED_KEY_HINT,required=false \
    --mount=type=secret,id=BALUFFO_SYNC_BUILD_EMBEDDED_KEY_VERSION,required=false \
    --mount=type=secret,id=BALUFFO_SYNC_BUILD_KEY_SALT,required=false \
    if [ "$BALUFFO_CONTAINER_REQUIRE_SYNC_CONFIG" = "true" ]; then \
      python scripts/build_container_sync_config.py --require; \
    else \
      python scripts/build_container_sync_config.py; \
    fi

RUN groupadd --gid 1000 baluffo \
    && useradd --uid 1000 --gid baluffo --create-home --shell /usr/sbin/nologin baluffo \
    && mkdir -p /data \
    && chown -R baluffo:baluffo /data

EXPOSE 8080
VOLUME ["/data"]

HEALTHCHECK --interval=30s --timeout=5s --start-period=30s --retries=3 \
  CMD python -c "from urllib.request import urlopen; import sys; sys.exit(0 if urlopen('http://127.0.0.1:8080/ops/health', timeout=3).status == 200 else 1)"

CMD ["python", "-m", "src.container_entrypoint", "--host", "0.0.0.0", "--port", "8080", "--data-dir", "/data", "--log-format", "jsonl"]
