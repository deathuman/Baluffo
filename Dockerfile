FROM python:3.13-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    BALUFFO_DATA_DIR=/data \
    BALUFFO_CONTAINER_HOST=0.0.0.0 \
    BALUFFO_CONTAINER_PORT=8080 \
    PLAYWRIGHT_BROWSERS_PATH=/ms-playwright

WORKDIR /app

COPY requirements-lock.txt ./
RUN python -m pip install --no-cache-dir --upgrade pip \
    && python -m pip install --no-cache-dir -r requirements-lock.txt \
    && python -m playwright install --with-deps chromium \
    && chmod -R a+rX /ms-playwright

COPY . .

RUN groupadd --gid 1000 baluffo \
    && useradd --uid 1000 --gid baluffo --create-home --shell /usr/sbin/nologin baluffo \
    && mkdir -p /data \
    && chown -R baluffo:baluffo /data

USER baluffo

EXPOSE 8080
VOLUME ["/data"]

HEALTHCHECK --interval=30s --timeout=5s --start-period=30s --retries=3 \
  CMD python -c "from urllib.request import urlopen; import sys; sys.exit(0 if urlopen('http://127.0.0.1:8080/ops/health', timeout=3).status == 200 else 1)"

CMD ["python", "-m", "src.container_server", "--host", "0.0.0.0", "--port", "8080", "--data-dir", "/data", "--log-format", "jsonl"]
