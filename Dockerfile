FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    UV_PROJECT_ENVIRONMENT=/opt/venv

WORKDIR /app

RUN apt-get update \
 && apt-get install -y --no-install-recommends git \
 && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir uv

COPY pyproject.toml uv.lock README.md /app/
COPY src /app/src

RUN uv sync --frozen --no-dev

RUN /opt/venv/bin/python -c "import fastapi; import uvicorn; print('ok')"

COPY . /app

WORKDIR /app/src/civil_unrest_correlation_analysis

EXPOSE 8000

CMD ["/opt/venv/bin/python", "-m", "uvicorn", "civil_unrest_correlation_analysis.main:app", "--host", "0.0.0.0", "--port", "8000"]

