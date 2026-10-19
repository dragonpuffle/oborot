FROM python:3.12-slim
COPY --from=ghcr.io/astral-sh/uv:0.13.1 /uv /bin/uv
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
ENV PATH="/app/.venv/bin:$PATH" UV_PYTHON_DOWNLOADS=0
WORKDIR /app
COPY pyproject.toml ./
COPY uv.lock ./
COPY common ./common
COPY inventory_api ./inventory_api
COPY analytics_worker ./analytics_worker
RUN uv sync --locked --no-dev --no-editable --no-cache
COPY alembic.ini ./
COPY migrations ./migrations
RUN useradd --create-home app
USER app
CMD ["python", "-m", "inventory_api"]
