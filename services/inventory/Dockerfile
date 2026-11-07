FROM python:3.14-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    POETRY_VIRTUALENVS_CREATE=false \
    POETRY_NO_INTERACTION=1

WORKDIR /app

# Install Poetry
RUN pip install --no-cache-dir "poetry==2.4.3"

# Install dependencies
COPY pyproject.toml poetry.lock ./

RUN poetry install --only main --no-root

# Create non-root user
RUN groupadd --system app \
    && useradd --system \
        --gid app \
        --home-dir /app \
        --shell /usr/sbin/nologin \
        app

# Copy application files
COPY --chown=app:app app ./app
COPY --chown=app:app migrations ./migrations
COPY --chown=app:app alembic.ini ./

# Run as non-root user
USER app

EXPOSE 8000

CMD ["sh", "-c", "alembic upgrade head && uvicorn app.main:app --host 0.0.0.0 --port 8000"]