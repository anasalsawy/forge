FROM python:3.11-slim

WORKDIR /app

COPY pyproject.toml alembic.ini ./
COPY alembic ./alembic
COPY src ./src

RUN pip install --no-cache-dir . "litellm>=1.82,<1.83"

ENV PYTHONPATH=/app/src

COPY docker/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

EXPOSE 8801
ENTRYPOINT ["/entrypoint.sh"]