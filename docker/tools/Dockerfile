# syntax=docker/dockerfile:1.7
FROM python:3.12-slim-bookworm
COPY --from=ghcr.io/astral-sh/uv:0.12.23 /uv /usr/local/bin/uv
ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy UV_PROJECT_ENVIRONMENT=/opt/venv PATH=/opt/venv/bin:$PATH \
    CONTRACTS_DIR=/app/contracts/clickstream
WORKDIR /app
COPY pyproject.toml uv.lock README.md LICENSE NOTICE ./
RUN uv sync --locked --no-dev --no-install-project
COPY src ./src
COPY contracts ./contracts
RUN uv sync --locked --no-dev --no-editable \
 && useradd --uid 10001 --no-create-home app && mkdir -p /app/runs && chown app /app/runs
USER app
ENTRYPOINT ["streamplat"]
