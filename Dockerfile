# RelayQ — Multi-stage Docker build
#
# Stage 1: Build (install deps)
FROM python:3.12-slim AS builder

WORKDIR /app
COPY pyproject.toml setup.py ./
RUN pip install --no-cache-dir --user .

# Stage 2: Runtime
FROM python:3.12-slim AS runtime

# Non-root user (security best practice)
RUN groupadd -r relayq && useradd --no-log-init -r -g relayq relayq

WORKDIR /app
COPY --from=builder /root/.local /usr/local
COPY relayq/ relayq/
COPY api/ api/
COPY cli.py .

USER relayq

EXPOSE 8000

# Default: run the API server
CMD ["uvicorn", "api.main:app", "--host", "0.0.0.0", "--port", "8000", "--log-level", "info"]
