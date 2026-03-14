# ─────────────────────────────────────────────
# Stage 1: builder
# Installs uv and resolves all runtime deps into
# a self-contained virtual environment (.venv/).
# Nothing from this stage leaks into the runtime image.
# ─────────────────────────────────────────────
FROM python:3.12-slim AS builder

# Install uv (fast resolver/installer, replaces pip for dep installation)
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

WORKDIR /app

# Copy only the files uv needs to resolve and install dependencies.
# Doing this before copying src/ lets Docker cache the layer when only
# source code changes (deps are unchanged).
COPY pyproject.toml uv.lock ./

# Install runtime dependencies only (no dev extras) into .venv/.
# --frozen        → fail if lockfile is out of sync (reproducible builds)
# --no-install-project → skip installing the project itself; we copy src/ manually
# --no-dev        → exclude [dependency-groups] dev
ENV UV_PROJECT_ENVIRONMENT=/app/.venv
RUN uv sync --frozen --no-dev --no-install-project

# ─────────────────────────────────────────────
# Stage 2: runtime
# Minimal image with Python, the pre-built venv,
# and the application source. No uv, no build tools.
# ─────────────────────────────────────────────
FROM python:3.12-slim AS runtime

WORKDIR /app

# Copy the populated virtual environment from the builder stage.
COPY --from=builder /app/.venv /app/.venv

# Copy the application source package.
COPY src/ ./src/

# Put the venv on PATH so `python` resolves to the venv interpreter
# and all installed packages are available.
ENV PATH="/app/.venv/bin:$PATH"
ENV PYTHONPATH="/app"

# Default entry point mirrors `uv run -m src`.
# All CLI flags are passed through as CMD arguments.
ENTRYPOINT ["python", "-m", "src"]
