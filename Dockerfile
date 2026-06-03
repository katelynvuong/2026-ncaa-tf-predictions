FROM python:3.12-slim

# libgomp1 is required by XGBoost on Linux (replaces the libomp needed on Mac)
RUN apt-get update && apt-get install -y \
    libgomp1 \
    curl \
    && rm -rf /var/lib/apt/lists/*

RUN pip install uv

WORKDIR /app

# Install dependencies first (layer-cached — only re-runs if pyproject.toml/uv.lock change)
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev

# Copy pipeline code
COPY pipeline/ ./pipeline/

# Create data directories — will be overridden by EFS mount in production
RUN mkdir -p \
    data/profiles \
    data/regional_athletes \
    data/flattened_dataframes \
    data/training \
    data/predictions \
    app/frontend/public/data

# Dagster stores run history here — on EFS in production for persistence
ENV DAGSTER_HOME=/app/.dagster_home
RUN mkdir -p $DAGSTER_HOME

EXPOSE 3000

CMD ["sh", "-c", "mkdir -p $DAGSTER_HOME && /app/.venv/bin/dagster dev -m pipeline --host 0.0.0.0 --port 3000"]
