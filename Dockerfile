# syntax=docker/dockerfile:1@sha256:4edf897a3ffa55b89f906fc8cc78afdb3f1834cc9c7083565e611a8a7d5fe99e
FROM ghcr.io/astral-sh/uv:python3.13-trixie-slim@sha256:331fbb158258608b9391e802305a9a2c9e42d689435c1d98706647122a122ab0

ENV UV_NO_DEV=1
ENV UV_NO_CACHE=1

WORKDIR /app

# Install git (needed for git dependencies) and g++ (needed to build pysubnettree, pulled in by eodhp-utils)
RUN apt-get update && apt-get install -y --no-install-recommends git g++ && rm -rf /var/lib/apt/lists/*

# Install dependencies
RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=uv.lock,target=uv.lock \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    uv sync --frozen --no-install-project

# Copy project files
COPY . /app

# Sync the project
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen

ENTRYPOINT ["uv", "run", "--no-sync", "python", "-m", "airbus_harvester", "harvest"]
