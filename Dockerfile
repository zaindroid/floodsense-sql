FROM python:3.12-slim

# Coolify's in-container healthcheck needs one of these present.
RUN apt-get update && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /srv

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app

# Replaced on every sync; the build is identified by it.
ENV GIT_SHA=unknown BUILT_AT=unknown
ENV PORT=8080 \
    PYTHONUNBUFFERED=1

EXPOSE 8080
CMD ["python", "-m", "app"]
