FROM python:3.13-slim
ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1 PIP_NO_CACHE_DIR=1 PIP_DISABLE_PIP_VERSION_CHECK=1
RUN pip install sanic==25.12.1 msgspec==0.22.0 "redis[hiredis]==8.1.0" psqlpy==0.12.1 uvloop==0.23.0 \
    loguru==0.7.3 xxhash==4.0.1 cachebox==6.2.7 rnet==2.4.2 valkey-glide==2.5.3 \
    granian==2.8.4 py-spy==0.4.2
WORKDIR /app
COPY lt /app/lt
COPY bench /app/bench
