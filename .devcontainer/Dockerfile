FROM python:3.12-slim-bookworm

# System deps
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential curl git libpq-dev ca-certificates \
    && rm -rf /var/lib/apt/lists/*



WORKDIR /workspace

RUN curl -LsSf https://astral.sh/uv/install.sh | sh && \
    if [ -f /root/.cargo/bin/uv ]; then src=/root/.cargo/bin/uv; else src=/root/.local/bin/uv; fi && \
    install -m 0755 -o root -g root "$src" /usr/local/bin/uv
RUN apt-get update \
 && apt-get install -y --no-install-recommends postgresql-client \
 && rm -rf /var/lib/apt/lists/*
RUN apt-get update \
	&& apt-get install -y --no-install-recommends docker.io \
	&& rm -rf /var/lib/apt/lists/*
COPY requirements.txt .


RUN pip install --no-cache-dir -r requirements.txt
SHELL ["/bin/bash", "-lc"]

CMD ["bash", "-lc", "sleep infinity"]
