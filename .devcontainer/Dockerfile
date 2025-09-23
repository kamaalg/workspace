FROM python:3.11-slim

# System deps
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential curl git libpq-dev ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Create a non-root user (optional; VS Code works with root too)
# RUN useradd -ms /bin/bash vscode && chown -R vscode:vscode /workspace
# USER vscode

WORKDIR /workspace

# Pre-install uv for faster installs (optional); fallback to pip if needed
RUN curl -LsSf https://astral.sh/uv/install.sh | sh && \
    if [ -f /root/.cargo/bin/uv ]; then src=/root/.cargo/bin/uv; else src=/root/.local/bin/uv; fi && \
    install -m 0755 -o root -g root "$src" /usr/local/bin/uv

# Default shell
SHELL ["/bin/bash", "-lc"]

# Keep image lean; actual deps installed in post-create to use mounted code
CMD ["bash", "-lc", "sleep infinity"]
