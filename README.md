# FastAPI + Postgres Dev Container (Mac-ready)

This gives you a **Linux dev shell** inside VS Code with **FastAPI** and **PostgreSQL** ready to go.

## Quick Start (VS Code Dev Containers)
1. Install prerequisites (one-time):
   - Docker Desktop for Mac
   - VS Code + extension: *Dev Containers* (ms-vscode-remote.remote-containers)

2. Open this folder in VS Code and **Reopen in Container**:
   - VS Code → Command Palette → “Dev Containers: Reopen in Container”

3. First-time setup runs `.devcontainer/post-create.sh` which:
   - Creates a virtualenv at `/workspace/.venv`
   - Installs Python deps from `requirements.txt`

4. Run FastAPI (inside the dev terminal):
   ```bash
   source /workspace/.venv/bin/activate
   uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload
   ```

5. Visit the app:
   - http://localhost:8000
   - Docs: http://localhost:8000/docs

### Postgres details
- Host (from inside the container): `postgres`
- Host (from your Mac): `localhost`
- Port: `5432`
- DB: `app`
- User: `app`
- Password: `app`
- DSN: `postgresql+psycopg://app:app@postgres:5432/app`

### Useful commands
```bash
# (inside the dev container)
cat /etc/os-release       # confirm Linux
psql -h postgres -U app -d app   # password: app
python -c "import sqlalchemy; print(sqlalchemy.__version__)"
```

## Optional: Run without VS Code
```bash
cd .devcontainer
docker compose -f compose.yml up -d
docker compose -f compose.yml exec dev bash
# now you're inside Linux
```

## Notes
- Apple Silicon (M1/M2/M3) works with these images. If you ever need x86-only images, add `platform: linux/amd64` under the service in `compose.yml`.
