# FastAPI + Postgres + Google Earth Engine Dev Stack

A full-stack sandbox for exploring agronomic analytics with FastAPI, PostgreSQL, Google Earth Engine (GEE), Apache Airflow, and a minimal Nginx frontend. Everything is wired to run inside a Docker/VS Code dev container so you get a reproducible Linux environment on macOS.

## Stack Overview
- **API (`app/`)** – FastAPI service exposing parcel ingestion plus Earth Engine NDVI/NDWI/EVI analytics (`main.py`, `ee_physical.py`).
- **Database (`db_migration/`)** – SQL schema for `parcels` + `sentinelresults`, helper shell to apply migrations, and a PostgreSQL dump for bootstrapping data.
- **Data pipeline (`airflow/`)** – Airflow DAG (`gee_incremental_etl.py`) that calls the batch Earth Engine processor on a schedule.
- **Frontend (`frontend/`)** – Static Nginx site for lightweight visualization or manual testing.
- **Dev tooling (`.devcontainer/`)** – VS Code dev container definition, compose stack, and provision scripts.

## Repository Layout
```
app/                 FastAPI code, ETL helpers, requirements
frontend/            Nginx static site
airflow/             DAGs, plugin scaffold, extra requirements
config/              Hypercorn + misc Codex config
db_migration/       SQL schema, pg dump, apply script
.devcontainer/       Dockerfile, compose.yml, post-create automation
geozona_*.csv       Source CSV used by the migrate script
```

## Prerequisites
- Docker Desktop 4.24+ (with the Docker Compose plugin).
- VS Code **Dev Containers** extension (recommended) or pure Docker Compose CLI.
- Python 3.12 locally (only if you plan to run scripts outside the container).
- Google Cloud SDK (`gcloud`) + Google Earth Engine access (see next section).

## Google Earth Engine & Cloud Credentials
The FastAPI app and Airflow DAG talk to Earth Engine using the default Google Cloud application credentials that are bind mounted into the containers.

1. Install the [Google Cloud SDK](https://cloud.google.com/sdk/docs/install) locally.
2. Authenticate for Earth Engine + Cloud APIs:
   ```bash
   gcloud auth application-default login
   gcloud auth login            # ensures you can run earthengine commands
   earthengine authenticate --quiet
   ```
3. Confirm you have `~/.config/gcloud/application_default_credentials.json` – this is the file mounted into both the dev and Airflow containers at runtime.
4. (Optional) If you use a specific Cloud project for Earth Engine, set `EE_PROJECT=<your-project-id>` in `.env`. The code falls back to default credentials if the variable is absent.

> **Service accounts**: If you prefer a service account key, drop the JSON at `~/.config/gcloud/application_default_credentials.json` and ensure the account has Earth Engine + BigQuery/Cloud Storage permissions as needed.

## Environment Variables
- `.env` (already tracked) exposes `DB_URL`. Adjust if you rename the database, username, or password.
- Additional options the code inspects:
  - `EE_PROJECT` – forwarded into `ee.Initialize(project=...)` in `ee_physical.py`.
  - `GOOGLE_APPLICATION_CREDENTIALS` – already set inside the containers to `/root/.config/gcloud/application_default_credentials.json` (dev) / `/home/airflow/.config/gcloud/application_default_credentials.json` (Airflow).

## Bringing Up the Stack
### Option 1 – VS Code Dev Container (recommended)
1. Open the repository in VS Code.
2. Run “Dev Containers: Reopen in Container”.
3. First boot runs `.devcontainer/post-create.sh`, which creates `/workspace/.venv` and installs `app/requirements.txt`.
4. Once the container is ready, you can launch services from the integrated terminal (see the Run sections below).

### Option 2 – Docker Compose CLI
If you prefer the terminal, use the compose file that ships with the dev container definition:
```bash
cd .devcontainer
docker compose up -d        # launches dev, postgres, airflow, frontend
# tail logs if needed
docker compose logs -f dev
```
Volumes mounted by the dev service:
- `../:/workspace` – your repo synced into the container.
- `${HOME}/.config/gcloud:/root/.config/gcloud:ro` – injects Google Cloud application credentials.
- `/var/run/docker.sock` – lets the dev container run sibling containers (required for the migration helper script).
- `../app/requirements.txt:/app/requirements.txt` – shared requirements snapshot.

Shut everything down with `docker compose down` (run it from `.devcontainer/`).

## Database Schema & Migrations
All database assets live in `db_migration/`:

- `schema.sql` – idempotent DDL creating `parcels` (parcel metadata + geometry as WKT/GeoJSON) and `sentinelresults` (Earth Engine metrics keyed by `object_id` + acquisition date).
- `apply_schema.sh` – convenience script that waits for Postgres to become healthy and runs `schema.sql` inside the container.
- `app.dump` – PostgreSQL custom-format dump you can restore with `pg_restore`.

To apply the schema against the compose Postgres instance:
```bash
cd /Users/kamalgurbanov/Downloads/fastapi-pg-dev
COMPOSE_FILE=.devcontainer/compose.yml ./db_migration/apply_schema.sh
```
If the script is not executable, run `chmod +x db_migration/apply_schema.sh` once. The helper expects the Postgres service from `.devcontainer/compose.yml` to be up.

Restore the snapshot dump instead (optional):
```bash
COMPOSE_FILE=.devcontainer/compose.yml docker compose exec -T postgres \
  pg_restore -U app -d app /tmp/app.dump  # copy the file in with docker cp first
```

## Loading Source CSV Data
`app/migrate.py` migrates the provided `geozona_2025_202509161646.csv` into the `parcels` table, handling timestamp normalization and null geometry values.

Run it from inside the dev container so `/workspace` resolves correctly:
```bash
# inside dev container terminal
source /workspace/.venv/bin/activate
python app/migrate.py
```
The script expects the CSV at `/workspace/geozona_2025_202509161646.csv`; update the path if you introduce new datasets. Reruns will error on duplicate `object_id` values because the column is unique—truncate or adjust the SQL before replaying the migration.

## Running the FastAPI Application
```bash
# inside dev container
source /workspace/.venv/bin/activate
uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload
```
Key routes:
- `GET /` – simple health probe.
- `POST /addUser` – ingests a parcel record. Accepts both WKT and GeoJSON geometries (see `UserAddRequest` in `app/main.py`).
- `GET /ee/physical/get_graphs` and `POST /ee/physical/today` – exposed through the router in `app/ee_physical.py` and execute Earth Engine lookups.

When running locally (outside Docker), install dependencies with `pip install -r app/requirements.txt` and ensure Postgres + credentials are reachable.

## Earth Engine Batch Processing & Airflow
- `app/ee_physical.batch_process()` walks all rows with geometry, computes NDVI/NDWI/EVI for up to ~12 months of Sentinel-2 imagery, and upserts results into `sentinelresults`. You can run it ad hoc: `python app/ee_physical.py`.
- The Airflow DAG `airflow/dags/gee_incremental_etl.py` wraps that batch job on an hourly schedule. When the stack is up, visit `http://localhost:8080` (username/password `admin/admin`) to trigger or monitor runs.
- Airflow mounts the same `/workspace` tree and Google credentials directory, so the DAG executes the identical Python module used by the API.

## Frontend Preview
The lightweight frontend container serves `frontend/index.html` via Nginx on `http://localhost:8009`. Use it for quick manual checks or extend it to visualize metrics fetched from the API.

## Useful Commands & Troubleshooting
- Connect to Postgres from the dev container: `psql -h postgres -U app -d app` (password `app`).
- Inspect logs: `docker compose -f .devcontainer/compose.yml logs -f postgres`.
- Reset state: `docker compose -f .devcontainer/compose.yml down -v pgdata` (destroys the persistent database volume).
- Earth Engine credential issues usually manifest as `EEException: Please authorize access`. Re-run `gcloud auth application-default login` and ensure the JSON file is readable from the host.

Happy hacking!
