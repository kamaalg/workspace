set -euo pipefail

cid=$(docker compose ps -q postgres)
if [ -z "$cid" ]; then
  echo "Postgres ishlemir. Bu komand ile start eliyin:  docker compose up -d postgres"
  exit 1
fi

docker compose exec postgres bash -lc \
 'until pg_isready -U ${POSTGRES_USER:-app} -d ${POSTGRES_DB:-app} -h localhost >/dev/null 2>&1; do sleep 1; done'

docker cp db/schema.sql "$cid":/tmp-schema.sql
docker compose exec -T postgres psql \
  -U "${POSTGRES_USER:-app}" \
  -d "${POSTGRES_DB:-app}" \
  -h localhost \
  -v "ON_ERROR_STOP=1" \
  -f /tmp-schema.sql

echo "✅ parcels + sentinelresults schema ensured (with FK)."
