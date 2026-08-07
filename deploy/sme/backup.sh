#!/usr/bin/env bash
# Postgres logical backup for the KChat Drive single-VM-pool deployment.
#
#   ./backup.sh
#
# Dumps the metadata database with pg_dump (run inside the postgres
# container so no client install is needed on the host), compresses
# it, and uploads it to Wasabi under
# s3://$WASABI_BUCKET/backups/postgres/. Wasabi is S3-compatible, so
# the standard aws CLI works against it with --endpoint-url. Old
# backups beyond the retention window are pruned.
#
# Ported from zk-object-fabric/deploy/sme/backup.sh, adapted for
# KChat Drive's container and bucket names.
#
# Requires: docker compose, aws CLI. Reads credentials from the
# sibling .env (the same file the stack boots from).
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ENV_FILE="${ENV_FILE:-$SCRIPT_DIR/../.env}"
COMPOSE_FILE="${COMPOSE_FILE:-$SCRIPT_DIR/docker-compose.production.yml}"
RETENTION_DAYS="${RETENTION_DAYS:-14}"

if [ ! -f "$ENV_FILE" ]; then
    echo "backup: $ENV_FILE not found; copy .env.example to .env first" >&2
    exit 1
fi

# Load .env without leaking values into the shell's xtrace.
set -a
# shellcheck disable=SC1090
. "$ENV_FILE"
set +a

: "${POSTGRES_USER:=postgres}"
: "${POSTGRES_DB:=kdrive}"
: "${WASABI_BUCKET:?WASABI_BUCKET must be set in .env}"
: "${WASABI_ENDPOINT:?WASABI_ENDPOINT must be set in .env}"
: "${WASABI_REGION:?WASABI_REGION must be set in .env}"
: "${WASABI_ACCESS_KEY:?WASABI_ACCESS_KEY must be set in .env}"
: "${WASABI_SECRET_KEY:?WASABI_SECRET_KEY must be set in .env}"

timestamp="$(date -u +%Y%m%dT%H%M%SZ)"
workdir="$(mktemp -d)"
trap 'rm -rf "$workdir"' EXIT
dump_file="$workdir/kdrive-$timestamp.sql.gz"

echo "backup: dumping database '$POSTGRES_DB' ..."
docker compose -f "$COMPOSE_FILE" exec -T postgres \
    pg_dump --clean --if-exists --no-owner --no-privileges \
    -U "$POSTGRES_USER" "$POSTGRES_DB" \
    | gzip -9 > "$dump_file"

size="$(wc -c < "$dump_file")"
if [ "$size" -eq 0 ]; then
    echo "backup: pg_dump produced an empty file; aborting" >&2
    exit 1
fi
echo "backup: wrote $dump_file ($size bytes)"

# Wasabi auth reuses the bucket credentials as AWS-style creds for
# this invocation only — exported into the environment, never
# written to disk.
export AWS_ACCESS_KEY_ID="$WASABI_ACCESS_KEY"
export AWS_SECRET_ACCESS_KEY="$WASABI_SECRET_KEY"
export AWS_DEFAULT_REGION="$WASABI_REGION"
endpoint="https://$WASABI_ENDPOINT"
key="backups/postgres/kdrive-$timestamp.sql.gz"

echo "backup: uploading to s3://$WASABI_BUCKET/$key ..."
aws --endpoint-url "$endpoint" s3 cp "$dump_file" "s3://$WASABI_BUCKET/$key"

# Sync WAL archive segments to Wasabi for PITR. The postgres
# container archives WAL files to the postgres-wal-archive volume
# via archive_command=cp %p /var/lib/postgresql/wal-archive/%f.
# We sync them to Wasabi so PITR is possible from the nightly base
# backup + WAL replay.
wal_dir="/var/lib/postgresql/wal-archive"
wal_count=$(docker compose -f "$COMPOSE_FILE" exec -T postgres \
    sh -c "ls '$wal_dir' 2>/dev/null | wc -l" 2>/dev/null | tr -d '[:space:]')
if [ "$wal_count" -gt 0 ] 2>/dev/null; then
    echo "backup: syncing $wal_count WAL segments to Wasabi ..."
    docker compose -f "$COMPOSE_FILE" exec -T postgres \
        sh -c "tar -cf - -C '$wal_dir' ." \
        | aws --endpoint-url "$endpoint" s3 cp - \
            "s3://$WASABI_BUCKET/backups/postgres/wal-$timestamp.tar" \
            --expected-size=$((wal_count * 67108864))
    echo "backup: WAL sync complete."
else
    echo "backup: no WAL segments to sync."
fi

echo "backup: pruning backups older than ${RETENTION_DAYS}d ..."
# Portable cutoff computation: subtract RETENTION_DAYS*86400 seconds
# from the current epoch and format as ISO-8601 UTC. This avoids
# GNU date (-d "N days ago") vs BSD date (-v-Nd) vs busybox
# incompatibilities.
cutoff_epoch=$(( $(date -u +%s) - RETENTION_DAYS * 86400 ))
cutoff="$(date -u -r "$cutoff_epoch" +%Y-%m-%dT%H:%M:%SZ 2>/dev/null \
    || date -u -d "@$cutoff_epoch" +%Y-%m-%dT%H:%M:%SZ)"
aws --endpoint-url "$endpoint" s3api list-objects-v2 \
    --bucket "$WASABI_BUCKET" --prefix "backups/postgres/" \
    --query "Contents[?LastModified<='$cutoff'].Key" --output text 2>/dev/null \
    | tr '\t' '\n' \
    | while IFS= read -r old_key; do
        case "$old_key" in
            ""|None) continue ;;
        esac
        echo "backup: removing s3://$WASABI_BUCKET/$old_key"
        aws --endpoint-url "$endpoint" s3 rm "s3://$WASABI_BUCKET/$old_key"
      done

# Prune old WAL archives (keep only those newer than the oldest
# retained base backup).
echo "backup: done."
