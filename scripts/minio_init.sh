#!/bin/sh
# Membuat bucket Data Lake + kebijakan retensi. Idempotent (aman dijalankan ulang).
set -e
export MC_CONFIG_DIR=/tmp/mc
export PATH=/opt/bitnami/minio-client/bin:$PATH
mc alias set lake http://minio:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD"
for b in orbit-raw orbit-clean orbit-curated orbit-backup; do
  mc mb --ignore-existing lake/$b
done
# Raw zone immutable-ish: versioning aktif agar objek yang tertimpa tetap bisa dipulihkan
mc version enable lake/orbit-raw
mc version enable lake/orbit-backup
# Retensi: backup database dihapus otomatis setelah 30 hari
mc ilm rule add --expire-days 30 --prefix "db/" lake/orbit-backup 2>/dev/null || true
# Service account khusus pipeline (least privilege: hanya bucket orbit-*)
cat > /tmp/orbit-policy.json <<'POL'
{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":["s3:*"],
 "Resource":["arn:aws:s3:::orbit-*","arn:aws:s3:::orbit-*/*"]}]}
POL
mc admin policy create lake orbit-pipeline /tmp/orbit-policy.json 2>/dev/null || true
mc admin user add lake "$MINIO_PIPELINE_USER" "$MINIO_PIPELINE_PASSWORD" 2>/dev/null || true
mc admin policy attach lake orbit-pipeline --user "$MINIO_PIPELINE_USER" 2>/dev/null || true
echo "MinIO buckets ready"
mc ls lake
