#!/bin/sh
set -e

echo "Waiting for MinIO to start..."
sleep 5

mc alias set local http://minio:9010 "$MINIO_USER" "$MINIO_PASSWORD"

# BRONZE (RAW), active + archive buckets
mc mb --ignore-existing local/events-bronze
mc mb --ignore-existing local/events-bronze-archive


# SILVER (cleaned layer), active + archive buckets
mc mb --ignore-existing local/events-silver
mc mb --ignore-existing local/events-silver-archive


# QUALITY (rejected Bronze records)
mc mb --ignore-existing local/events-quality-issues


# GOLD — Page Views, active + archive buckets
mc mb --ignore-existing local/events-gold-page-views
mc mb --ignore-existing local/events-gold-page-views-archive


# GOLD — Product Events, active + archive buckets
mc mb --ignore-existing local/events-gold-product-events
mc mb --ignore-existing local/events-gold-product-events-archive

echo "Buckets created!"
