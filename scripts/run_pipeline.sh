#!/usr/bin/env bash
# Runs the full medallion pipeline end to end: bronze -> silver -> gold.
# Requires BRONZE_BUCKET (and optionally the other vars in .env.example)
# to be set in the environment.
set -euo pipefail

python -m pipeline.bronze.ingest
python -m pipeline.silver.refine
python -m pipeline.gold.embed
