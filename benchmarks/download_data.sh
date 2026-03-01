#!/usr/bin/env bash
# ──────────────────────────────────────────────────────────────────────
# download_data.sh — Download NYC Taxi trip parquet files for benchmarks
#
# Source: s3://altinity-clickhouse-data/nyc_taxi_rides/data/tripdata_parquet/
#         (public bucket, no credentials required)
#
# Usage:
#   ./benchmarks/download_data.sh            # downloads 5 files (~1.1 GB)
#   ./benchmarks/download_data.sh 3          # downloads first 3 files
#   ./benchmarks/download_data.sh all        # downloads ALL 96 files (~21 GB)
# ──────────────────────────────────────────────────────────────────────
set -euo pipefail

S3_BUCKET="s3://altinity-clickhouse-data/nyc_taxi_rides/data/tripdata_parquet"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DATA_DIR="${SCRIPT_DIR}/data"

# Files to download (5 months spanning different years for variety)
DEFAULT_FILES=(
    "data-200901.parquet"   # Jan 2009 — early dataset, ~222 MB
    "data-201206.parquet"   # Jun 2012 — mid-era dataset
    "data-201501.parquet"   # Jan 2015 — recent-era dataset
    "data-201706.parquet"   # Jun 2017 — late-era dataset
    "data-201901.parquet"   # Jan 2019 — latest-era dataset
)

# ── Parse arguments ──────────────────────────────────────────────────
COUNT="${1:-5}"

# ── Pre-flight checks ───────────────────────────────────────────────
if ! command -v aws &>/dev/null; then
    echo "❌  AWS CLI not found. Install it:"
    echo "    brew install awscli          # macOS"
    echo "    pip install awscli           # pip"
    echo "    https://aws.amazon.com/cli/  # other"
    exit 1
fi

mkdir -p "${DATA_DIR}"

# ── Download ─────────────────────────────────────────────────────────
download_file() {
    local file="$1"
    local dest="${DATA_DIR}/${file}"

    if [[ -f "${dest}" ]]; then
        echo "⏭  Already exists: ${file}"
        return 0
    fi

    echo "⬇  Downloading ${file} …"
    aws s3 cp "${S3_BUCKET}/${file}" "${dest}" --no-sign-request --no-progress
    echo "✅  Downloaded: ${file} ($(du -h "${dest}" | cut -f1))"
}

if [[ "${COUNT}" == "all" ]]; then
    echo "📦  Downloading ALL parquet files from the NYC taxi dataset …"
    echo ""
    # List all files and download each
    aws s3 ls "${S3_BUCKET}/" --no-sign-request \
        | awk '{print $4}' \
        | grep '\.parquet$' \
        | while read -r file; do
            download_file "${file}"
        done
else
    # Clamp to available default files
    if (( COUNT > ${#DEFAULT_FILES[@]} )); then
        COUNT=${#DEFAULT_FILES[@]}
    fi

    echo "📦  Downloading ${COUNT} NYC taxi parquet file(s) for benchmarks …"
    echo ""

    for (( i=0; i<COUNT; i++ )); do
        download_file "${DEFAULT_FILES[$i]}"
    done
fi

echo ""
echo "────────────────────────────────────────────────"
FILE_COUNT=$(find "${DATA_DIR}" -name '*.parquet' | wc -l | tr -d ' ')
TOTAL_SIZE=$(du -sh "${DATA_DIR}" 2>/dev/null | cut -f1)
echo "📁  Data directory : ${DATA_DIR}"
echo "📄  Files ready    : ${FILE_COUNT}"
echo "💾  Total size     : ${TOTAL_SIZE}"
echo "────────────────────────────────────────────────"
echo ""
echo "Run the benchmark:"
echo "  uv run python benchmarks/run_benchmark.py"
