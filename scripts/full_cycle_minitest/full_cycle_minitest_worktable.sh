#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
OUTPUT_DIR=${OUTPUT_DIR:-/tmp/tsbs-worktable-minitest}
SCALE=${SCALE:-32}
QUERIES=${QUERIES:-2}
WORKERS=${WORKERS:-4}

if (( SCALE < 32 )); then
  echo "SCALE must be at least 32 because cpu-max-all-32-24 selects 32 hosts" >&2
  exit 2
fi

mkdir -p "${OUTPUT_DIR}"

go build -o "${REPO_ROOT}/bin/tsbs_generate_data" "${REPO_ROOT}/cmd/tsbs_generate_data"
go build -o "${REPO_ROOT}/bin/tsbs_generate_queries" "${REPO_ROOT}/cmd/tsbs_generate_queries"
cargo build --release --manifest-path "${REPO_ROOT}/worktable/Cargo.toml"

data_file="${OUTPUT_DIR}/worktable-data"
"${REPO_ROOT}/bin/tsbs_generate_data" \
  --use-case cpu-only --format worktable --seed 123 --scale "${SCALE}" \
  --timestamp-start 2016-01-01T00:00:00Z \
  --timestamp-end 2016-01-02T00:00:00Z \
  --log-interval 10s --file "${data_file}"

query_types=(
  cpu-max-all-1
  cpu-max-all-8
  cpu-max-all-32-24
  single-groupby-1-1-1
  single-groupby-1-1-12
  single-groupby-1-8-1
  single-groupby-5-1-1
  single-groupby-5-1-12
  single-groupby-5-8-1
  double-groupby-1
  double-groupby-5
  double-groupby-all
  high-cpu-1
  high-cpu-all
  lastpoint
  groupby-orderby-limit
)

runner_args=(--data-file "${data_file}" --workers "${WORKERS}")
for query_type in "${query_types[@]}"; do
  query_file="${OUTPUT_DIR}/worktable-queries-${query_type}"
  "${REPO_ROOT}/bin/tsbs_generate_queries" \
    --use-case cpu-only --format worktable --seed 123 --scale "${SCALE}" \
    --timestamp-start 2016-01-01T00:00:00Z \
    --timestamp-end 2016-01-02T00:00:01Z \
    --queries "${QUERIES}" --query-type "${query_type}" \
    --file "${query_file}"
  runner_args+=(--query-file "${query_file}")
done

"${REPO_ROOT}/worktable/target/release/tsbs_run_worktable" "${runner_args[@]}"
