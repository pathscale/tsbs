# WorkTable TSBS target

This target runs the deterministic TSBS `cpu-only` dataset and query plans in
the same Rust process as WorkTable. It is intentionally not presented as a
durable client/server comparison: WorkTable is embedded and this first target
uses its in-memory row backend with no WAL or fsync.

Generate uncompressed input with the TSBS generators:

```bash
./bin/tsbs_generate_data --use-case cpu-only --format worktable \
  --seed 123 --scale 4000 \
  --timestamp-start 2016-01-01T00:00:00Z \
  --timestamp-end 2016-01-02T00:00:00Z \
  --log-interval 10s --file /tmp/worktable-data

./bin/tsbs_generate_queries --use-case cpu-only --format worktable \
  --seed 123 --scale 4000 \
  --timestamp-start 2016-01-01T00:00:00Z \
  --timestamp-end 2016-01-02T00:00:01Z \
  --queries 1000 --query-type single-groupby-1-1-1 \
  --file /tmp/worktable-queries-single-groupby-1-1-1

cargo run --release --manifest-path worktable/Cargo.toml -- \
  --data-file /tmp/worktable-data \
  --workers 32 \
  --query-file /tmp/worktable-queries-single-groupby-1-1-1
```

The data format is the same Influx Line Protocol emitted for QuestDB. Query
plans are JSON Lines rather than SQL or Go `gob`, allowing the Rust runner to
execute compiled host-language aggregations without including query generation
or SQL parsing in measured latency.

For a fair report, show WorkTable's memory/no-durability, in-process boundary,
row layout, and owned-row materialization beside QuestDB's WAL, network, and
columnar SQL semantics. The planned columnar WorkTable backend should be a
separate result mode over the identical files.

Run `scripts/full_cycle_minitest/full_cycle_minitest_worktable.sh` to exercise
all 16 `cpu-only` query types over a 24-hour, 32-host development dataset. Its
output is a functional smoke test, not a publishable performance run.
