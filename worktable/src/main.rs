use std::collections::BTreeMap;
use std::fs::File;
use std::hint::black_box;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::Arc;
use std::sync::mpsc::sync_channel;
use std::thread;
use std::time::Instant;

use eyre::{Context, Result, bail, eyre};
use serde::{Deserialize, Serialize};
use worktable::prelude::*;
use worktable::worktable;

const CPU_METRICS: usize = 10;
type CpuMetrics = [i64; CPU_METRICS];

worktable!(
    name: TsbsCpu,
    columns: {
        id: u128 primary_key,
        host_id: u64,
        timestamp: i64,
        metrics: CpuMetrics,
    }
);

#[derive(Debug)]
struct Config {
    data_file: PathBuf,
    query_files: Vec<PathBuf>,
    burn_in: usize,
    max_queries: Option<usize>,
    workers: usize,
}

impl Config {
    fn from_args() -> Result<Self> {
        let mut data_file = None;
        let mut query_files = Vec::new();
        let mut burn_in = 0;
        let mut max_queries = None;
        let mut workers = 1;
        let mut args = std::env::args().skip(1);
        while let Some(flag) = args.next() {
            if flag == "--help" || flag == "-h" {
                println!(
                    "tsbs_run_worktable options:\n\
                     --data-file PATH       WorkTable/QuestDB ILP cpu-only data (required)\n\
                     --query-file PATH      WorkTable JSONL queries (repeatable, required)\n\
                     --workers N            concurrent load workers (default 1)\n\
                     --burn-in N            execute but omit first N queries per file\n\
                     --max-queries N        cap queries executed per file"
                );
                std::process::exit(0);
            }
            let value = args
                .next()
                .ok_or_else(|| eyre!("missing value for {flag}"))?;
            match flag.as_str() {
                "--data-file" => data_file = Some(value.into()),
                "--query-file" => query_files.push(value.into()),
                "--workers" => workers = parse(&flag, &value)?,
                "--burn-in" => burn_in = parse(&flag, &value)?,
                "--max-queries" => max_queries = Some(parse(&flag, &value)?),
                _ => bail!("unknown option: {flag}"),
            }
        }
        let data_file = data_file.ok_or_else(|| eyre!("--data-file is required"))?;
        if query_files.is_empty() {
            bail!("at least one --query-file is required");
        }
        if max_queries == Some(0) {
            bail!("--max-queries must be greater than zero");
        }
        if workers == 0 {
            bail!("--workers must be greater than zero");
        }
        Ok(Self {
            data_file,
            query_files,
            burn_in,
            max_queries,
            workers,
        })
    }
}

fn parse<T>(flag: &str, value: &str) -> Result<T>
where
    T: FromStr,
    T::Err: std::fmt::Display,
{
    value
        .parse()
        .map_err(|error| eyre!("invalid value for {flag}: {error}"))
}

#[derive(Debug, Deserialize)]
struct QueryPlan {
    human_label: String,
    #[allow(dead_code)]
    human_description: String,
    operation: String,
    #[serde(default)]
    hosts: Vec<String>,
    #[serde(default)]
    host_count: u64,
    #[serde(default)]
    start_timestamp: i64,
    #[serde(default)]
    end_timestamp: i64,
    #[serde(default)]
    metric_count: usize,
    #[serde(default)]
    bucket_nanos: i64,
    #[serde(default)]
    limit: usize,
    #[serde(default)]
    threshold: f64,
}

#[derive(Serialize)]
struct LoadResult<'a> {
    schema_version: u32,
    suite: &'static str,
    engine: &'static str,
    phase: &'static str,
    source_file: &'a Path,
    rows: u64,
    metrics: u64,
    elapsed_ns: u128,
    rows_per_second: f64,
    durability: &'static str,
    process_model: &'static str,
    storage_model: &'static str,
    workers: usize,
}

#[derive(Serialize)]
struct QueryResult<'a> {
    schema_version: u32,
    suite: &'static str,
    engine: &'static str,
    phase: &'static str,
    query_file: &'a Path,
    label: &'a str,
    queries: usize,
    burn_in: usize,
    mean_ms: f64,
    p50_ms: f64,
    p95_ms: f64,
    p99_ms: f64,
    max_ms: f64,
    checksum: u64,
    durability: &'static str,
    execution: &'static str,
    read_ownership: &'static str,
}

fn main() -> Result<()> {
    let config = Config::from_args().map_err(|error| {
        eprintln!("error: {error:?}\nrun with --help for usage");
        error
    })?;
    let table = Arc::new(TsbsCpuWorkTable::default());
    load_data(&table, &config.data_file, config.workers)?;
    for query_file in &config.query_files {
        run_query_file(&table, query_file, config.burn_in, config.max_queries)?;
    }
    Ok(())
}

fn load_data(table: &Arc<TsbsCpuWorkTable>, path: &Path, workers: usize) -> Result<()> {
    reject_gzip(path)?;
    let started = Instant::now();
    let rows = if workers == 1 {
        load_single(table, path)?
    } else {
        load_parallel(table, path, workers)?
    };
    let elapsed_ns = started.elapsed().as_nanos();
    let result = LoadResult {
        schema_version: 1,
        suite: "tsbs-cpu-only",
        engine: "worktable",
        phase: "load",
        source_file: path,
        rows,
        metrics: rows * CPU_METRICS as u64,
        elapsed_ns,
        rows_per_second: rows as f64 / (elapsed_ns as f64 / 1_000_000_000.0),
        durability: "memory; no WAL or fsync",
        process_model: "embedded in query runner",
        storage_model: "row; host-major primary key",
        workers,
    };
    println!("{}", serde_json::to_string(&result)?);
    Ok(())
}

fn load_single(table: &TsbsCpuWorkTable, path: &Path) -> Result<u64> {
    let file = File::open(path).wrap_err_with(|| format!("cannot open {}", path.display()))?;
    let reader = BufReader::with_capacity(4 << 20, file);
    let mut rows = 0_u64;
    for (line_number, line) in reader.lines().enumerate() {
        let line = line.wrap_err_with(|| format!("cannot read line {}", line_number + 1))?;
        if line.is_empty() {
            continue;
        }
        let row = parse_cpu_line(&line)
            .wrap_err_with(|| format!("invalid CPU row on line {}", line_number + 1))?;
        table
            .insert(row)
            .map_err(|error| eyre!("insert failed on line {}: {error}", line_number + 1))?;
        rows += 1;
    }
    Ok(rows)
}

fn load_parallel(table: &Arc<TsbsCpuWorkTable>, path: &Path, workers: usize) -> Result<u64> {
    let mut senders = Vec::with_capacity(workers);
    let mut handles = Vec::with_capacity(workers);
    for _ in 0..workers {
        let (sender, receiver) = sync_channel::<(usize, String)>(1024);
        let table = Arc::clone(table);
        senders.push(sender);
        handles.push(thread::spawn(move || -> Result<u64> {
            let mut rows = 0_u64;
            while let Ok((line_number, line)) = receiver.recv() {
                let row = parse_cpu_line(&line)
                    .wrap_err_with(|| format!("invalid CPU row on line {line_number}"))?;
                table
                    .insert(row)
                    .map_err(|error| eyre!("insert failed on line {line_number}: {error}"))?;
                rows += 1;
            }
            Ok(rows)
        }));
    }

    let file = File::open(path).wrap_err_with(|| format!("cannot open {}", path.display()))?;
    let reader = BufReader::with_capacity(4 << 20, file);
    for (line_number, line) in reader.lines().enumerate() {
        let line = line.wrap_err_with(|| format!("cannot read line {}", line_number + 1))?;
        if line.is_empty() {
            continue;
        }
        let worker = line_number % workers;
        senders[worker]
            .send((line_number + 1, line))
            .map_err(|_| eyre!("load worker {worker} stopped unexpectedly"))?;
    }
    drop(senders);

    handles.into_iter().try_fold(0_u64, |total, handle| {
        let rows = handle.join().map_err(|_| eyre!("load worker panicked"))??;
        Ok(total + rows)
    })
}

fn reject_gzip(path: &Path) -> Result<()> {
    if path.extension().is_some_and(|extension| extension == "gz") {
        bail!(
            "{} is compressed; decompress it before running so decompression is not timed",
            path.display()
        );
    }
    Ok(())
}

fn parse_cpu_line(line: &str) -> Result<TsbsCpuRow> {
    let mut parts = line.split_ascii_whitespace();
    let series = parts.next().ok_or_else(|| eyre!("missing series"))?;
    let fields = parts.next().ok_or_else(|| eyre!("missing fields"))?;
    let timestamp: i64 = parse(
        "timestamp",
        parts.next().ok_or_else(|| eyre!("missing timestamp"))?,
    )?;
    if parts.next().is_some() {
        bail!("unexpected data after timestamp");
    }
    if timestamp < 0 {
        bail!("negative timestamps are not supported by the host-major key encoding");
    }

    let mut tags = series.split(',');
    if tags.next() != Some("cpu") {
        bail!("only the TSBS cpu-only use case is supported");
    }
    let hostname = tags
        .find_map(|tag| tag.strip_prefix("hostname="))
        .ok_or_else(|| eyre!("missing hostname tag"))?;
    let host_id = parse_host_id(hostname)?;

    let mut metrics = [0_i64; CPU_METRICS];
    let mut seen = [false; CPU_METRICS];
    for field in fields.split(',') {
        let (name, value) = field
            .split_once('=')
            .ok_or_else(|| eyre!("invalid field {field}"))?;
        if let Some(index) = metric_index(name) {
            metrics[index] = parse_metric(value)?;
            seen[index] = true;
        }
    }
    if !seen.into_iter().all(|value| value) {
        bail!("cpu-only row does not contain all {CPU_METRICS} metrics");
    }
    Ok(TsbsCpuRow {
        id: row_key(host_id, timestamp),
        host_id,
        timestamp,
        metrics,
    })
}

fn parse_host_id(hostname: &str) -> Result<u64> {
    let value = hostname
        .strip_prefix("host_")
        .ok_or_else(|| eyre!("invalid TSBS hostname {hostname}"))?;
    parse("hostname", value)
}

fn parse_metric(value: &str) -> Result<i64> {
    let value = value.strip_suffix('i').unwrap_or(value);
    if let Ok(integer) = value.parse() {
        return Ok(integer);
    }
    let float: f64 = parse("metric", value)?;
    Ok(float.round() as i64)
}

fn metric_index(name: &str) -> Option<usize> {
    match name {
        "usage_user" => Some(0),
        "usage_system" => Some(1),
        "usage_idle" => Some(2),
        "usage_nice" => Some(3),
        "usage_iowait" => Some(4),
        "usage_irq" => Some(5),
        "usage_softirq" => Some(6),
        "usage_steal" => Some(7),
        "usage_guest" => Some(8),
        "usage_guest_nice" => Some(9),
        _ => None,
    }
}

fn row_key(host_id: u64, timestamp: i64) -> u128 {
    ((host_id as u128) << 64) | timestamp as u64 as u128
}

fn host_end_key(host_id: u64) -> u128 {
    ((host_id as u128) + 1) << 64
}

fn read_query_file(path: &Path, max_queries: Option<usize>) -> Result<Vec<QueryPlan>> {
    reject_gzip(path)?;
    let file = File::open(path).wrap_err_with(|| format!("cannot open {}", path.display()))?;
    let reader = BufReader::new(file);
    reader
        .lines()
        .take(max_queries.unwrap_or(usize::MAX))
        .enumerate()
        .map(|(line_number, line)| {
            let line = line.wrap_err("cannot read query")?;
            serde_json::from_str(&line)
                .wrap_err_with(|| format!("invalid query on line {}", line_number + 1))
        })
        .collect()
}

fn run_query_file(
    table: &TsbsCpuWorkTable,
    path: &Path,
    burn_in: usize,
    max_queries: Option<usize>,
) -> Result<()> {
    let plans = read_query_file(path, max_queries)?;
    if plans.is_empty() {
        bail!("{} contains no queries", path.display());
    }
    if burn_in >= plans.len() {
        bail!(
            "burn-in {burn_in} leaves no measured queries in {}",
            path.display()
        );
    }
    let label = plans[0].human_label.clone();
    if plans.iter().any(|plan| plan.human_label != label) {
        bail!(
            "{} mixes query labels; use one TSBS query type per file",
            path.display()
        );
    }

    let mut latencies = Vec::with_capacity(plans.len() - burn_in);
    let mut checksum = 0_u64;
    for (index, plan) in plans.iter().enumerate() {
        let started = Instant::now();
        let value = execute(table, plan)?;
        let elapsed = started.elapsed().as_nanos().min(u64::MAX as u128) as u64;
        black_box(value);
        if index >= burn_in {
            latencies.push(elapsed);
            checksum = checksum.wrapping_add(value);
        }
    }
    latencies.sort_unstable();
    let mean_ns = latencies.iter().map(|value| *value as f64).sum::<f64>() / latencies.len() as f64;
    let result = QueryResult {
        schema_version: 1,
        suite: "tsbs-cpu-only",
        engine: "worktable",
        phase: "query",
        query_file: path,
        label: &label,
        queries: latencies.len(),
        burn_in,
        mean_ms: mean_ns / 1_000_000.0,
        p50_ms: percentile(&latencies, 0.50) / 1_000_000.0,
        p95_ms: percentile(&latencies, 0.95) / 1_000_000.0,
        p99_ms: percentile(&latencies, 0.99) / 1_000_000.0,
        max_ms: *latencies.last().expect("latencies are non-empty") as f64 / 1_000_000.0,
        checksum,
        durability: "memory; no WAL or fsync",
        execution: "compiled host Rust; no SQL parser or client/server boundary",
        read_ownership: "materialized owned rows",
    };
    println!("{}", serde_json::to_string(&result)?);
    Ok(())
}

fn percentile(values: &[u64], quantile: f64) -> f64 {
    let index = ((values.len() - 1) as f64 * quantile).round() as usize;
    values[index] as f64
}

fn execute(table: &TsbsCpuWorkTable, plan: &QueryPlan) -> Result<u64> {
    if plan.metric_count > CPU_METRICS {
        bail!(
            "query requests {} metrics, maximum is {CPU_METRICS}",
            plan.metric_count
        );
    }
    match plan.operation.as_str() {
        "single-groupby" => aggregate_max(table, plan, &host_ids(&plan.hosts)?),
        "max-all" => aggregate_max(table, plan, &host_ids(&plan.hosts)?),
        "double-groupby" => aggregate_mean_by_host(table, plan),
        "groupby-orderby-limit" => groupby_orderby_limit(table, plan),
        "lastpoint" => lastpoint(table, plan.host_count),
        "high-cpu" => high_cpu(table, plan),
        operation => bail!("unsupported WorkTable query operation {operation}"),
    }
}

fn host_ids(hosts: &[String]) -> Result<Vec<u64>> {
    hosts.iter().map(|host| parse_host_id(host)).collect()
}

fn select_host_window(
    table: &TsbsCpuWorkTable,
    host_id: u64,
    start: i64,
    end: i64,
) -> Result<Vec<TsbsCpuRow>> {
    table
        .select_by_pk_range(row_key(host_id, start)..row_key(host_id, end))
        .execute()
        .map_err(|error| eyre!("WorkTable range query failed: {error}"))
}

fn select_all_window(table: &TsbsCpuWorkTable, start: i64, end: i64) -> Result<Vec<TsbsCpuRow>> {
    table
        .select_by_pk_range(0_u128..=u128::MAX)
        .where_by(|row| row.timestamp >= start && row.timestamp < end)
        .execute()
        .map_err(|error| eyre!("WorkTable full range query failed: {error}"))
}

fn aggregate_max(table: &TsbsCpuWorkTable, plan: &QueryPlan, hosts: &[u64]) -> Result<u64> {
    if plan.bucket_nanos <= 0 {
        bail!("aggregate requires a positive bucket width");
    }
    let mut buckets: BTreeMap<i64, [i64; CPU_METRICS]> = BTreeMap::new();
    for host_id in hosts {
        for row in select_host_window(table, *host_id, plan.start_timestamp, plan.end_timestamp)? {
            let bucket = row.timestamp / plan.bucket_nanos;
            let entry = buckets.entry(bucket).or_insert([i64::MIN; CPU_METRICS]);
            for (target, value) in entry
                .iter_mut()
                .zip(row.metrics.iter())
                .take(plan.metric_count)
            {
                *target = (*target).max(*value);
            }
        }
    }
    Ok(checksum_max_buckets(&buckets, plan.metric_count))
}

fn checksum_max_buckets(buckets: &BTreeMap<i64, [i64; CPU_METRICS]>, metric_count: usize) -> u64 {
    buckets.values().fold(0_u64, |checksum, metrics| {
        metrics
            .iter()
            .take(metric_count)
            .filter(|value| **value != i64::MIN)
            .fold(checksum, |sum, value| sum.wrapping_add(*value as u64))
    })
}

fn aggregate_mean_by_host(table: &TsbsCpuWorkTable, plan: &QueryPlan) -> Result<u64> {
    if plan.bucket_nanos <= 0 {
        bail!("aggregate requires a positive bucket width");
    }
    let rows = select_all_window(table, plan.start_timestamp, plan.end_timestamp)?;
    let mut groups: BTreeMap<(u64, i64), ([i128; CPU_METRICS], u64)> = BTreeMap::new();
    for row in rows {
        let bucket = row.timestamp / plan.bucket_nanos;
        let (sums, count) = groups
            .entry((row.host_id, bucket))
            .or_insert(([0; CPU_METRICS], 0));
        for (sum, value) in sums
            .iter_mut()
            .zip(row.metrics.iter())
            .take(plan.metric_count)
        {
            *sum += *value as i128;
        }
        *count += 1;
    }
    Ok(groups.values().fold(0_u64, |checksum, (sums, count)| {
        sums.iter()
            .take(plan.metric_count)
            .fold(checksum, |sum, value| {
                let mean = *value as f64 / *count as f64;
                sum.wrapping_add(mean.to_bits())
            })
    }))
}

fn groupby_orderby_limit(table: &TsbsCpuWorkTable, plan: &QueryPlan) -> Result<u64> {
    if plan.bucket_nanos <= 0 || plan.limit == 0 {
        bail!("groupby-orderby-limit requires bucket width and limit");
    }
    let start = plan
        .end_timestamp
        .saturating_sub(plan.bucket_nanos.saturating_mul(plan.limit as i64));
    let rows = select_all_window(table, start, plan.end_timestamp)?;
    let mut buckets: BTreeMap<i64, i64> = BTreeMap::new();
    for row in rows {
        let bucket = row.timestamp / plan.bucket_nanos;
        buckets
            .entry(bucket)
            .and_modify(|value| *value = (*value).max(row.metrics[0]))
            .or_insert(row.metrics[0]);
    }
    Ok(buckets
        .values()
        .rev()
        .take(plan.limit)
        .fold(0_u64, |sum, value| sum.wrapping_add(*value as u64)))
}

fn lastpoint(table: &TsbsCpuWorkTable, host_count: u64) -> Result<u64> {
    let mut checksum = 0_u64;
    for host_id in 0..host_count {
        let rows = table
            .select_by_pk_range(row_key(host_id, 0)..host_end_key(host_id))
            .order_on(TsbsCpuRowFields::Id, Order::Desc)
            .limit(1)
            .execute()
            .map_err(|error| eyre!("WorkTable lastpoint query failed: {error}"))?;
        if let Some(row) = rows.first() {
            checksum = checksum.wrapping_add(row.timestamp as u64);
            checksum = row
                .metrics
                .iter()
                .fold(checksum, |sum, value| sum.wrapping_add(*value as u64));
        }
    }
    Ok(checksum)
}

fn high_cpu(table: &TsbsCpuWorkTable, plan: &QueryPlan) -> Result<u64> {
    let rows = if plan.hosts.is_empty() {
        select_all_window(table, plan.start_timestamp, plan.end_timestamp)?
    } else {
        let mut rows = Vec::new();
        for host_id in host_ids(&plan.hosts)? {
            rows.extend(select_host_window(
                table,
                host_id,
                plan.start_timestamp,
                plan.end_timestamp,
            )?);
        }
        rows
    };
    Ok(rows
        .iter()
        .filter(|row| row.metrics[0] as f64 > plan.threshold)
        .fold(0_u64, |checksum, row| {
            row.metrics
                .iter()
                .fold(checksum.wrapping_add(row.timestamp as u64), |sum, value| {
                    sum.wrapping_add(*value as u64)
                })
        }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_questdb_cpu_line() {
        let line = "cpu,hostname=host_7,region=test usage_user=1i,usage_system=2i,usage_idle=3i,usage_nice=4i,usage_iowait=5i,usage_irq=6i,usage_softirq=7i,usage_steal=8i,usage_guest=9i,usage_guest_nice=10i 100";
        let row = parse_cpu_line(line).unwrap();
        assert_eq!(row.host_id, 7);
        assert_eq!(row.timestamp, 100);
        assert_eq!(row.metrics, [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
        assert_eq!(row.id, row_key(7, 100));
    }

    #[test]
    fn primary_key_orders_by_host_then_time() {
        assert!(row_key(1, 200) < row_key(2, 100));
        assert!(row_key(1, 100) < row_key(1, 200));
    }
}
