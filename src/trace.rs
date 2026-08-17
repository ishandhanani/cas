// SPDX-License-Identifier: Apache-2.0

use std::collections::{BTreeSet, HashSet, VecDeque};
use std::fs::{self, File};
use std::io::{BufRead, BufReader, Read};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use flate2::read::MultiGzDecoder;
use rusqlite::{Connection, params};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tempfile::{Builder as TempBuilder, TempDir};

const TRACE_SCHEMA_V1: &str = "dynamo.request.trace.v1";

#[derive(Debug, Clone, Deserialize)]
struct TraceRecord {
    schema: String,
    event_type: String,
    #[serde(default)]
    agent_context: Option<AgentContext>,
    #[serde(default)]
    request: Option<RequestMetrics>,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize, PartialEq, Eq)]
pub struct AgentContext {
    pub session_id: String,
    #[serde(default)]
    pub parent_session_id: Option<String>,
    #[serde(default)]
    pub session_final: Option<bool>,
    #[serde(default)]
    pub compaction: Option<serde_json::Value>,
    #[serde(default)]
    pub input_trigger: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
struct RequestMetrics {
    request_id: String,
    #[serde(default)]
    x_request_id: Option<String>,
    #[serde(default)]
    model: Option<String>,
    #[serde(default)]
    input_tokens: Option<u64>,
    #[serde(default)]
    output_tokens: Option<u64>,
    #[serde(default)]
    request_received_ms: Option<u64>,
    #[serde(default)]
    replay: Option<ReplayMetrics>,
}

#[derive(Debug, Clone, Deserialize)]
struct ReplayMetrics {
    trace_block_size: usize,
    input_length: usize,
    input_sequence_hashes: Vec<u64>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct TraceRequest {
    pub ordinal: usize,
    pub source_request_id: String,
    pub source_x_request_id: Option<String>,
    pub source_model: Option<String>,
    pub input_tokens: usize,
    pub output_tokens: u32,
    pub request_received_ms: u64,
    pub trace_block_size: usize,
    pub input_sequence_hashes: Vec<u64>,
    pub agent_context: Option<AgentContext>,
}

impl TraceRequest {
    pub fn is_session_close(&self) -> bool {
        self.output_tokens == 0
            && self
                .agent_context
                .as_ref()
                .and_then(|context| context.session_final)
                == Some(true)
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct TraceManifest {
    pub request_count: usize,
    pub zero_output_requests: usize,
    pub session_count: usize,
    pub requests_with_agent_context: usize,
    pub first_request_received_ms: u64,
    pub last_request_received_ms: u64,
    pub duration_ms: u64,
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub distinct_sequence_hashes: usize,
    pub trace_block_size: usize,
    pub source_digest_sha256: String,
}

#[derive(Debug, Clone)]
pub(crate) struct LoadedTrace {
    pub requests: Vec<TraceRequest>,
    #[cfg(test)]
    pub manifest: TraceManifest,
}

#[derive(Debug, Clone, Serialize)]
pub struct TraceStorageManifest {
    pub backend: &'static str,
    pub database_bytes: u64,
    pub request_batch_size: usize,
}

pub struct StoredTrace {
    _directory: TempDir,
    database_path: PathBuf,
    pub manifest: TraceManifest,
    pub storage: TraceStorageManifest,
}

impl std::fmt::Debug for StoredTrace {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("StoredTrace")
            .field("database_path", &self.database_path)
            .field("manifest", &self.manifest)
            .field("storage", &self.storage)
            .finish()
    }
}

impl StoredTrace {
    pub(crate) fn reader(&self) -> StoredTraceReader {
        StoredTraceReader {
            database_path: self.database_path.clone(),
            next_ordinal: 0,
            batch_size: self.storage.request_batch_size,
            buffered: VecDeque::new(),
        }
    }
}

pub(crate) struct StoredTraceReader {
    database_path: PathBuf,
    next_ordinal: usize,
    batch_size: usize,
    buffered: VecDeque<TraceRequest>,
}

impl StoredTraceReader {
    pub(crate) fn next_request(&mut self) -> Result<Option<TraceRequest>> {
        if let Some(request) = self.buffered.pop_front() {
            self.next_ordinal = request.ordinal + 1;
            return Ok(Some(request));
        }
        let connection = Connection::open(&self.database_path).with_context(|| {
            format!(
                "failed to open trace spool {}",
                self.database_path.display()
            )
        })?;
        let mut statement = connection.prepare_cached(
            "SELECT payload FROM selected_requests WHERE ordinal >= ?1 ORDER BY ordinal LIMIT ?2",
        )?;
        let rows = statement.query_map(
            params![
                usize_to_i64(self.next_ordinal)?,
                usize_to_i64(self.batch_size)?
            ],
            |row| row.get::<_, Vec<u8>>(0),
        )?;
        for row in rows {
            let payload = row?;
            let request = serde_json::from_slice(&payload)
                .context("trace spool contains an invalid request")?;
            self.buffered.push_back(request);
        }
        let Some(request) = self.buffered.pop_front() else {
            return Ok(None);
        };
        self.next_ordinal = request.ordinal + 1;
        Ok(Some(request))
    }
}

pub fn load_stored_trace(
    paths: &[PathBuf],
    max_requests: Option<usize>,
    session_id: Option<&str>,
    spool_directory: Option<&Path>,
    request_batch_size: usize,
) -> Result<StoredTrace> {
    if paths.is_empty() {
        bail!("at least one trace path is required");
    }
    if request_batch_size == 0 {
        bail!("trace request batch size must be greater than zero");
    }
    let directory = match spool_directory {
        Some(parent) => {
            std::fs::create_dir_all(parent).with_context(|| {
                format!(
                    "failed to create trace spool directory {}",
                    parent.display()
                )
            })?;
            TempBuilder::new()
                .prefix("agent-loadgen-trace-")
                .tempdir_in(parent)
        }
        None => TempBuilder::new().prefix("agent-loadgen-trace-").tempdir(),
    }
    .context("failed to create the trace spool")?;
    let database_path = directory.path().join("trace.sqlite3");
    let mut connection =
        Connection::open(&database_path).context("failed to create the trace spool database")?;
    initialize_trace_database(&connection)?;

    let mut source_digest = Sha256::new();
    let mut source_ordinal = 0_usize;
    {
        let transaction = connection.transaction()?;
        for path in paths {
            read_path(path, &mut source_digest, |record| {
                if record.event_type != "request_end" {
                    return Ok(());
                }
                let request = compile_request(record, source_ordinal)?;
                source_ordinal += 1;
                if session_id.is_some_and(|selected| {
                    request
                        .agent_context
                        .as_ref()
                        .is_none_or(|context| context.session_id != selected)
                }) {
                    return Ok(());
                }
                let payload = serde_json::to_vec(&request)?;
                transaction.execute(
                    "INSERT INTO raw_requests(source_ordinal, received_ms, payload) VALUES (?1, ?2, ?3)",
                    params![usize_to_i64(request.ordinal)?, u64_to_i64(request.request_received_ms)?, payload],
                )?;
                Ok(())
            })?;
        }
        transaction.commit()?;
    }

    let raw_count: i64 =
        connection.query_row("SELECT COUNT(*) FROM raw_requests", [], |row| row.get(0))?;
    if raw_count == 0 {
        if let Some(session_id) = session_id {
            bail!("the trace contains no requests for session {session_id:?}");
        }
        bail!("the trace contains no request_end records");
    }

    let manifest = select_and_index_requests(
        &mut connection,
        max_requests,
        source_digest.finalize().as_slice(),
        request_batch_size,
    )?;
    connection.execute_batch("PRAGMA optimize;")?;
    drop(connection);
    let database_bytes = fs::metadata(&database_path)
        .with_context(|| format!("failed to stat trace spool {}", database_path.display()))?
        .len();
    Ok(StoredTrace {
        _directory: directory,
        database_path,
        manifest,
        storage: TraceStorageManifest {
            backend: "sqlite-spool-v1",
            database_bytes,
            request_batch_size,
        },
    })
}

fn initialize_trace_database(connection: &Connection) -> Result<()> {
    connection.execute_batch(
        "
        PRAGMA journal_mode = OFF;
        PRAGMA synchronous = OFF;
        PRAGMA temp_store = FILE;
        PRAGMA cache_size = -65536;
        CREATE TABLE raw_requests (
            source_ordinal INTEGER PRIMARY KEY,
            received_ms INTEGER NOT NULL,
            payload BLOB NOT NULL
        );
        CREATE INDEX raw_requests_order ON raw_requests(received_ms, source_ordinal);
        CREATE TABLE selected_requests (
            ordinal INTEGER PRIMARY KEY,
            payload BLOB NOT NULL
        );
        CREATE TABLE request_ids (value TEXT PRIMARY KEY) WITHOUT ROWID;
        CREATE TABLE sequence_hashes (value BLOB PRIMARY KEY) WITHOUT ROWID;
        CREATE TABLE sessions (value TEXT PRIMARY KEY) WITHOUT ROWID;
        CREATE TABLE block_sizes (value INTEGER PRIMARY KEY) WITHOUT ROWID;
        ",
    )?;
    Ok(())
}

fn select_and_index_requests(
    connection: &mut Connection,
    max_requests: Option<usize>,
    source_digest: &[u8],
    batch_size: usize,
) -> Result<TraceManifest> {
    let selected_limit = max_requests.unwrap_or(usize::MAX);
    let mut selected = 0_usize;
    let mut after_key: Option<(u64, usize)> = None;
    let mut first_received_ms = None;
    let mut last_received_ms = None;
    let mut input_tokens = 0_u64;
    let mut output_tokens = 0_u64;
    let mut requests_with_agent_context = 0_usize;
    let mut zero_output_requests = 0_usize;

    while selected < selected_limit {
        let remaining = selected_limit - selected;
        let rows = read_raw_batch(connection, after_key, batch_size.min(remaining))?;
        if rows.is_empty() {
            break;
        }
        let transaction = connection.transaction()?;
        for mut request in rows {
            after_key = Some((request.request_received_ms, request.ordinal));
            request.ordinal = selected;
            selected += 1;
            if transaction.execute(
                "INSERT OR IGNORE INTO request_ids(value) VALUES (?1)",
                [&request.source_request_id],
            )? == 0
            {
                bail!("duplicate request_id {}", request.source_request_id);
            }
            transaction.execute(
                "INSERT OR IGNORE INTO block_sizes(value) VALUES (?1)",
                [usize_to_i64(request.trace_block_size)?],
            )?;
            for hash in &request.input_sequence_hashes {
                transaction.execute(
                    "INSERT OR IGNORE INTO sequence_hashes(value) VALUES (?1)",
                    [hash.to_be_bytes().as_slice()],
                )?;
            }
            if let Some(context) = &request.agent_context {
                requests_with_agent_context += 1;
                transaction.execute(
                    "INSERT OR IGNORE INTO sessions(value) VALUES (?1)",
                    [&context.session_id],
                )?;
            }
            first_received_ms.get_or_insert(request.request_received_ms);
            last_received_ms = Some(request.request_received_ms);
            input_tokens = input_tokens
                .checked_add(request.input_tokens as u64)
                .context("input token count overflow")?;
            output_tokens = output_tokens
                .checked_add(request.output_tokens as u64)
                .context("output token count overflow")?;
            if request.output_tokens == 0 {
                zero_output_requests += 1;
            }
            transaction.execute(
                "INSERT INTO selected_requests(ordinal, payload) VALUES (?1, ?2)",
                params![
                    usize_to_i64(request.ordinal)?,
                    serde_json::to_vec(&request)?
                ],
            )?;
        }
        transaction.commit()?;
    }
    if selected == 0 {
        bail!("the selected trace contains no requests");
    }

    let block_sizes = query_i64_values(connection, "SELECT value FROM block_sizes ORDER BY value")?;
    if block_sizes.len() != 1 {
        bail!("shape-strict replay requires one trace_block_size, found {block_sizes:?}");
    }
    let distinct_sequence_hashes = query_count(connection, "sequence_hashes")?;
    let session_count = query_count(connection, "sessions")?;
    let first_request_received_ms = first_received_ms.context("trace has no requests")?;
    let last_request_received_ms = last_received_ms.context("trace has no requests")?;
    Ok(TraceManifest {
        request_count: selected,
        zero_output_requests,
        session_count,
        requests_with_agent_context,
        first_request_received_ms,
        last_request_received_ms,
        duration_ms: last_request_received_ms - first_request_received_ms,
        input_tokens,
        output_tokens,
        distinct_sequence_hashes,
        trace_block_size: usize::try_from(block_sizes[0])
            .context("trace block size does not fit usize")?,
        source_digest_sha256: hex::encode(source_digest),
    })
}

fn read_raw_batch(
    connection: &Connection,
    after_key: Option<(u64, usize)>,
    limit: usize,
) -> Result<Vec<TraceRequest>> {
    let mut output = Vec::with_capacity(limit);
    if limit == 0 {
        return Ok(output);
    }
    match after_key {
        Some((received_ms, source_ordinal)) => {
            let mut statement = connection.prepare_cached(
                "SELECT payload FROM raw_requests
                 WHERE received_ms > ?1 OR (received_ms = ?1 AND source_ordinal > ?2)
                 ORDER BY received_ms, source_ordinal LIMIT ?3",
            )?;
            let rows = statement.query_map(
                params![
                    u64_to_i64(received_ms)?,
                    usize_to_i64(source_ordinal)?,
                    usize_to_i64(limit)?
                ],
                |row| row.get::<_, Vec<u8>>(0),
            )?;
            for row in rows {
                output.push(serde_json::from_slice(&row?)?);
            }
        }
        None => {
            let mut statement = connection.prepare_cached(
                "SELECT payload FROM raw_requests ORDER BY received_ms, source_ordinal LIMIT ?1",
            )?;
            let rows =
                statement.query_map([usize_to_i64(limit)?], |row| row.get::<_, Vec<u8>>(0))?;
            for row in rows {
                output.push(serde_json::from_slice(&row?)?);
            }
        }
    }
    Ok(output)
}

fn query_count(connection: &Connection, table: &str) -> Result<usize> {
    let sql = format!("SELECT COUNT(*) FROM {table}");
    let count: i64 = connection.query_row(&sql, [], |row| row.get(0))?;
    usize::try_from(count).context("trace spool count does not fit usize")
}

fn query_i64_values(connection: &Connection, sql: &str) -> Result<Vec<i64>> {
    let mut statement = connection.prepare(sql)?;
    let rows = statement.query_map([], |row| row.get(0))?;
    rows.collect::<rusqlite::Result<Vec<_>>>()
        .map_err(Into::into)
}

fn usize_to_i64(value: usize) -> Result<i64> {
    i64::try_from(value).context("trace value does not fit SQLite INTEGER")
}

fn u64_to_i64(value: u64) -> Result<i64> {
    i64::try_from(value).context("trace timestamp does not fit SQLite INTEGER")
}

pub(crate) fn load_trace(
    paths: &[PathBuf],
    max_requests: Option<usize>,
    session_id: Option<&str>,
) -> Result<LoadedTrace> {
    if paths.is_empty() {
        bail!("at least one trace path is required");
    }

    let mut requests = Vec::new();
    let mut source_digest = Sha256::new();
    for path in paths {
        load_path(path, &mut requests, &mut source_digest)?;
    }
    if requests.is_empty() {
        bail!("the trace contains no request_end records");
    }

    requests.sort_by_key(|request| (request.request_received_ms, request.ordinal));
    if let Some(session_id) = session_id {
        requests.retain(|request| {
            request
                .agent_context
                .as_ref()
                .is_some_and(|context| context.session_id == session_id)
        });
        if requests.is_empty() {
            bail!("the trace contains no requests for session {session_id:?}");
        }
    }
    if let Some(limit) = max_requests {
        requests.truncate(limit);
    }
    for (ordinal, request) in requests.iter_mut().enumerate() {
        request.ordinal = ordinal;
    }

    let source_digest = source_digest.finalize();
    #[cfg(test)]
    let manifest = make_manifest(&requests, source_digest.as_slice())?;
    #[cfg(not(test))]
    make_manifest(&requests, source_digest.as_slice())?;
    Ok(LoadedTrace {
        requests,
        #[cfg(test)]
        manifest,
    })
}

fn load_path(path: &Path, requests: &mut Vec<TraceRequest>, digest: &mut Sha256) -> Result<()> {
    read_path(path, digest, |record| {
        if record.event_type != "request_end" {
            return Ok(());
        }
        let request =
            compile_request(record, requests.len()).context("invalid request_end record")?;
        requests.push(request);
        Ok(())
    })
}

fn read_path(
    path: &Path,
    digest: &mut Sha256,
    mut visit: impl FnMut(TraceRecord) -> Result<()>,
) -> Result<()> {
    let reader = open_trace(path)?;
    for (line_index, line) in reader.lines().enumerate() {
        let line =
            line.with_context(|| format!("failed to read {}:{}", path.display(), line_index + 1))?;
        if line.trim().is_empty() {
            continue;
        }
        digest.update(line.as_bytes());
        digest.update(b"\n");

        let value: serde_json::Value = serde_json::from_str(&line)
            .with_context(|| format!("invalid JSON at {}:{}", path.display(), line_index + 1))?;
        let record_value = value
            .get("event")
            .or_else(|| value.get("record"))
            .unwrap_or(&value);
        let record: TraceRecord =
            serde_json::from_value(record_value.clone()).with_context(|| {
                format!(
                    "invalid trace record at {}:{}",
                    path.display(),
                    line_index + 1
                )
            })?;
        if record.schema != TRACE_SCHEMA_V1 {
            bail!(
                "unsupported trace schema {:?} at {}:{}",
                record.schema,
                path.display(),
                line_index + 1
            );
        }
        visit(record).with_context(|| {
            format!(
                "invalid trace record at {}:{}",
                path.display(),
                line_index + 1
            )
        })?;
    }
    Ok(())
}

fn open_trace(path: &Path) -> Result<Box<dyn BufRead>> {
    let file = File::open(path).with_context(|| format!("failed to open {}", path.display()))?;
    let is_gzip = path.extension().is_some_and(|extension| extension == "gz");
    let reader: Box<dyn Read> = if is_gzip {
        Box::new(MultiGzDecoder::new(file))
    } else {
        Box::new(file)
    };
    Ok(Box::new(BufReader::new(reader)))
}

fn compile_request(record: TraceRecord, ordinal: usize) -> Result<TraceRequest> {
    let request = record
        .request
        .context("request_end has no request metrics")?;
    let replay = request
        .replay
        .context("request_end has no replay metrics")?;
    if replay.trace_block_size == 0 {
        bail!("trace_block_size must be greater than zero");
    }
    if replay.input_length == 0 {
        bail!("input_length must be greater than zero");
    }
    let required_hashes = replay.input_length.div_ceil(replay.trace_block_size);
    if replay.input_sequence_hashes.len() != required_hashes {
        bail!(
            "request {} has {} replay hashes, expected {}",
            request.request_id,
            replay.input_sequence_hashes.len(),
            required_hashes
        );
    }
    if request
        .input_tokens
        .is_some_and(|input_tokens| input_tokens != replay.input_length as u64)
    {
        bail!(
            "request {} input_tokens does not match replay input_length",
            request.request_id
        );
    }
    let output_tokens = request
        .output_tokens
        .context("request has no output_tokens")?;
    let output_tokens = u32::try_from(output_tokens).context("output_tokens does not fit u32")?;
    if output_tokens == 0
        && record
            .agent_context
            .as_ref()
            .and_then(|context| context.session_final)
            != Some(true)
    {
        bail!(
            "request {} has zero output but is not a session-final control request",
            request.request_id
        );
    }
    let request_received_ms = request
        .request_received_ms
        .context("request has no request_received_ms")?;

    Ok(TraceRequest {
        ordinal,
        source_request_id: request.request_id,
        source_x_request_id: request.x_request_id,
        source_model: request.model,
        input_tokens: replay.input_length,
        output_tokens,
        request_received_ms,
        trace_block_size: replay.trace_block_size,
        input_sequence_hashes: replay.input_sequence_hashes,
        agent_context: record.agent_context,
    })
}

fn make_manifest(requests: &[TraceRequest], source_digest: &[u8]) -> Result<TraceManifest> {
    let first = requests.first().context("trace has no requests")?;
    let last = requests.last().context("trace has no requests")?;
    let mut request_ids = HashSet::with_capacity(requests.len());
    let mut sessions = BTreeSet::new();
    let mut hashes = BTreeSet::new();
    let mut block_sizes = BTreeSet::new();
    let mut input_tokens = 0_u64;
    let mut output_tokens = 0_u64;
    let mut requests_with_agent_context = 0;
    let mut zero_output_requests = 0;

    for request in requests {
        if !request_ids.insert(&request.source_request_id) {
            bail!("duplicate request_id {}", request.source_request_id);
        }
        if let Some(context) = &request.agent_context {
            requests_with_agent_context += 1;
            sessions.insert(&context.session_id);
        }
        hashes.extend(request.input_sequence_hashes.iter().copied());
        block_sizes.insert(request.trace_block_size);
        input_tokens = input_tokens
            .checked_add(request.input_tokens as u64)
            .context("input token count overflow")?;
        output_tokens = output_tokens
            .checked_add(request.output_tokens as u64)
            .context("output token count overflow")?;
        if request.output_tokens == 0 {
            zero_output_requests += 1;
        }
    }
    if block_sizes.len() != 1 {
        bail!("shape-strict replay requires one trace_block_size, found {block_sizes:?}");
    }

    Ok(TraceManifest {
        request_count: requests.len(),
        zero_output_requests,
        session_count: sessions.len(),
        requests_with_agent_context,
        first_request_received_ms: first.request_received_ms,
        last_request_received_ms: last.request_received_ms,
        duration_ms: last.request_received_ms - first.request_received_ms,
        input_tokens,
        output_tokens,
        distinct_sequence_hashes: hashes.len(),
        trace_block_size: *block_sizes.first().expect("one block size exists"),
        source_digest_sha256: hex::encode(source_digest),
    })
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use tempfile::NamedTempFile;

    use super::*;

    #[test]
    fn loads_raw_and_wrapped_records() {
        let mut file = NamedTempFile::new().unwrap();
        let record = serde_json::json!({
            "schema": TRACE_SCHEMA_V1,
            "event_type": "request_end",
            "event_time_unix_ms": 2000,
            "agent_context": {"session_id": "thread-1"},
            "request": {
                "request_id": "req-1",
                "input_tokens": 3,
                "output_tokens": 2,
                "request_received_ms": 1000,
                "replay": {
                    "trace_block_size": 2,
                    "input_length": 3,
                    "input_sequence_hashes": [11, 22]
                }
            }
        });
        writeln!(file, "{}", record).unwrap();
        let wrapped = serde_json::json!({"timestamp": 1, "event": {
            "schema": TRACE_SCHEMA_V1,
            "event_type": "request_end",
            "event_time_unix_ms": 2100,
            "request": {
                "request_id": "req-2",
                "output_tokens": 4,
                "request_received_ms": 1025,
                "replay": {
                    "trace_block_size": 2,
                    "input_length": 4,
                    "input_sequence_hashes": [11, 33]
                }
            }
        }});
        writeln!(file, "{}", wrapped).unwrap();

        let trace = load_trace(&[file.path().to_path_buf()], None, None).unwrap();
        assert_eq!(trace.manifest.request_count, 2);
        assert_eq!(trace.manifest.duration_ms, 25);
        assert_eq!(trace.manifest.distinct_sequence_hashes, 3);
        assert_eq!(trace.requests[1].input_tokens, 4);
    }

    #[test]
    fn stored_trace_orders_before_applying_limit() {
        let mut file = NamedTempFile::new().unwrap();
        for (request_id, received_ms, hash) in [("late", 1025, 22), ("early", 1000, 11)] {
            writeln!(
                file,
                "{}",
                serde_json::json!({
                    "schema": TRACE_SCHEMA_V1,
                    "event_type": "request_end",
                    "agent_context": {"session_id": "thread-1"},
                    "request": {
                        "request_id": request_id,
                        "output_tokens": 2,
                        "request_received_ms": received_ms,
                        "replay": {
                            "trace_block_size": 16,
                            "input_length": 3,
                            "input_sequence_hashes": [hash]
                        }
                    }
                })
            )
            .unwrap();
        }

        let stored = load_stored_trace(
            &[file.path().to_path_buf()],
            None,
            Some("thread-1"),
            None,
            2,
        )
        .unwrap();
        assert_eq!(stored.manifest.request_count, 2);
        assert_eq!(stored.manifest.distinct_sequence_hashes, 2);
        assert_eq!(stored.storage.backend, "sqlite-spool-v1");
        let mut reader = stored.reader();
        assert_eq!(
            reader.next_request().unwrap().unwrap().source_request_id,
            "early"
        );
        assert_eq!(
            reader.next_request().unwrap().unwrap().source_request_id,
            "late"
        );
        assert!(reader.next_request().unwrap().is_none());
    }

    #[test]
    fn rejects_incomplete_hash_coverage() {
        let record: TraceRecord = serde_json::from_value(serde_json::json!({
            "schema": TRACE_SCHEMA_V1,
            "event_type": "request_end",
            "request": {
                "request_id": "req-1",
                "output_tokens": 2,
                "request_received_ms": 1000,
                "replay": {
                    "trace_block_size": 2,
                    "input_length": 3,
                    "input_sequence_hashes": [11]
                }
            }
        }))
        .unwrap();
        assert!(compile_request(record, 0).is_err());
    }

    #[test]
    fn accepts_zero_output_only_for_session_close() {
        let close = serde_json::json!({
            "schema": TRACE_SCHEMA_V1,
            "event_type": "request_end",
            "agent_context": {"session_id": "thread-1", "session_final": true},
            "request": {
                "request_id": "close",
                "output_tokens": 0,
                "request_received_ms": 1000,
                "replay": {
                    "trace_block_size": 16,
                    "input_length": 3,
                    "input_sequence_hashes": [11]
                }
            }
        });
        let request = compile_request(serde_json::from_value(close.clone()).unwrap(), 0).unwrap();
        assert!(request.is_session_close());

        let mut model_turn = close;
        model_turn["agent_context"]["session_final"] = serde_json::json!(false);
        model_turn["request"]["request_id"] = serde_json::json!("bad-zero");
        assert!(
            compile_request(serde_json::from_value(model_turn).unwrap(), 0)
                .unwrap_err()
                .to_string()
                .contains("not a session-final control request")
        );
    }

    #[test]
    #[ignore = "large trace stress"]
    fn stored_trace_handles_one_hundred_thousand_requests_in_batches() {
        const REQUESTS: usize = 100_000;
        let mut file = NamedTempFile::new().unwrap();
        for ordinal in (0..REQUESTS).rev() {
            writeln!(
                file,
                "{}",
                serde_json::json!({
                    "schema": TRACE_SCHEMA_V1,
                    "event_type": "request_end",
                    "agent_context": {"session_id": format!("session-{}", ordinal % 100)},
                    "request": {
                        "request_id": format!("request-{ordinal}"),
                        "output_tokens": 8,
                        "request_received_ms": ordinal,
                        "replay": {
                            "trace_block_size": 16,
                            "input_length": 32,
                            "input_sequence_hashes": [ordinal % 1000, 10_000 + ordinal % 1000]
                        }
                    }
                })
            )
            .unwrap();
        }
        let stored =
            load_stored_trace(&[file.path().to_path_buf()], None, None, None, 128).unwrap();
        assert_eq!(stored.manifest.request_count, REQUESTS);
        assert_eq!(stored.manifest.session_count, 100);
        assert_eq!(stored.manifest.distinct_sequence_hashes, 2_000);
        let mut reader = stored.reader();
        let mut count = 0;
        while let Some(request) = reader.next_request().unwrap() {
            assert_eq!(request.request_received_ms, count as u64);
            count += 1;
        }
        assert_eq!(count, REQUESTS);
    }
}
