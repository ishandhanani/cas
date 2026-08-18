// SPDX-License-Identifier: Apache-2.0

use std::collections::VecDeque;
use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use rusqlite::{Connection, params};
use serde::Serialize;
use sha2::{Digest, Sha256};
use tempfile::{Builder as TempBuilder, TempDir};

use super::{TraceManifest, TraceRequest, compile_request, read_path};

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
            fs::create_dir_all(parent).with_context(|| {
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
