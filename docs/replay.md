<!-- SPDX-License-Identifier: Apache-2.0 -->

# Trace replay

Captured replay reproduces the request shape and arrival schedule in Dynamo `dynamo.request.trace.v1` `request_end` records.

## Required trace data

Every selected request must contain:

- `request_id` and `request_received_ms`.
- Positive `output_tokens`.
- Replay metadata with `trace_block_size`, `input_length`, and complete `input_sequence_hashes`.
- Positive input length and exactly `ceil(input_length / trace_block_size)` hashes.

The loader accepts raw JSONL, wrapped trace records, gzip JSONL, and multiple shards. It streams records into a temporary SQLite database, orders them globally by `(request_received_ms, source ordinal)`, and applies `--session-id` or `--max-requests` after ordering.

Shape-strict loading rejects duplicate request IDs, mixed block sizes, incomplete hashes, and inconsistent input counts before network traffic starts.

## Synthetic tokens

Replay hashes describe block equality, not the original prompt tokens. The token dictionary maps each hash to a deterministic synthetic codeword:

- Equal hashes produce equal token blocks.
- Different hashes produce different full-block codewords.
- Partial final blocks preserve as much hash entropy as their token capacity allows.

Verified mode loads `tokenizer.json` from a local file, model directory, or Hugging Face model ID. It excludes known special IDs and records tokenizer and alphabet digests.

A manual range requires both `--token-start` and `--allow-unverified-token-range`. Use that only with a controlled mocker or a known-safe model.

Synthetic IDs are sent through `nvext.token_data`. Use Dynamo's Rust chat processor so the supplied IDs reach the engine unchanged.

## Open-loop timing

Replay normalizes the first selected arrival to time zero and divides later offsets by `--time-scale`.

The runner uses absolute monotonic deadlines. Responses never move later requests. A local admission miss is recorded as a failed request rather than silently retiming the trace.

Linux uses `timerfd` with `CLOCK_MONOTONIC`. Other platforms use Tokio's monotonic timer.

The default client-offer gates are:

- `--max-dispatch-p99-ms 2`
- `--max-dispatch-max-ms 5`

These measure when the prepared request reaches the HTTP client. They do not measure when Dynamo receives the body.

HTTP/2 prior knowledge is the default because multiplexing avoids HTTP/1 connection-pool serialization during bursts. Use `--http-transport auto` when the target does not accept cleartext HTTP/2.

## Memory bounds

Trace ingestion and replay do not materialize the selected trace in memory:

- SQLite owns global ordering and selection.
- `--trace-request-batch-size` controls reader batches.
- `--prepare-lookahead-ms` controls prepared request bodies.
- Fixed-memory histograms summarize timing.
- `requests.jsonl` is written incrementally.

The `compare` command currently loads the selected source and replay traces in memory.

## Compare a captured replay

Enable Dynamo request tracing while replay runs, then compare the source with the captured trace:

```bash
target/release/agent-loadgen compare \
  --source source-trace.jsonl.gz \
  --replay captured-trace.jsonl.gz \
  --requests artifacts/replay/requests.jsonl
```

Comparison checks:

- Exact input and output lengths.
- Trace block size.
- Agent session, parent-session, and input-trigger context.
- Canonical KV-prefix equality topology.
- Frontend-observed arrival error.

Codex compaction metadata is currently opaque to Dynamo request tracing. The comparator reports unavailable metadata as a warning while keeping KV topology strict.

## Engine telemetry

Cache policy stays outside the load generator. Optional engine JSONL can be joined after a run:

```bash
target/release/agent-loadgen join-telemetry \
  --requests artifacts/replay/requests.jsonl \
  --engine-telemetry engine.jsonl \
  --output artifacts/replay/requests-with-engine.jsonl
```

Records are matched across replay request ID, source `x_request_id`, and source request ID. Known fields include cache-source tokens, physical frees, ownership, occupancy, and queue time. Unknown fields are retained without interpreting engine policy.

## Known limits

- Older traces without replay hashes cannot reconstruct KV topology from token counts alone.
- Source arrivals have millisecond resolution.
- Dummy tokens preserve equality shape, not literal production KV hash values.
- Operating-system scheduling, body transfer, frontend parsing, and target backpressure can miss deadlines; the run reports those misses instead of hiding them.
