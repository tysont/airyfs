# AiryFS Performance Optimization Scratch Pad

## Purpose

This document is the working record for AiryFS performance experiments. It tracks ideas before implementation, the exact implementation and validation plan while work is active, measured results after deployment, and the final retain or reject decision.

The optimization loop is deliberately conservative:

1. Change one mechanism at a time.
2. Build and run the complete local test suites.
3. Deploy to the integration environment.
4. Pass all deployed feature checks.
5. Measure the target workload and unrelated control workloads.
6. Confirm that request-amplification counters changed in the way predicted by the mechanism.
7. Retain, document, commit, and push only a repeatable deployed win without correctness regressions.
8. Remove rejected experiments and restore the last accepted deployment.

Detailed benchmark methodology lives in [`PERFORMANCE_BENCHMARK.md`](./PERFORMANCE_BENCHMARK.md). This document is broader: it is the experiment queue and decision log.

## Status Vocabulary

| Status | Meaning |
|---|---|
| `Accepted` | Implemented, validated, benchmarked, committed, and retained. |
| `Rejected` | Implemented and measured, but removed because it regressed performance, correctness, or stability. |
| `Queued` | Concrete enough to implement and benchmark, but not started. |
| `Investigate` | Plausible direction that needs design or instrumentation before implementation. |
| `Active` | The one experiment currently being implemented or measured. |
| `Blocked` | Cannot proceed until a named prerequisite is satisfied. |

## Experiment Index

| ID | Status | Experiment | Primary target | Expected or measured mechanism |
|---|---|---|---|---|
| A1 | `Accepted` | Skip remote prepared-statement descriptions | All remote FUSE operations | Remove one unused Hrana `describe` round trip per prepared statement |
| A2 | `Accepted` | Reuse a healthy Hrana bridge | All multi-command benchmark and FUSE workflows | Preserve the data session instead of reconnecting between healthy execs |
| A3 | `Accepted` | Adaptive FUSE `READDIRPLUS` | Metadata and Git | Let Linux consume directory entries and attributes from the existing combined query |
| A4 | `Accepted` | Single-query `pread` | Sequential and random reads | Return inode size and requested chunks in one query |
| R1 | `Rejected` | SQL-literal FUSE write batching | Sequential and random writes | Concatenate write work into SQL text to reduce requests |
| Q1 | `Accepted` | Strengthen amplification-counter provenance | Benchmark integrity | Reject counter deltas across every observed data-session replacement |
| Q2 | `Accepted` | Fold `pwrite` size lookup into the inode update | Writes, small files, Git commit | Remove one serial statement from each write without changing transaction shape |
| Q3 | `Accepted` | Return directory stats from `readdir_plus` | Metadata and Git traversal | Remove redundant `getattr` queries for `.` and `..` |
| Q4 | `Rejected` | Add an inode attribute cache with journal invalidation | Warm metadata and Git | Cache coherence complexity did not produce a repeatable request or latency win |
| Q5 | `Accepted` | Remove `create_file` existence pre-checks | Small files and Git commit | Let the unique dentry constraint detect `EEXIST` inside the transaction |
| Q6 | `Rejected` | Add a negative-entry cache | Git and missing-path probes | Synthetic request savings did not translate to latency or Git wins |
| Q7 | `Blocked` | Prime caches from `readdir_plus` | Warm metadata and Git status | Depends on Q4's rejected attribute cache |
| Q8 | `Rejected` | Reduce remote `fsync` round trips | Git commit and checkout | The presumed remote round trips do not occur |
| Q9 | `Accepted` | Execute remote `pwrite` as one Worker transaction | Sequential and random writes | Replace per-chunk remote statements with one compound Hrana operation |
| Q10 | `Accepted` | Execute remote create-family mutations atomically | Directory creation and Git | Replace multi-statement `mkdir`, `mknod`, `symlink`, and hard-link sequences with compound Worker transactions |
| Q11 | `Accepted` | Execute remote `truncate` atomically | Truncate-heavy file mutation | Move chunk trimming, zero extension, and inode metadata into one compound Worker transaction |

## Canonical Baseline

The canonical score-100 baseline is [`benchmarks/quick-baseline.json`](../benchmarks/quick-baseline.json), captured on 2026-07-19 from the historical control endpoint. It uses a 256 KiB chunk size and three samples.

| Dimension | Scenario | Baseline p50 | Baseline rate or operation time |
|---|---|---:|---:|
| Startup | Container startup | 4.03 s | Not applicable |
| Direct I/O | Direct 1 MiB read | 268 ms | 3.73 MiB/s |
| Direct I/O | Direct 1 MiB write | 858 ms | 1.17 MiB/s |
| FUSE sequential | FUSE 1 MiB read | 6.89 s client | 4.83 s operation |
| FUSE sequential | FUSE 1 MiB write | 7.36 s client | 5.40 s operation |
| FUSE random | 16 random 4 KiB reads | 11.02 s client | 2.05 operations/s |
| FUSE random | 16 random 4 KiB writes | 25.05 s client | 0.70 operations/s |
| Metadata | Walk 20 files after create | 15.04 s client | 1.53 entries/s |
| Metadata | Warm walk of 20 files | 11.55 s client | 2.06 entries/s |
| Small files | Create 10 files | 21.01 s client | 0.57 files/s |
| Git | Clean status | 33.39 s | 0.030 operations/s |
| Git | Add and commit | 72.32 s | 0.0138 commits/s |
| Git | Checkout | 46.17 s | 0.022 operations/s |

The baseline is useful for long-term scoring. Optimization decisions should also use a close-in-time control because deployed latency varies materially between windows.

## Measurement Rules and Known Hazards

### Correctness Gate

Every retained filesystem change must pass the deployed feature smoke. At the time of A4, the suite contained 33 checks covering direct changes, snapshots, uploads, execution, jobs, binary reads, unaligned cross-chunk reads, random writes verified through the direct API, directory traversal, directory attributes, negative lookup invalidation, concurrent reads, Hrana bridge reuse, and FUSE-origin change feeds.

Targeted unit tests are required for new edge cases, but they do not replace the deployed smoke.

### Benchmark Profiles

The `quick` profile is the iteration loop. It uses 1 MiB sequential files, 16 random operations, 20 metadata files, 10 small files, one Git file, a 256 KiB chunk size, and normally three samples.

The `full` profile is the final decision profile when the quick result is ambiguous or when an optimization changes a broad path. It uses 95 MiB sequential files, 2,048 random operations, 1,000 metadata files, and 1,000 small files.

### Decision Signals

Latency alone is insufficient. A credible result should have all of these properties:

- The target scenario improves in repeated deployed runs.
- The Hrana pipeline and SQL statement deltas match the proposed mechanism.
- Unrelated control scenarios do not regress materially.
- Direct HTTP behavior remains unchanged when only FUSE code changed.
- The deployed correctness gate passes.
- Results are compared in the same Cloudflare location, deployment configuration, profile, run count, and scenario selection.

### Integration Transport Instability

The integration endpoint intermittently returns HTTP 500, an empty response, or `AiryFSTransportError: fetch failed`. A run that terminates early is not benchmark evidence. Retry the complete run rather than combining partial samples.

### Container Image Rollback Behavior

Wrangler does not reliably switch the Container application back to an already-published older image when rebuilding an old source state. During the A4 A/B test, the purported control remained on the candidate image even though the Worker version changed. `wrangler containers info` exposed the still-active image tag.

To force a real close-in-time control, add a temporary unique Docker image label, build the accepted source state, deploy it, and verify the active application image changed. Do the same for the candidate. Remove the label from the worktree before committing. Never interpret a control run until the Container application image is independently verified.

### Amplification Counter Limitations

The benchmark records per-scenario Hrana pipeline and SQL statement deltas. It currently returns `null` when the session ID changes or a counter moves backward. A reset followed by enough activity to surpass the old counter can still look like a valid positive delta. Q1 exists to close this measurement gap.

## Accepted Experiments

## A1: Skip Remote Prepared-Statement Descriptions

**Status:** `Accepted`
**Commit:** `0786579` (`Add deployed performance benchmark`)
**Implementation:** [`agentfs/dependency-patches/libsql-0.9.30-skip-remote-describe.patch`](../agentfs/dependency-patches/libsql-0.9.30-skip-remote-describe.patch)

### Problem

libSQL 0.9.30's remote Hrana statement constructor called `stream.describe(&sql)` before executing each prepared statement. AgentFS binds parameters using libSQL's local SQL parser and does not consume the prepared statement's column metadata. The describe request therefore added a serial network round trip without changing execution or result decoding.

Remote AgentFS uses a single database connection. Serial request count dominates many FUSE operations, so removing one request from a frequently executed primitive has a multiplicative effect across reads, writes, metadata, small-file, and Git workloads.

### Change

The version-pinned dependency patch removes:

```rust
let desc = stream.describe(&sql).await?;
let cols: Vec<_> = desc.cols.into_iter().map(|col| col.name).collect();
```

It replaces the unused metadata with an empty column vector:

```rust
let cols = Vec::new();
```

The patch is applied by `agentfs/build.sh` before Rust compilation. Pinning it to libSQL 0.9.30 keeps the fork delta explicit and prevents the behavior from silently drifting during a dependency update.

### Correctness Validation

The integration deployment passed the then-current 30-check deployed feature suite. Query execution and result decoding remained unchanged because query responses still carry row data.

### Measured Results

The three-run quick score was 195.93 against the checked-in score-100 baseline and 229.12 against a fresh close-in-time control.

| Scenario | Fresh control p50 | Candidate p50 | Speedup |
|---|---:|---:|---:|
| FUSE 1 MiB read | 5.41 s operation | 2.40 s operation | 2.25x |
| FUSE 1 MiB write | 7.21 s operation | 2.63 s operation | 2.74x |
| 16 random 4 KiB reads | 7.90 s operation | 4.17 s operation | 1.90x |
| 16 random 4 KiB writes | 27.45 s operation | 13.00 s operation | 2.11x |
| Metadata walk after create | 17.40 s operation | 2.21 s operation | 7.86x |
| Create 10 small files | 20.74 s operation | 6.84 s operation | 3.03x |
| Git status | 38.77 s | 9.85 s | 3.94x |
| Git add and commit | 92.05 s | 24.58 s | 3.75x |

### Counter Caveat

Hrana deltas were unavailable for this comparison because the Container replaced its data bridge before every exec. The benchmark correctly emitted `null` instead of subtracting counters from different sessions. A2 fixed that lifecycle problem for later experiments.

### Decision

Retained. The improvement was broad, large, consistent with eliminating a serial request from every remote prepared statement, and correctness remained green.

## A2: Reuse a Healthy Hrana Bridge

**Status:** `Accepted`
**Commit:** `0786579` (`Add deployed performance benchmark`)
**Primary files:** `worker/src/hrana-server.ts`, `worker/src/index.ts`, and `e2e/benchmark.mjs`

### Problem

The Container lifecycle replaced the Hrana data bridge between exec calls even when the existing bridge remained healthy. Reconnection added avoidable setup work and changed the `/perf` session ID, which made per-scenario pipeline and statement deltas impossible to calculate safely.

This was both a runtime inefficiency and a measurement-integrity problem. A benchmark could report latency but could not verify that a proposed query optimization actually reduced remote requests.

### Change

The lifecycle now keeps a healthy bridge and reconnects only after a drop. The benchmark records the active data-session ID before and after each scenario and calculates deltas only when the session remains the same.

### Validation and Result

The deployed feature suite verifies that a bridge session is established, reused across execs, and that counters advance. Later accepted experiments produced numeric pipeline and statement deltas, enabling mechanism-level validation.

No standalone speedup was isolated for A2. Its retained value is lower reconnect overhead and trustworthy per-scenario amplification data.

### Decision

Retained as performance infrastructure. Do not conflate this with Q1: session reuse fixed routine reconnects, while Q1 addresses resets that occur during a sample and evade the current subtraction guard.

## A3: Adaptive FUSE `READDIRPLUS`

**Status:** `Accepted`
**Commit:** `bfad980` (`Enable adaptive FUSE readdirplus`)
**Implementation:** [`agentfs/patches/0011-enable-adaptive-readdirplus.patch`](../agentfs/patches/0011-enable-adaptive-readdirplus.patch)

### Problem

AgentFS already had a combined directory-entry and attribute implementation, but the FUSE mount did not advertise the ABI and capabilities required for Linux to use it adaptively. Directory enumeration was followed by many separate lookup or attribute requests, making metadata traversal and Git exceptionally slow.

### Change

The patch enables the FUSE 7.21 feature chain and advertises:

- `FUSE_DO_READDIRPLUS`
- `FUSE_READDIRPLUS_AUTO`

Linux can now choose the combined path when it predicts subsequent attribute lookups, while retaining plain `readdir` for scans where the extra attributes would be wasteful.

### Correctness Risks

Directory pagination, duplicate handling, entry names, inode attributes, and offsets must remain correct. A combined path can expose subtle bugs that plain `readdir` did not exercise.

### Correctness Validation

The integration deployment passed all 33 feature checks. The directory test traversed 257 entries and verified exact names, no duplicates, and correct file sizes.

### Measured Results

| Scenario | Comparison p50 | Candidate p50 | Change |
|---|---:|---:|---:|
| Metadata walk after create | 2.21 s operation | 0.50 s operation | 4.39x faster |
| Warm metadata walk | 11.55 s client canonical baseline | 1.20 s client | 9.65x faster |
| Git status | 9.85 s | 10.21 s | 3.6% slower |
| Git add and commit | 24.58 s | 18.78 s | 1.31x faster |
| Git checkout | 46.17 s canonical baseline | 13.72 s | 3.37x faster |

The median metadata walk used 38 Hrana pipelines and 19 SQL statements for 20 entries.

### Decision

Retained. The small Git status regression was outweighed by repeatable metadata, commit, and checkout improvements, with no correctness regressions.

### Remaining Opportunity

The path still performs redundant work for `.` and `..`, and it does not prime an inode attribute cache. Q3 and Q7 build on this accepted foundation.

## A4: Single-Query `pread`

**Status:** `Accepted`
**Commit:** `1d8a9ba` (`Collapse FUSE read queries`)
**Implementation:** [`agentfs/patches/0012-collapse-pread-queries.patch`](../agentfs/patches/0012-collapse-pread-queries.patch)

### Problem

Each `AgentFSFile::pread` first queried `fs_inode.size`, then issued a second query for the requested `fs_data` chunk range. Both requests were serialized over the one remote connection. Random reads amplified this cost across many calls.

### Change

The implementation calculates the requested chunk range and executes one query:

```sql
SELECT i.size, d.chunk_index, d.data
FROM fs_inode i
LEFT JOIN fs_data d
  ON d.ino = i.ino
 AND d.chunk_index >= ?
 AND d.chunk_index <= ?
WHERE i.ino = ?
ORDER BY d.chunk_index
```

The `LEFT JOIN` is essential. It returns the inode and its size even when the requested range contains no materialized chunk rows, preserving sparse-file and EOF behavior.

The implementation also returns immediately for zero-length reads and uses saturating offset arithmetic when calculating the initial end chunk.

### Correctness Risks

The combined result shape changed column indexes and row iteration. The implementation had to preserve:

- Zero-length reads.
- Reads at or beyond EOF.
- Reads clipped at EOF.
- Unaligned reads crossing chunk boundaries.
- Sparse gaps before, between, and after materialized chunks.
- Entirely missing requested chunk ranges.
- Binary data and null bytes.

### Correctness Validation

A new Rust test, `test_pread_sparse_across_missing_chunks`, writes four bytes into the third chunk and reads from near the end of the first chunk through the missing second chunk. It verifies the exact zero-filled result and payload position.

The complete local build passed 234 TypeScript tests, 113 non-ignored Rust SDK tests with the candidate test included, and 79 CLI tests. The integration deployment passed all 33 feature checks, including sequential binary and unaligned cross-chunk read checks.

### A/B Measurement Hazard

The first attempted control did not actually roll the Container application back from the candidate image. Its numbers were discarded as a control. Fresh unique image labels forced verified control and candidate deployments for the final comparison.

### Measured Results

| Scenario | Fresh control p50 | Candidate p50 | Change | Pipelines | SQL statements |
|---|---:|---:|---:|---:|---:|
| 16 random 4 KiB reads | 6.08 s operation | 4.80 s operation | 1.27x faster | 95 -> 63 | 51 -> 35 |
| FUSE 1 MiB read | 3.13 s operation | 2.63 s operation | 1.19x faster | 64 -> 45 | 35 -> 24 |
| 16 random 4 KiB writes | 20.12 s operation | 20.93 s operation | 4.0% slower | 313 -> 312 | 208 -> 207 |
| FUSE 1 MiB write | 4.50 s operation | 4.32 s operation | 4.0% faster | 104 -> 105 | 68 -> 69 |

The comparison tool reported a random group score of 107.05 and a sequential group score of 96.75. The sequential score included a slow third candidate sample that inflated p95 and p99; the median target operation still improved by 16.11%. Startup scored 95.08 and was unrelated to the code path. Overall score was 99.49 because the targeted report included startup and write controls in group scoring.

### Decision

Retained. Both target read workloads improved, request amplification dropped by the expected magnitude, write controls stayed within 4%, and correctness remained green.

## Rejected Experiments

## R1: SQL-Literal FUSE Write Batching

**Status:** `Rejected`
**Disposition:** Removed; accepted deployment restored
**Commit:** None

### Hypothesis

Random writes, small-file creation, and Git commit were the slowest canonical dimensions. The experiment attempted to reduce remote request count by encoding or concatenating FUSE write work into SQL literals so more work could execute in a batch.

### Why It Was Plausible

Remote AgentFS serializes statements through one connection. A write currently includes transaction control, an inode-size read, chunk mutations, an inode update, and commit. Reducing serial requests should help when network latency dominates.

### What Was Tried

The write path was changed to batch work using SQL text with literal values rather than retaining normal parameterized statement execution. This was an experiment, not the same design as Q2. Q2 removes one redundant statement while preserving parameter binding and the existing transaction structure.

### Measured Result

The representative FUSE 1 MiB write regressed from 2.63 s to 6.50 s, approximately 2.47x slower. Full benchmark runs also became unstable and could not produce a trustworthy broad win.

### Likely Failure Modes

The following are hypotheses, not isolated measurements:

- Constructing and parsing large SQL text may have outweighed saved request overhead.
- Literal encoding may have increased payload size and CPU work.
- Large statements may have interacted poorly with Hrana or SQLite execution limits.
- The changed batching boundary may have worsened transaction or chunk-write behavior.

### Decision

Rejected immediately. The experiment failed its primary sequential-write target and destabilized the full benchmark. The code was removed and the last accepted deployment was restored.

### Reusable Lesson

Do not re-propose SQL-literal batching without new evidence that directly addresses the 2.47x regression. Prefer small statement-elimination changes such as Q2 before changing representation or transaction boundaries.

## Queued Experiments

## Q1: Strengthen Amplification-Counter Provenance

**Status:** `Accepted`
**Priority:** First, because it improves confidence in every later experiment
**Risk:** Low
**Effort:** Low to medium
**Primary files:** `worker/src/hrana-server.ts`, `worker/src/index.ts`, `e2e/benchmark.mjs`, `e2e/benchmark-lib.mjs`

### Current Behavior

The Hrana server exposes `sessionId`, `pipelineCount`, and `statementCount` through `/perf`. The benchmark reads counters before and after a scenario. `counterDelta` returns `null` when the session ID changes or the later counter is lower than the earlier counter.

### Measurement Gap

A session can reset during a sample and then process enough requests to exceed the old counter value. The session ID or epoch used by the two snapshots must expose that reset. Subtracting monotonically-looking values without reset provenance can produce a plausible but incorrect positive delta.

### Proposed Change

Expose a never-ambiguous epoch or reset counter with every `/perf` snapshot. Increment it whenever the active data session is replaced. The benchmark should emit `null` for pipeline and statement deltas whenever the epoch differs between snapshots, regardless of the numeric counter values.

An alternative is a process-lifetime cumulative counter that never resets, but it still needs a clearly defined lifecycle and overflow behavior. An explicit epoch is easier to reason about.

### Expected Result

No filesystem latency win is required. The acceptance signal is that hermetic tests cover stable sessions, resets with lower counters, and resets whose new counters surpass old values. Existing deployed runs should continue to report numeric deltas when no reset occurs.

### Change

The Worker now exposes `sessionEpoch` with every `/perf` response. The epoch increments whenever a successfully opened data socket installs a new `HranaServer`. It is paired with the random session UUID so a Durable Object isolate restart also changes provenance even though its in-memory epoch starts over.

The benchmark now reports a counter delta only when the before and after snapshots have the same non-null session UUID and the same non-negative integer epoch. Counter monotonicity remains an additional guard.

### Validation Plan

- Add unit tests for all reset and no-reset combinations.
- Run benchmark script tests.
- Deploy and verify `/perf` fields.
- Run a targeted benchmark and confirm numeric deltas remain available.
- Deliberately force or simulate a reconnect and confirm the affected sample reports `null`.

### Decision Record

Result: All local suites passed: 343 Worker tests, 11 SDK tests, 226 CLI tests, and 23 script tests. The integration deployment passed 39 feature checks. The smoke test destroyed and restarted the Container, then verified that the session UUID changed, the epoch advanced, and the cross-reconnect delta was `null`.

A one-run targeted random-I/O benchmark retained numeric uninterrupted-session counters: 60 pipelines and 32 statements for 16 random reads, and 312 pipelines and 207 statements for 16 random writes. Raw report: `/tmp/q1-session-epoch.json`.

Decision: Accepted. No latency change was required or attributed to this instrumentation-only experiment.

## Q2: Fold `pwrite` Size Lookup into the Inode Update

**Status:** `Accepted`
**Priority:** Highest expected runtime benefit among low-risk ideas
**Risk:** Low
**Effort:** Low
**Primary function:** `AgentFSFile::pwrite` in the patched AgentFS Rust SDK
**Implementation:** [`agentfs/patches/0013-collapse-pwrite-size-query.patch`](../agentfs/patches/0013-collapse-pwrite-size-query.patch)

### Current Path

Each `pwrite` currently performs work equivalent to:

1. `BEGIN IMMEDIATE`.
2. `SELECT size FROM fs_inode WHERE ino = ?`.
3. Insert or replace affected chunk rows.
4. `UPDATE fs_inode SET size = ?, mtime = ?, mtime_nsec = ? WHERE ino = ?`.
5. `COMMIT`.

The size query exists only to compute `max(current_size, offset + data_length)`. It adds one serial round trip to every write.

### Proposed Change

Remove the pre-read and express the invariant in the update:

```sql
UPDATE fs_inode
SET size = MAX(size, ?),
    mtime = ?,
    mtime_nsec = ?
WHERE ino = ?
```

Bind the write end offset as the candidate size. Preserve the existing parameterized chunk statements and transaction boundary.

### Why This Is Not R1

This change does not concatenate SQL, encode data as literals, combine multiple FUSE writes, or alter commit boundaries. It removes one redundant statement from the current transaction. It is the write-side analogue of A4.

### Expected Signal

Estimated, not measured: one fewer SQL statement per `pwrite`. Remote preparation and execution can make that approximately two fewer pipelines, potentially around a 20% reduction for simple writes. The exact percentage depends on chunk count and transaction implementation.

Primary benchmark targets:

- `fuse_random_write_4k`
- `fuse_sequential_write_1m`
- `fuse_small_file_create`
- `git_add_commit`

Read, metadata, and direct HTTP scenarios are controls.

### Correctness Risks

- In-place overwrite within EOF must update mtime without shrinking size.
- Extending writes must grow size to the exact write end.
- Sparse writes beyond EOF must preserve zero-filled gaps.
- Cross-chunk writes must retain exact bytes.
- Zero-length write behavior must remain unchanged.
- Integer overflow at `offset + length` must retain current handling.

### Validation Plan

- Add an in-place overwrite test proving size is unchanged and mtime advances.
- Retain existing extend, sparse, cross-chunk, and round-trip tests.
- Run all local suites and the deployed feature checks.
- Run a three-sample targeted quick A/B with sequential and random writes.
- Include small-file and Git commit scenarios before acceptance.
- Confirm the statement and pipeline reduction matches one eliminated size lookup per observed write call.

### Correctness Validation

The patch adds an in-place overwrite assertion to `test_pwrite_basic`. It verifies that a second write advances mtime without changing file size. Existing tests retain coverage for extension, sparse gaps, cross-chunk writes, empty writes, and byte-for-byte round trips.

The complete AgentFS build passed 234 TypeScript tests, 113 non-ignored Rust SDK tests plus its doc test, and 79 CLI tests. The candidate deployment passed all 39 integration feature checks.

### Measured Results

The verified control image was `63c6cb2b`; the verified candidate image was `9f712319`. Both reports used the quick profile, three samples, a 256 KiB chunk size, and the same scenario selection.

| Scenario | Control p50 | Candidate p50 | Change | Pipelines | SQL statements |
|---|---:|---:|---:|---:|---:|
| 16 random 4 KiB writes | 31.72 s | 15.65 s | 2.03x faster | 315 -> 250 | 210 -> 176 |
| FUSE 1 MiB write | 8.10 s | 3.60 s | 2.25x faster | 107 -> 88 | 71 -> 60 |
| Create 10 small files | 26.60 s | 11.90 s | 2.24x faster | 410 -> 353 | 263 -> 234 |
| Git add and commit | 120.32 s | 42.30 s | 2.84x faster | 1,661 -> 1,247 | 934 -> 718 |

The approximately two-pipeline reduction for each eliminated statement reflects remote statement preparation plus execution. Different workloads issue different numbers of `pwrite` calls, but their pipeline reduction remains close to twice their statement reduction.

The candidate also improved read, startup, Git status, and checkout controls substantially. That broad movement indicates a faster candidate measurement window, so the full 2.03x to 2.84x target latency improvement cannot be attributed solely to Q2. The request-count reduction is deterministic, matches the mechanism, and no control regressed.

Raw reports: `/tmp/q2-control.json` and `/tmp/q2-candidate.json`. Two earlier control attempts and one earlier candidate attempt ended with integration transport errors and were discarded in full.

### Decision Record

Result: Repeatable target latency improvement with the predicted request-amplification reduction and no correctness or control regression.
Decision: Accepted. Retain the parameterized single-statement inode update and existing transaction boundary.

## Q3: Return Directory Stats from `readdir_plus`

**Status:** `Accepted`
**Priority:** High
**Risk:** Low to medium
**Effort:** Low to medium
**Primary functions:** FUSE `readdirplus` handler and SDK `readdir_plus`

### Current Path

The SDK `readdir_plus` path already queries the directory and builds child `Stats`. The FUSE handler separately asks for stats for `.` and `..`, producing up to two additional `getattr` round trips for each remote `readdirplus` call.

### Implemented Change

Added `DirectoryListing` and `readdir_plus_with_directory_stats` while retaining the existing `readdir_plus` API. The AgentFS override now uses one CTE to return the current directory, actual parent, and child stats. The FUSE handler consumes those returned stats for `.` and `..` instead of issuing separate `getattr` calls.

Root and nested-directory tests verify the parent identity and attributes. Pagination, entry ordering, and adaptive fallback behavior remain unchanged.

### Expected Signal

Matched three-run metadata-only measurements compare Q2 image `9f712319` with Q3 image `bfebcdbf`:

| Scenario | Q2 operation p50 | Q3 operation p50 | Q2 -> Q3 pipelines | Q2 -> Q3 SQL statements | Client p50 |
|---|---:|---:|---:|---:|---:|
| Metadata walk after create | 982.69 ms | 816.42 ms | 38 -> 28 | 19 -> 15 | 2,065.81 -> 2,147.67 ms |
| Warm metadata walk | 854.42 ms | 598.94 ms | 38 -> 22 | 19 -> 11 | 1,905.62 -> 1,671.36 ms |

The target operation improved 1.20x after create and 1.43x warm. Warm client latency improved 1.14x. After-create client p50 moved 4.0% slower while its p95 improved 12.7%, consistent with integration variance rather than a mechanism regression. Pipeline counts fell 26.3% after create and 42.1% warm; SQL counts fell 21.1% and 42.1%. The approximately two-pipeline reduction per eliminated statement matches remote preparation plus execution.

Raw reports: `/tmp/q3-matched-control.json` and `/tmp/q3-candidate-metadata.json`. Four complete metadata-plus-Git candidate attempts were discarded: three hit integration transport or HTTP 500 failures, and one exceeded the harness timeout before writing its report.

Primary targets:

- Metadata walk after create.
- Warm metadata walk.
- Git status.
- Git checkout.

### Correctness Risks

- `.` must use the current directory inode and attributes.
- `..` must identify the correct parent, including root behavior.
- ENOENT and ENOTDIR behavior must remain unchanged.
- Pagination offsets and entry ordering must not change.
- Adaptive fallback to plain `readdir` must remain valid.

### Validation Plan

- Add focused tests for root and nested directory `.` and `..` attributes.
- Retain the 257-entry deployed traversal check.
- Run metadata and Git targeted benchmarks.
- Confirm exactly the predicted per-call request reduction.

### Decision Record

Result: Deterministic request and statement reductions, improved target operation latency, correct root and nested-parent behavior, and all 44 deployed checks passed after the expanded coherence gate.
Decision: Accepted. Retain directory and parent stats in the combined `readdir_plus` query.

## Q4: Inode Attribute Cache with Journal Invalidation

**Status:** `Rejected`
**Priority:** High after Q1-Q3
**Risk:** Medium to high
**Effort:** Medium to high
**Primary areas:** SDK caches, FUSE deferred notifier, remote mutation journal poller

### Current Behavior

AgentFS has a dentry name-to-inode LRU, but no inode attribute cache. `getattr` always queries the database. A lookup that hits the dentry cache still issues a stats query. Repeated metadata walks and Git operations therefore pay remote latency for hot inode attributes.

The remote mutation journal already carries `(seq, parent_ino, name, ino)`. The poller uses the parent and name to invalidate directory entries but discards the inode. The low-level notifier supports inode invalidation, while the deferred notifier currently exposes only entry invalidation.

### Proposed Change

Add a bounded TTL attribute cache keyed by inode. Populate it from successful `getattr`, lookup, and eventually `readdir_plus` results. Evict on every local mutation that changes attributes or removes identity, including write, truncate, chmod, chown, utimens, unlink, rename, and overwrite.

Extend deferred notification with an inode invalidation operation. Consume the journal's inode field to evict the SDK cache and notify the kernel when a direct API mutation changes an inode.

Retain a short TTL as the safety bound. Active invalidation can fail if the poller cannot start, so correctness cannot depend exclusively on notifications.

### Expected Signal

The largest expected wins are repeated hot metadata operations:

- Warm metadata walk.
- Git status.
- Git checkout.
- Git add and commit.

Expected counter behavior is fewer stats queries after the first access. The exact reduction depends on kernel caching and the selected TTL.

### Correctness Risks

- Direct API writes, chmod, rename, and delete must become visible to a mounted Container.
- Local FUSE mutations must not leave stale attributes in either user-space or kernel caches.
- In-place rewrites must invalidate size, mtime, and page state correctly.
- Rename-over and unlink with open handles must preserve inode lease semantics.
- Poller startup or backlog failures must self-heal through TTL expiry.
- Cache memory must be bounded.

### Validation Plan

- Add unit tests for cache hit, expiry, local mutation eviction, and remote journal eviction.
- Add or extend deployed checks for direct API truncate, overwrite, chmod, rename, and delete visibility through FUSE.
- Preserve the negative lookup invalidation test.
- Benchmark cold and warm walks separately.
- Compare Git status before and after cache warmup.
- Record cache hit and miss counts if available; do not infer cache effectiveness from latency alone.

### Candidate Change (Removed)

The removed candidate was `agentfs/patches/0015-cache-inode-attributes-with-journal-invalidation.patch`.

- Adds `StatsCache`, a bounded LRU keyed by inode with a one-second safety TTL, shared across `AgentFS` clones by `Arc`. The TTL bounds staleness if active invalidation is ever missed; local eviction and the journal poller keep it coherent otherwise.
- Populates the cache from `getattr` and `lookup`. It deliberately does not populate from `readdir_plus`; that priming is Q7.
- Evicts on every local mutation that changes or removes an inode: `pwrite`, `truncate`, `chmod`, `chown`, `utimens`, the create family (evicting the changed parent), `unlink` and `rmdir` (parent and target), `link` (parent and target), and `rename` (both parents, the moved inode, and any overwritten destination).
- Exposes `AgentFS::stats_cache()` so the shared handle is captured in `cmd::mount` before the filesystem is type-erased to `dyn FileSystem`, then threaded through `fuse::mount` into the poller. This is the smallest surface that lets the poller — which runs on an independent `AgentFS` connection — evict the mounted instance's cache.
- Extends the deferred notifier with `inval_inode`, routed through the existing notify thread to avoid the `/dev/fuse` deadlock. The mutation-journal poller now consumes the journal's inode field to evict the SDK cache through the shared handle and queue a kernel attribute invalidation, in addition to the existing directory-entry invalidation.

### Correctness Validation

Added Rust unit tests, all passing under the ordered build:

- `test_stats_cache_hit_then_expiry`, `test_stats_cache_invalidate_and_bound`: hit, TTL expiry, explicit invalidation, and LRU bounding.
- `test_getattr_populates_stats_cache`, `test_lookup_populates_stats_cache`: population sources.
- `test_pwrite_evicts_stats_cache`, `test_chmod_evicts_stats_cache`, `test_unlink_evicts_parent_and_target`, `test_rename_over_evicts_destination`: representative local mutation eviction, including the overwritten rename destination.
- `test_shared_cache_handle_evicts_mounted_instance`: eviction through the shared handle (the poller's path) reaches attributes served by the mount.

The full candidate build passed 234 TypeScript tests, 123 Rust SDK tests plus one doc test, and 80 CLI tests. All 44 deployed checks passed, including direct API overwrite, truncate, chmod, rename, and delete visibility through the mounted filesystem.

### Measurements

Matched three-run metadata-only measurements:

| Scenario | Q3 operation p50 | Q4 operation p50 | Q3 -> Q4 pipelines | Q3 -> Q4 SQL statements | Client p50 |
|---|---:|---:|---:|---:|---:|
| Metadata walk after create | 211.04 ms | 636.51 ms | 22 -> 20 | 11 -> 10 | 1,321.93 -> 1,634.68 ms |
| Warm metadata walk | 496.20 ms | 646.99 ms | 22 -> 20 | 11 -> 10 | 1,506.78 -> 1,564.83 ms |

The candidate saved one SQL statement and two pipelines per walk, but operation latency regressed 3.02x after create and 30.4% warm. Client latency regressed 23.7% and 3.9% respectively.

Matched three-run Git measurements:

| Scenario | Q3 p50 | Q4 p50 | Latency change | Q3 -> Q4 pipelines | Q3 -> Q4 SQL statements |
|---|---:|---:|---:|---:|---:|
| Git status | 20.01 s | 26.86 s | 34.2% slower | 541 -> 569 | 304 -> 318 |
| Git add and commit | 46.87 s | 64.93 s | 38.5% slower | 1,241 -> 1,331 | 715 -> 764 |
| Git checkout | 31.42 s | 43.52 s | 38.5% slower | 820 -> 871 | 474 -> 500 |

A one-sample Git screen initially showed lower request counts, but the complete runs reversed that signal across every workload. The cache did not produce a repeatable amplification win and added mutex, TTL, mutation-eviction, and cross-instance invalidation complexity.

Raw reports: `/tmp/q4-control-metadata.json`, `/tmp/q4-candidate-metadata.json`, `/tmp/q4-control-git.json`, and `/tmp/q4-candidate-git.json`.

### Decision Record

Result: Correctness passed, but complete metadata and Git runs showed latency regressions and no repeatable request-count reduction.
Decision: Rejected. Remove the inode attribute cache and retain Q3 image `bfebcdbf`.

## Q5: Remove `create_file` Existence Pre-Checks

**Status:** `Accepted`
**Priority:** Medium-high
**Risk:** Medium
**Effort:** Medium
**Primary functions:** the path-based and `FileSystem` `create_file` implementations

### Current Behavior

Both `create_file` implementations called `lookup_child` before beginning the mutation transaction. For the common successful-create path, that query had to miss and therefore added a wasted serial round trip. The transaction then inserted the dentry.

### Implemented Change

Every Rust, TypeScript, and Worker schema initialization path enforces a unique `(parent_ino, name)` dentry constraint. Patch `0015-remove-create-file-existence-precheck.patch` removes the pre-check from both `create_file` implementations and inserts the dentry with `ON CONFLICT(parent_ino, name) DO NOTHING`. A zero-row insert maps to `EEXIST`; returning before commit rolls back the newly allocated inode.

The experiment deliberately leaves `mkdir`, `mknod`, `symlink`, and `link` unchanged. Their current mutation sequences are not transactional, so handling a late dentry conflict could leak partial state.

### Measured Signal

The removed miss query reduced request amplification on successful creates. The measured targets were:

- Create 10 small files.
- Metadata fixture creation.
- Git add and commit, especially loose object and lock-file creation.

### Correctness Risks

- Concurrent creates of the same name must produce one success and one `EEXIST`.
- The error mapper must not turn unrelated constraint failures into `EEXIST`.
- Overlay, rename, link, and whiteout semantics must not rely on duplicate dentries.
- Existing databases must have the required constraint.
- Transaction rollback must leave no orphan inode when dentry insertion fails.

### Validation and Measurements

- Clean patch-series rebuild: 234 TypeScript SDK tests, 116 Rust SDK tests plus one doc test, and 79 Rust CLI tests passed.
- Duplicate and concurrent same-name create tests passed. Concurrent creation produced one success and one `EEXIST`, with no orphan inode.
- All 45 deployed feature checks pass on the retained Q5 stack.

Matched three-run small-file measurements compare Q3 image `bfebcdbf` with Q5 image `dffca22e`:

| Scenario | Q3 operation p50 | Q5 operation p50 | Latency change | Q3 -> Q5 pipelines | Q3 -> Q5 SQL statements | Client p50 |
|---|---:|---:|---:|---:|---:|---:|
| Create 10 small files | 25.85 s | 17.50 s | 32.3% faster | 381 -> 358 | 249 -> 236 | 29.78 -> 20.59 s |

Matched three-run Git measurements reuse the Q3 control captured for Q4:

| Scenario | Q3 p50 | Q5 p50 | Latency change | Q3 -> Q5 pipelines | Q3 -> Q5 SQL statements |
|---|---:|---:|---:|---:|---:|
| Git status | 20.01 s | 17.68 s | 11.6% faster | 541 -> 528 | 304 -> 297 |
| Git add and commit | 46.87 s | 36.77 s | 21.5% faster | 1,241 -> 1,186 | 715 -> 687 |
| Git checkout | 31.42 s | 25.04 s | 20.3% faster | 820 -> 793 | 474 -> 458 |

The first Git candidate run failed during sample two with an integration transport error and was discarded. Raw complete reports: `/tmp/q5-control-small-files.json`, `/tmp/q5-candidate-small-files.json`, `/tmp/q4-control-git.json`, and `/tmp/q5-candidate-git.json`.

### Decision Record

Result: Correctness passed, deterministic request counts fell, and all target workload p50 latencies improved.
Decision: Accepted. Retain patch `0015-remove-create-file-existence-precheck.patch` and image `dffca22e`.

## Q6: Negative-Entry Cache

**Status:** `Rejected`
**Priority:** Medium
**Risk:** Medium to high
**Effort:** Medium
**Primary functions:** lookup paths and mutation-journal invalidation

### Current Behavior

Successful names can enter the dentry cache, but `Ok(None)` results are not cached. Git and common tooling repeatedly probe paths that do not exist, including configuration files, lock files, optional metadata, and executable candidates. Every probe can become a remote query.

### Proposed Change

Cache negative `(parent_ino, name)` results with a short TTL. Evict a negative immediately on every local create, mkdir, mknod, symlink, link, or rename into that name. Evict it when the remote mutation journal reports the same parent and name.

Consider bypassing or synchronously revalidating the negative cache for `O_CREAT`, `O_EXCL`, and lock-file operations. Correct creation and locking semantics are more important than eliminating one lookup.

### Expected Signal

Potential targets:

- Git status.
- Git add and commit.
- Small-file workloads with repeated optional-name probes.
- Any future package-manager benchmark.

The mechanism should reduce repeated miss queries for the same key. Add miss-cache counters if practical.

### Correctness Risks

- A direct API create must invalidate a previously cached FUSE miss.
- A stale negative for `index.lock` or another lock file can violate process coordination.
- Rename into a negatively cached name must become visible immediately.
- Poller failure requires a short TTL safety bound.
- Cache keying must include parent inode, not only name.

### Validation Plan

- Extend the deployed negative lookup check to cover direct create, rename into name, and delete/recreate.
- Add lock-file create/exclusive-create tests.
- Verify TTL expiration without notifications.
- Benchmark repeated missing-path probes separately before relying on Git aggregate results.

### Decision Record

The minimal candidate returned a one-second, zero-inode FUSE entry for a missing name. It reused kernel dentry invalidation rather than adding an AgentFS cache: successful local mutation replies replace cached misses, and the existing remote mutation-journal poller invalidates them after direct mutations.

The dedicated 20-lookup benchmark produced the expected deterministic request reduction, from 52 to 14 pipelines and from 26 to 7 SQL statements. Timing did not repeat: the first candidate operation p50 regressed from 103.06 ms to 152.48 ms, while a repeat improved to 88.12 ms but still regressed client p50 from 369.33 ms to 470.01 ms.

Matched three-run Git measurements compared Q5 image `dffca22e` with Q6 image `551a795b`:

| Scenario | Q5 p50 | Q6 p50 | Latency change | Q5 -> Q6 pipelines | Q5 -> Q6 SQL statements |
|---|---:|---:|---:|---:|---:|
| Git status | 17.68 s | 24.36 s | 37.8% slower | 528 -> 527 | 297 -> 300 |
| Git add and commit | 36.77 s | 63.28 s | 72.1% slower | 1,186 -> 1,275 | 687 -> 734 |
| Git checkout | 25.04 s | 37.67 s | 50.5% slower | 793 -> 813 | 458 -> 470 |

All 45 deployed correctness checks passed, including direct create, rename, and delete/recreate invalidation of cached misses. Correctness was not the rejection reason.

Raw reports: `/tmp/q6-control-negative-lookups.json`, `/tmp/q6-candidate-negative-lookups.json`, `/tmp/q6-candidate-negative-lookups-repeat.json`, `/tmp/q5-candidate-git.json`, and `/tmp/q6-candidate-git.json`.

Result: Synthetic amplification improved, but repeated timing was inconsistent and every Git target regressed substantially.
Decision: Rejected. Remove the negative-entry runtime patch and restore the accepted Q5 stack. Container version 20, image `29b7b1f0`, contains the Q5 binary plus image metadata used to force the rollback past registry deduplication.

## Q7: Prime Caches from `readdir_plus`

**Status:** `Blocked`
**Priority:** Medium; should follow Q4
**Risk:** Medium
**Effort:** Low after Q4, medium as a standalone change
**Primary function:** SDK `readdir_plus`

### Current Behavior

`readdir_plus` already materializes child inode stats for the kernel response, but those values are not reused by later SDK-level `getattr` or lookup operations. The filesystem can pay for attributes once during enumeration and then query them again.

### Proposed Change

Insert every successful child result into the dentry cache and the Q4 attribute cache while constructing the `readdir_plus` response. Use the same TTL and invalidation rules as normal lookup and `getattr` population.

This should not create a separate cache implementation. It is a population strategy built on Q4.

### Expected Signal

- Lower request count on a metadata walk immediately following enumeration.
- A larger difference between cold and warm metadata passes.
- Potential improvement in Git status when Git revisits entries enumerated through `READDIRPLUS`.

### Correctness Risks

The risks are the same as Q4: stale attributes and names after local or direct mutations. Additional risks are partial directory pages and duplicate population. Cache insertion must tolerate pagination and repeated entries without changing enumeration behavior.

### Validation Plan

- Prove cache population for a paginated directory.
- Mutate an enumerated entry through both FUSE and direct APIs and confirm immediate or TTL-bounded visibility.
- Benchmark cold walk, warm walk, and Git status.
- Verify request reduction rather than relying only on latency.

### Decision Record

Result: Not run.
Decision: Blocked because Q4's attribute cache was rejected and removed.

## Q8: Reduce Remote `fsync` Round Trips

**Status:** `Rejected`
**Priority:** Deferred
**Risk:** High
**Effort:** Medium implementation, high validation
**Primary functions:** file and filesystem `fsync`

### Current Behavior

The original hypothesis assumed the remote `fsync` path performed four statements:

```sql
PRAGMA synchronous=FULL;
BEGIN;
COMMIT;
PRAGMA synchronous=OFF;
```

The current implementation no longer sends that sequence. It attempts only `PRAGMA synchronous=FULL` followed by `PRAGMA synchronous=OFF`, ignores both errors, and returns success. Remote libSQL rejects valued PRAGMAs client-side; if one reached the Worker, the Hrana server would filter it as a no-op. No transaction or durability boundary occurs between the two attempts.

### Investigation Result

An optional deployed `fsync` diagnostic compares opening and closing a primed file with calling `fsync` 20 times on an equivalent file. Twenty calls took 0.57 ms operation p50 in the matched run. The expected 40 extra PRAGMA statements and pipelines did not appear:

| Scenario | Operation p50 | Client p50 | Pipelines | SQL statements |
|---|---:|---:|---:|---:|
| Open and close, zero `fsync` calls | 0.04 ms | 251.55 ms | 14 | 9 |
| Open, 20 `fsync` calls, and close | 0.57 ms | 263.28 ms | 18 | 11 |

The small fixed counter difference is not linear in the 20 calls and can include mount invalidation or lease activity. A prior three-run diagnostic measured 0.98 ms p50 for the same 20 calls. Both results rule out serial remote round trips as a material Git bottleneck.

Raw reports: `/tmp/q8-fsync-control.json` and `/tmp/q8-fsync-matched.json`. The first matched attempt failed with the known integration transport error and was discarded.

### Expected Signal

Removing the two rejected parser calls could save only tens of microseconds per `fsync`. Distinguishing local and remote connection behavior would add more code than this deployed benefit justifies, and it would not establish stronger durability semantics.

### Correctness and Durability Risks

- A successful fsync must survive Container destruction and remount.
- Git history, refs, index, and object contents must survive abrupt process and Container termination.
- Error reporting must remain synchronous.
- A local SQLite assumption must not be projected onto remote libSQL without proof.

### Correctness Finding

The Worker Hrana server filters explicit `BEGIN`, `COMMIT`, `ROLLBACK`, `SAVEPOINT`, and `RELEASE` statements because Durable Object SQLite does not support them through this bridge. Rust mutation paths still create nominal libSQL transactions, but their explicit transaction control does not establish the intended remote atomic boundary. This is a correctness and durability investigation, not evidence for an `fsync` performance change. Do not claim remote multi-statement mutations are atomic until fault injection proves it or the bridge implements a supported transaction mechanism.

### Decision Record

Result: The hypothesized remote `fsync` amplification does not exist; 20 calls complete in under 1 ms of measured operation time.
Decision: Rejected without a runtime candidate. Retain Q5 and investigate remote mutation atomicity separately.

## Q9: Atomic Compound Remote `pwrite`

**Status:** `Accepted`
**Priority:** High
**Risk:** High
**Effort:** High
**Primary files:** `agentfs/patches/0016-atomic-remote-pwrite.patch`, `worker/src/hrana-server.ts`, and `worker/test/hrana-server.test.ts`

### Problem

Remote `pwrite` performed chunk reads, chunk replacements, and the inode size and timestamp update as separate Hrana operations. The nominal Rust transaction did not provide a remote atomic boundary because explicit transaction-control statements are filtered by the Worker. A 16-operation random-write sample used 247 pipelines and 175 SQL statements, while a 1 MiB sequential write used 83 pipelines and 58 statements.

### Implemented Change

Patch `0016-atomic-remote-pwrite.patch` marks remote connection pools and sends remote writes through one six-argument compound operation. Local writes continue to use normal libSQL transactions, now consistently executing chunk and metadata statements through the transaction handle.

The Worker strictly recognizes libSQL's normalized `SELECT airyfs_pwrite_v1 (?, ?, ?, ?, ?, ?);` wire form. It validates arguments, verifies the inode, reads and updates all affected chunks, and updates inode size and timestamps inside one Durable Object `transactionSync()` callback. The response returns the byte count and the Rust query path consumes that result.

The implementation deliberately does not infer inode existence from `rowsWritten`. Durable Object SQLite includes trigger work in write counters, so a successful `fs_inode` update can report more than one written row.

### Correctness Validation

- A clean patch-series rebuild passed 234 TypeScript SDK tests, 117 Rust SDK tests including one ignored encryption test, one Rust SDK doc test, and 79 Rust CLI tests.
- All 347 Worker tests and 23 repository script tests passed; Worker typechecking passed.
- Worker protocol tests cover cross-chunk writes, successful commit, and rollback when the metadata update fails.
- All 46 deployed feature checks passed, including unaligned writes within and across 256 KiB chunks verified byte-for-byte through the direct API.
- Three earlier full-gate attempts passed the compound-write checks and then encountered the known integration transport failure at unrelated later checks. Those incomplete runs were discarded.

### Measurements

Matched three-run quick reports compare Q5 Container image `29b7b1f0` with Q9 image `62c6430e`. The Worker retained for the final measurement was version `4c3c023c`.

| Scenario | Q5 operation p50 | Q9 operation p50 | Latency change | Q5 -> Q9 pipelines | Q5 -> Q9 SQL statements | Client p50 change |
|---|---:|---:|---:|---:|---:|---:|
| 16 random 4 KiB writes | 20.33 s | 4.57 s | 77.5% faster | 247 -> 87 | 175 -> 46 | 22.75 -> 6.46 s |
| FUSE 1 MiB sequential write | 5.30 s | 2.41 s | 54.5% faster | 83 -> 41 | 58 -> 24 | 6.57 -> 3.92 s |
| 16 random 4 KiB reads | 5.20 s | 5.30 s | 1.9% slower | 57 -> 58 | 31 -> 32 | 7.77 -> 7.54 s |
| FUSE 1 MiB sequential read | 2.87 s | 2.90 s | 1.1% slower | 42 -> 41 | 23 -> 22 | 4.73 -> 4.56 s |

The overall quick score was 128.13 against the matched control. The random workload group scored 191.56 and the sequential group scored 135.79. Read operation latency remained effectively flat while read client latency improved slightly.

Raw reports: `/tmp/compound-write-control.json` and `/tmp/compound-write-candidate.json`.

### Decision Record

Result: Exact deployed write correctness passed repeatedly, write amplification fell by roughly half to three quarters, and target latency improved 54.5% to 77.5% without a material read regression.
Decision: Accepted. Retain patch `0016-atomic-remote-pwrite.patch`, Worker compound-operation support, Container version 22, and image `62c6430e`. Continue migrating truncate and rename separately; this change does not make those paths atomic.

## Q10: Atomic Compound Remote Create Operations

**Status:** `Accepted`
**Priority:** High
**Risk:** High
**Effort:** High
**Primary files:** `agentfs/patches/0017-atomic-remote-create-operations.patch`, `worker/src/hrana-server.ts`, `worker/test/hrana-server.test.ts`, and `e2e/features.mjs`

### Problem

Remote `mkdir`, `mknod`, `symlink`, and hard-link operations issued inode, dentry, link-count, symlink-target, and parent-metadata mutations as separate Hrana operations. Their nominal libSQL transactions did not establish a remote atomic boundary because the Worker filters explicit transaction-control statements. A conflict or failure could therefore leave partial filesystem state. Q5 intentionally left these operations unchanged for that reason.

### Implemented Change

The Worker now strictly recognizes normalized `airyfs_create_node_v1` and `airyfs_link_v1` compound operations. It validates every argument and executes each complete mutation inside one Durable Object `transactionSync()` callback. Positive results return the created or linked inode; status `0` maps to `EEXIST`, `-1` to `ENOENT`, and hard-link status `-2` to `EISDIR`.

Patch `0017-atomic-remote-create-operations.patch` routes both path-based and `FileSystem` remote implementations through those procedures, populates the dentry cache only after success, and constructs the returned create metadata from the committed operation. Local SQLite behavior remains unchanged.

### Correctness Validation

- A clean patch-series rebuild passed 234 TypeScript SDK tests, 117 Rust SDK tests including one ignored encryption test, one Rust SDK doc test, and 79 Rust CLI tests.
- All 360 Worker tests, 20 Container tests, 11 public TypeScript SDK tests, and 23 repository script tests passed; Worker typechecking passed.
- The 36 focused Worker protocol tests cover successful creates and links, duplicate destinations without mutation, invalid parents and sources, directory hard-link rejection, and injected rollback failures.
- All 53 deployed feature checks passed. The added FUSE checks verify directory and regular-node types and modes, symlink targets, hard-link inode identity, and link counts.
- Two earlier full-gate attempts passed every create-family check and then encountered known integration transport failures at unrelated later checks. Those incomplete runs were discarded.

### Measurements

Matched three-run quick reports compare Q9 image `62c6430e` with Q10 image `a454c246`. The comparison isolates the Rust branches while retaining the same Worker compound-procedure implementation in both runs.

| Scenario | Q9 operation p50 | Q10 operation p50 | Latency change | Q9 -> Q10 pipelines | Q9 -> Q10 SQL statements | Client p50 change |
|---|---:|---:|---:|---:|---:|---:|
| Create 10 small files | 16.00 s | 14.83 s | 7.3% faster | 310 -> 308 | 198 -> 196 | 19.52 -> 16.95 s |
| Git status | 36.69 s | 29.96 s | 18.3% faster | 633 -> 590 | 351 -> 329 | Same as operation |
| Git add and commit | 85.03 s | 66.98 s | 21.2% faster | 1,396 -> 1,314 | 784 -> 739 | Same as operation |
| Git checkout | 53.01 s | 43.83 s | 17.3% faster | 880 -> 858 | 496 -> 485 | Same as operation |

The targeted comparison scored 118.77 for small files and 124.52 for Git. Request counts fell in every scenario, matching the compound-operation mechanism. The small-file workload primarily uses regular `create_file`, so its two-pipeline reduction comes from the surrounding directory setup rather than changing regular-file creation itself.

Raw reports: `/tmp/q10-control.json` and `/tmp/q10-candidate.json`. Two control attempts failed during setup with known integration HTTP 500 or transport errors and produced no samples.

### Decision Record

Result: Exact deployed create-family semantics passed, atomic rollback is covered by fault injection, target p50 latency improved 7.3% to 21.2%, and deterministic request counts fell in every measured workload.
Decision: Accepted. Retain patch `0017-atomic-remote-create-operations.patch`, the Worker create/link procedures, Container version 25, image `a454c246`, and Worker version `4f6a1d08-c619-4ba1-8ac9-f3a72d0990b5`.

## Q11: Atomic Compound Remote `truncate`

**Status:** `Accepted`
**Priority:** High
**Risk:** High
**Effort:** High
**Primary files:** `agentfs/patches/0018-atomic-remote-truncate.patch`, `worker/src/hrana-server.ts`, `worker/test/hrana-server.test.ts`, `e2e/features.mjs`, and `e2e/benchmark.mjs`

### Problem

Remote truncate read the current inode size, deleted and rewrote chunks, and updated inode size and timestamps as separate Hrana operations. Explicit transaction control is filtered by the Worker, so a failure could leave chunk data inconsistent with inode metadata. Path-based extension also generated one remote zero-chunk insert at a time.

### Implemented Change

The Worker now strictly recognizes `airyfs_truncate_v1(ino, new_size, chunk_size, now_secs, now_nsec)` and executes the entire mutation inside `transactionSync()`. Shrink deletes excess chunks and trims the retained final chunk. Extension pads the existing final chunk and materializes zero chunks before updating inode size, mtime, and ctime. Missing inodes return `ENOENT` without mutation.

Patch `0018-atomic-remote-truncate.patch` routes both path-based and open-handle remote truncates through this procedure. Local SQLite behavior remains unchanged.

The first deployed correctness run exposed an incorrect sparse-extension assumption: Rust reads synthesize gaps, but the TypeScript AgentFS reader concatenates stored chunks and does not preserve sparse offsets. The procedure was corrected to materialize zero-filled extension chunks atomically, preserving cross-runtime byte layout without restoring network amplification.

### Correctness Validation

- A clean patch-series rebuild passed 234 TypeScript SDK tests, 117 Rust SDK tests including one ignored encryption test, one Rust SDK doc test, and 79 Rust CLI tests.
- All 367 Worker tests, 20 Container tests, 11 public TypeScript SDK tests, and 23 repository script tests passed; Worker typechecking passed.
- Focused protocol tests cover cross-chunk shrink, truncate-to-zero, zero-filled extension, missing inodes, normalized SQL, required transaction support, and rollback after destructive chunk changes.
- All 59 deployed feature checks passed. Added checks verify exact bytes after cross-chunk shrink, zero-filled extension through the direct TypeScript API, and truncate-to-zero.

### Measurements

The opt-in quick truncate scenario alternates 20 open-handle shrinks and extensions between 4 MiB and 2 MiB plus 123 bytes. A separate open/close sample controls for command and handle overhead. Matched three-run reports compare Q10 image `a454c246` with Q11 image `d7776e01`.

| Scenario | Q10 p50 | Q11 p50 | Latency change | Q10 -> Q11 pipelines | Q10 -> Q11 SQL statements |
|---|---:|---:|---:|---:|---:|
| Open and close control | 0.05 ms operation / 1.42 s client | 0.16 ms operation / 1.11 s client | Operation timing is noise | 24 -> 24 | 14 -> 14 |
| 20 alternating truncates | 7.61 s operation / 10.55 s client | 3.10 s operation / 4.14 s client | 59.3% operation / 60.8% client | 198 -> 104 | 138 -> 54 |

The truncate workload scored 224.69 against the matched control. Request amplification fell exactly on the treatment while control counts remained unchanged. One candidate benchmark attempt failed during its first sample with the known integration transport error and was discarded.

Raw reports: `/tmp/q11-truncate-control.json` and `/tmp/q11-truncate-candidate.json`.

### Decision Record

Result: Atomic rollback and exact cross-runtime bytes passed, truncate p50 improved roughly 60%, and deterministic request counts fell 47.5% to 60.9% with no control amplification change.
Decision: Accepted. Retain patch `0018-atomic-remote-truncate.patch`, the Worker truncate procedure, Container version 26, image `d7776e01`, and Worker version `f9e136df-5ef1-4d6c-80c2-3215c544d36a`. Continue migrating rename separately.

## Q12: Atomic Remote Rename

**Status:** `Accepted`
**Priority:** High
**Risk:** High
**Effort:** Medium
**Commit:** None
**Primary files/functions:** `worker/src/hrana-server.ts`, `AgentFS::rename`, `FileSystem for AgentFS::rename`

### Problem

Remote rename performed source and destination lookup, compatibility checks, destination unlink and orphan cleanup, source dentry movement, link-count changes, and timestamp updates as separate Hrana operations. The Worker filters explicit transaction control, so the intended libSQL transaction did not provide an atomic remote boundary. Twenty same-directory renames required a median 612 pipelines and 330 SQL statements.

### Implemented Change

The Worker now strictly recognizes `airyfs_rename_v1(old_parent_ino, old_name, new_parent_ino, new_name, now_secs, now_nsec)` and runs validation plus mutation inside `transactionSync()`. Integer statuses preserve `ENOENT`, `EISDIR`, `ENOTDIR`, `ENOTEMPTY`, `EINVAL`, and root-operation mappings. The procedure handles same-path and same-inode no-ops, rejects directory cycles, retains overwritten inodes with live leases, and atomically updates dentries, link counts, and timestamps.

Patch `0019-atomic-remote-rename.patch` routes both path-based and inode-based remote rename through the procedure while retaining dentry-cache updates. Patch `0020-fix-rename-directory-replacement.patch` fixes the inherited local and remote empty-directory replacement leak and parent link count.

### Correctness Validation

- A clean patch-series rebuild passed 234 TypeScript SDK tests, 117 Rust SDK tests including one ignored encryption test, one Rust SDK doc test, and 79 Rust CLI tests.
- All 377 Worker tests, 20 Container tests, 11 public TypeScript SDK tests, 226 CLI tests, and 23 repository script tests passed; Worker typechecking passed.
- Focused protocol tests cover simple and cross-parent rename, parent link counts, live-lease retention, file and empty-directory orphan cleanup, incompatible types, nonempty directories, cycles, same-path and same-inode no-ops, normalized SQL, required transaction support, and rollback after destination removal.
- All 67 deployed feature checks passed on the corrected candidate. Seven rename checks cover rename-over contents, open destination handle retention, cycle rejection, nonempty-directory rejection, empty-directory replacement, parent link counts, and same-path no-op.
- Five earlier attempts were interrupted at unrelated later requests by the known integration `fetch failed` transport issue and were discarded.

### Measurements

The opt-in quick rename scenario alternates one file between two names 20 times. A zero-operation sample controls for command overhead. Matched three-run reports compare verified Q11 image `d7776e01` with verified Q12 image `d631c7de` on `https://airyfs-int.tyson-s-sandbox.workers.dev`.

| Scenario | Q11 p50 | Q12 p50 | Latency change | Q11 -> Q12 pipelines | Q11 -> Q12 SQL statements |
|---|---:|---:|---:|---:|---:|
| Open/close control | 0.002 ms operation / 2.04 s client | 0.003 ms operation / 0.37 s client | Operation timing is noise | 10 -> 10 | 5 -> 5 |
| 20 alternating renames | 38.35 s operation / 40.11 s client | 24.70 s operation / 25.26 s client | 35.6% operation / 37.0% client | 612 -> 418 | 330 -> 212 |

The repeated-rename sample improved independently of its zero-operation control. Controls remain in reports for amplification checks but are excluded from scoring because they measure only exec and Python startup variance. One baseline and one candidate attempt failed with the known integration transport error and were discarded. Raw reports: `/var/folders/jb/lf55qr397lzd9_37jjs9b4th0000gn/T/opencode/q12-rename-baseline.json` and `/var/folders/jb/lf55qr397lzd9_37jjs9b4th0000gn/T/opencode/q12-rename-candidate.json`.

### Decision Record

Result: Atomic rollback and focused deployed semantics passed, rename p50 improved 35.6% to 37.0%, and deterministic request counts fell 31.7% to 35.8% with no control amplification change.
Decision: Accepted. Retain patches `0019-atomic-remote-rename.patch` and `0020-fix-rename-directory-replacement.patch`, the Worker rename procedure, Container version 28, image `451b0f12`, and Worker version `451b0f12-fb0f-4906-b8a8-ce8e3471c9ec`.

## Q13: Skip Repeated Warmed-Exec Lifecycle Probes

**Status:** `Inconclusive; runtime change not retained`
**Priority:** Medium
**Risk:** Medium
**Effort:** Low
**Commit:** None
**Primary function:** `AiryFS.ensureContainer` in `worker/src/index.ts`

### Problem

Every buffered or streaming exec calls `ensureContainer`. Even when the data and invalidation Hrana channels are active, the warmed path performs Container state discovery, `/setup`, and `/health` before sending the command. The new opt-in exec benchmark measured five sequential warmed `exec('true')` calls at 785 ms client p50, approximately 157 ms per call. The sample generated only two Hrana pipelines and one statement, confirming that the target is Container control-plane overhead rather than filesystem SQL.

### Experiment

The candidate returned early from `ensureContainer` while both persistent Hrana serve promises were active. Channel loss, explicit destroy, startup, and FUSE-unavailable responses retained their existing recovery paths. The source change was one readiness guard and no Container code changed.

### Result

Three candidate benchmark attempts failed with endpoint `fetch failed` errors, and a tailed one-run attempt hung in its first warmed sample for more than 90 seconds. After reverting and redeploying the accepted Worker source, the generic benchmark also hung after its startup cycle for more than 300 seconds. A separate fresh-volume restored-source probe completed successfully in 6.48 seconds including Container startup.

The restored-source hang means the candidate failures are not valid evidence of a regression. It also prevents a trustworthy latency comparison. Active Hrana promises remain an insufficient readiness contract because they do not explicitly prove command-port or Container lifecycle readiness.

### Decision Record

Result: Inconclusive. No candidate timing was retained.
Decision: Do not retain the runtime shortcut. Keep the opt-in `exec` benchmark with five distinct warmed HTTP exec calls. A future candidate needs explicit command-channel generation/readiness provenance or a lifecycle-isolated benchmark that does not become unstable after repeated startup cycling. Restored Worker version: `c693fde2-f677-4e77-a0c2-4d5c58238bcf`; Container remains version 28, image `451b0f12`.

## Suggested Execution Order

| Order | Experiment | Reason |
|---:|---|---|
| 1 | Q1 counter provenance | Makes all later request-count evidence trustworthy. |
| 2 | Q2 fold `pwrite` size lookup | Small, low-risk write-side analogue of accepted A4. |
| 3 | Q3 return directory stats | Small metadata round-trip reduction built on A3. |
| 4 | Q4 attribute cache and inode invalidation | Foundational higher-risk cache change. |
| 5 | Q7 prime caches from `readdir_plus` | Low incremental effort once Q4 exists. |
| 6 | Q5 remove create pre-checks | Valuable for small files and Git after schema verification. |
| 7 | Q6 negative-entry cache | Useful but locking and cross-interface visibility require caution. |
| 8 | Q8 fsync reduction | High potential, but durability risk makes it last. |

Only one runtime mechanism should be active at a time. Q1 is instrumentation and can land before runtime experiments without violating that rule.

## New Experiment Template

Copy this section when adding an idea. Replace estimates with measurements only after a complete deployed run.

```markdown
## XN: Experiment Name

**Status:** `Queued`
**Priority:**
**Risk:**
**Effort:**
**Commit:** None
**Primary files/functions:**

### Problem

Describe the observed bottleneck and cite baseline numbers.

### Hypothesis

State one falsifiable mechanism. Predict request-count and latency changes.

### Proposed Change

Describe the smallest implementation that tests the hypothesis.

### Correctness Risks

List filesystem semantics, cross-interface visibility, durability, and failure modes.

### Validation Plan

List local tests, deployed checks, target benchmarks, controls, and counter expectations.

### Measurements

Record endpoint, image tag, commit, profile, run count, location if known, raw report paths, p50/p95/p99, pipelines, statements, and failed-run reasons.

### Decision

Retain or reject, with the exact evidence and any follow-up.
```

## Experiment Log

Add dated entries here while an experiment is active. Keep failed and invalid runs because they explain why a later measurement was repeated, but label them clearly as invalid evidence.

| Date | Experiment | Event | Evidence or outcome |
|---|---|---|---|
| 2026-07-19 | A1 | Accepted | Broad 1.90x to 7.86x target improvements; score 229.12 against fresh control. |
| 2026-07-19 | R1 | Rejected | FUSE 1 MiB write regressed from 2.63 s to 6.50 s; full runs unstable. |
| 2026-07-19 | A3 | Accepted | Metadata operation 4.39x faster; Git commit and checkout improved; 33 checks passed. |
| 2026-07-20 | A4 | Invalid control discovered | Container application remained on candidate image despite rebuilding accepted source. Results were not used as control evidence. |
| 2026-07-20 | A4 | Accepted | Random reads 1.27x faster; sequential reads 1.19x faster; request amplification fell about 30%; 33 checks passed. |
| 2026-07-20 | Q1 | Validation complete | Added explicit data-session epochs; 39 deployed checks passed, forced reconnects invalidate deltas, and uninterrupted benchmark samples retain numeric counters. |
| 2026-07-20 | Q2 | Invalid runs | Two control attempts and one candidate attempt failed with integration transport errors; all partial results were discarded. |
| 2026-07-20 | Q2 | Accepted | Write targets improved 2.03x to 2.84x in a faster candidate window; deterministic pipeline and statement reductions matched the eliminated size lookup, and 39 checks passed. |
| 2026-07-20 | Q3 | Accepted | Metadata walks saved 24 pipelines and 12 statements; nested-parent semantics and 44 deployed checks passed. |
| 2026-07-20 | Q4 | Rejected | Metadata and Git latency regressed despite one fewer statement per metadata walk; restored Q3 image `bfebcdbf`. |
| 2026-07-20 | Q5 | Invalid run | The first candidate Git run failed during sample two with an integration transport error and was discarded. |
| 2026-07-20 | Q5 | Accepted | Small-file creation improved 32.3%; Git targets improved 11.6% to 21.5%; request amplification fell and 44 checks passed. |
| 2026-07-20 | Q6 | Rejected | Repeated misses saved 38 pipelines and 19 statements, but timing was inconsistent and Git regressed 37.8% to 72.1%. |
| 2026-07-20 | Q8 | Rejected | Twenty deployed `fsync` calls took under 1 ms and did not generate per-call Hrana traffic; the four-round-trip premise was stale. |
| 2026-07-20 | Q9 | Accepted | Random writes improved 77.5% and sequential writes 54.5%; pipelines fell 64.8% and 50.6% respectively, with atomic Worker-side chunk and metadata updates. |
| 2026-07-21 | Q12 | Accepted | Repeated rename improved 35.6% operation and 37.0% client p50; pipelines fell 31.7%, empty-directory replacement was corrected, and all 67 deployed checks passed. |
| 2026-07-21 | Q13 | Inconclusive | Five warmed no-op execs measured 785 ms p50, but candidate and restored generic runs both became unstable after startup cycling; runtime shortcut reverted. |
