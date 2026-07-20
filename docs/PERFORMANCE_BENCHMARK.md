# Deployed Performance Benchmark

## Purpose

The deployed benchmark measures direct HTTP and Container/FUSE behavior separately. It records operation latency, client-observed latency, throughput or operations per second, and per-scenario Hrana pipeline and SQL statement counts. Run it before and after each optimization so a win in one workload cannot hide a regression in another.

The benchmark does not simulate multiple concurrent Containers. One AiryFS volume permits one active `exec`, so the harness measures concurrency inside one mounted filesystem where applicable.

## Prerequisites

Build the SDK and provide a deployed endpoint. Set `AIRYFS_TOKEN` when deployment authentication is enabled.

```bash
cd sdk && npm run build && cd ..
AIRYFS_URL=https://your-worker.workers.dev \
  AIRYFS_TOKEN="$AIRYFS_TOKEN" \
  npm run test:features:deployed
```

The deployed feature smoke is the correctness gate. It includes binary and unaligned cross-chunk reads, random FUSE writes checked through the direct API, directory traversal and attributes, negative lookup invalidation, concurrent reads, snapshots, uploads, execution, and jobs.

## Profiles

`quick` is the development loop. It uses 1 MiB sequential files, 16 random operations, 20 metadata files, 10 small files, and one Git file. These counts keep the complete baseline loop practical even when random writes run near one operation per second and small-file creation takes several seconds per file.

`full` is the decision profile. It adds 95 MiB sequential files and uses 2,048 random operations, 1,000 metadata files, and 1,000 small files while retaining the fixed one-file warm Git workflow. The 95 MiB size stays below common 100 MB deployed request limits while remaining large enough for sustained-I/O measurements.

Scenario groups are `direct-sequential`, `fuse-sequential`, `fuse-random`, `metadata`, `small-files`, and `git`. The default is all groups, three samples, and the 256 KiB production default.

## Score

The optimization score is relative to [`benchmarks/quick-baseline.json`](../benchmarks/quick-baseline.json), where 100 is the deployed baseline captured on 2026-07-19. Higher is better. Run a scored candidate with:

```bash
AIRYFS_URL=https://dofs-int.tyson-s-sandbox.workers.dev \
  npm run benchmark:quick -- \
  --label candidate \
  --output benchmark-candidate.json
```

The score gives equal weight to seven dimensions: Container startup, direct HTTP I/O, FUSE sequential I/O, FUSE random I/O, metadata traversal, small-file creation, and Git. Within each dimension, scenario scores use the geometric mean so no unit or single workload dominates. Each scenario combines client-observed p50, p95, and p99 latency with weights of 50%, 30%, and 20%. Fixed workload sizes make inverse latency a valid throughput comparison. The top-level score is the geometric mean of the seven dimension scores.

Git uses a warm-repository model. Fixture setup creates all 256 loose-object fanout directories outside the timed region because the baseline FUSE cache can otherwise fail Git's immediate create-after-mkdir sequence. Timed Git work still includes status, index updates, object writes, refs, one commit, and checkout. The report includes commits per second for `git_add_commit`.

The three-sample quick profile is intended for optimization iteration. Its p95 and p99 values are interpolated from a small sample and should be treated as regression signals, not production SLO estimates. Use more runs for a final decision.

### Current Baseline

The 2026-07-19 quick baseline produced an overall score of 100:

| Dimension | Representative p50 | Rate |
|---|---:|---:|
| Container startup | 4.03 s | - |
| Direct 1 MiB read | 268 ms | 3.73 MiB/s |
| Direct 1 MiB write | 858 ms | 1.17 MiB/s |
| FUSE 1 MiB read | 6.89 s client / 4.83 s operation | 0.21 MiB/s |
| FUSE 1 MiB write | 7.36 s client / 5.40 s operation | 0.19 MiB/s |
| 16 random 4 KiB reads | 11.02 s client | 2.05 operations/s |
| 16 random 4 KiB writes | 25.05 s client | 0.70 operations/s |
| Metadata walk after create | 15.04 s client | 1.53 entries/s |
| Warm metadata walk | 11.55 s client | 2.06 entries/s |
| Create 10 small files | 21.01 s client | 0.57 files/s |
| Git status | 33.39 s | 0.030 operations/s |
| Git add and commit | 72.32 s | 0.0138 commits/s |
| Git checkout | 46.17 s | 0.022 operations/s |

The baseline points to this optimization order:

1. Batch remote writes and remove compatibility-only transaction round trips because random writes, small files, and Git commit are the slowest dimensions.
2. Enable adaptive `READDIRPLUS` and improve inode/negative-entry cache invalidation because metadata and all Git operations remain extremely slow, and the warm metadata pass is not faster than the first pass.
3. Collapse `pread` size and chunk lookup work into one request because sequential and random FUSE reads remain an order of magnitude behind direct reads.

## Baseline

Use a stable prefix. The harness reuses one volume per chunk size, permanently clears `/bench` before and after a run, and destroys the Container after measurement. It writes a persistent ownership marker before using `/bench` and refuses to delete an unmarked tree. It does not delete volumes because volume deletion is unavailable.

```bash
AIRYFS_URL=https://your-worker.workers.dev \
  npm run benchmark:deployed -- \
  --profile full \
  --chunk-sizes 4096,65536,262144 \
  --runs 3 \
  --prefix airyfs-bench \
  --label baseline \
  --output benchmark-baseline.json
```

Reports contain every sample, p50/p95/p99 latency grouped by scenario and chunk size, harness revision and dirty-worktree state, workload configuration, startup latency, SQLite size before and after the workload, physical growth, logical-byte growth, and inode growth. Physical SQLite growth can be zero on a reused volume when SQLite reuses free pages. `operationMs` is measured inside the Container command. `clientMs` includes the Worker and execution protocol. Direct and Git scenarios report client-observed time. Hrana counters cover the active data channel, excluding invalidation polling, and are per-scenario deltas tied to one data-session ID; a session reset records `null` rather than a misleading value.

Avoid comparing runs across different Cloudflare locations, deployment configurations, profiles, run counts, or scenario selections. Run baseline and candidate close together, and repeat noisy results.

## Optimization Experiments

Change and deploy one optimization at a time. Run the feature smoke, run the same benchmark command with a new label and output file, then compare it with the unchanged baseline.

```bash
npm run benchmark:compare -- benchmark-baseline.json benchmark-candidate.json
```

The comparison reports latency change, speedup, Hrana pipeline change, and SQL statement change for matching scenarios. Negative deltas are improvements for latency and request counts.

Use this order:

1. Enable adaptive FUSE `READDIRPLUS`. Expect lower metadata and Git status latency and fewer lookup requests. Verify directory attributes and pagination first.
2. Collapse `pread` inode-size and chunk lookups into one SQL request. Expect lower random and sequential read latency and fewer pipelines.
3. Improve inode, attribute, and negative-entry caching with journal invalidation. Expect metadata and Git improvements. Preserve direct-create visibility and cross-interface mutation correctness.
4. Batch remote writes and remove compatibility-only transaction round trips. Expect fewer pipelines and lower sequential, random-write, small-file, and Git commit latency.

Retain an optimization only when its target scenario improves repeatably, its request-amplification change matches the mechanism, unrelated scenarios do not regress materially, and the deployed feature smoke remains green.

### Accepted: Skip Remote Prepared-Statement Descriptions

The first optimization patches libSQL 0.9.30's Hrana statement constructor to omit the separate `describe` pipeline. AgentFS binds parameters from libSQL's local SQL parser and never reads prepared-statement column metadata, so execution and result decoding remain unchanged. The version-pinned dependency patch is applied by `agentfs/build.sh` before Rust compilation.

The integration deployment passed all 30 deployed feature checks. Its three-run quick score was 195.93 against the checked-in score-100 baseline and 229.12 against a fresh close-in-time control. Compared with the fresh control, direct read and write did not regress, while representative operation-latency improvements were:

| Scenario | Control p50 | Candidate p50 | Speedup |
|---|---:|---:|---:|
| FUSE 1 MiB read | 5.41 s | 2.40 s | 2.25x |
| FUSE 1 MiB write | 7.21 s | 2.63 s | 2.74x |
| 16 random 4 KiB reads | 7.90 s | 4.17 s | 1.90x |
| 16 random 4 KiB writes | 27.45 s | 13.00 s | 2.11x |
| Metadata walk after create | 17.40 s | 2.21 s | 7.86x |
| Create 10 small files | 20.74 s | 6.84 s | 3.03x |
| Git status | 38.77 s | 9.85 s | 3.94x |
| Git add and commit | 92.05 s | 24.58 s | 3.75x |

Hrana request deltas were unavailable in this run because the Container reconnected its data bridge before every exec, changing the `/perf` session ID. The benchmark correctly recorded those deltas as `null` rather than subtracting counters from different sessions. The lifecycle now reuses a healthy bridge and reconnects only after a drop, so subsequent reports include numeric pipeline and statement deltas. The dependency patch itself removes the only `stream.describe(&sql)` call from the compiled Hrana statement path.
