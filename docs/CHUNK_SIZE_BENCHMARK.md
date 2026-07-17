# Chunk Size Benchmark

## Decision

New volumes default to 256 KiB chunks. Volumes may select a power-of-two chunk size from 4 KiB through 1 MiB at creation, and the setting becomes immutable once filesystem data exists. Existing volumes retain their configured size.

The local benchmark demonstrates a 4.31x sequential direct-SDK throughput improvement and a 64x reduction in SQLite chunk rows for a 1 MiB file. It does not exercise the Rust FUSE client or Container-to-Durable Object network path, so deployed FUSE measurements remain required before claiming an end-to-end throughput improvement.

## Local Results

Run on 2026-07-16 with:

```bash
cd worker
npm run benchmark:chunks
```

The benchmark writes and reads a 1 MiB file through the AgentFS Cloudflare adapter against in-memory SQLite.

| Chunk size | Operations/second | Mean latency | Relative to 4 KiB |
|------------|------------------:|-------------:|------------------:|
| 4 KiB | 765 | 1.31 ms | 1.00x |
| 64 KiB | 2,818 | 0.35 ms | 3.68x |
| 256 KiB | 3,299 | 0.30 ms | 4.31x |

The 1 MiB correctness test stores 256 rows at 4 KiB and 4 rows at 256 KiB. These results demonstrate that chunk size matters for sequential direct-SDK I/O and SQL amplification. They do not predict deployed FUSE performance because local SQLite has no network latency and the Rust client may issue a different SQL workload.

## Required Follow-Up

Run a deployed benchmark at 4 KiB, 64 KiB, and 256 KiB covering:

- Sequential 1 MiB and 100 MiB reads and writes through FUSE.
- Random 4 KiB reads and writes.
- Small-file creation and directory traversal.
- Git checkout, status, add, and commit.
- Direct SDK reads and writes.
- SQLite storage size and Hrana statement counts.

Use the deployed results to validate the 256 KiB default and quantify FUSE-path value. Never silently migrate existing volumes.
