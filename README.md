# AiryFS

AiryFS is a durable, programmable filesystem for Cloudflare Durable Objects. It supports four ways to work with the same volume:

1. **AgentFS inside the Durable Object:** application code uses the full AgentFS filesystem directly against `ctx.storage.sql`, with optional Linux execution through an attached Container.
2. **Web APIs and Workers RPC:** remote applications stream files, inspect metadata, mutate paths, and run commands.
3. **The TypeScript SDK:** Node, browser, and Worker applications use a complete typed client plus async iterators for exec, durable jobs, and change feeds.
4. **The `airyfs` CLI:** developers use named sessions, familiar filesystem commands, bulk transfers, snapshots, durable jobs, watch feeds, diagnostics, and an interactive shell.

The filesystem is an application primitive owned by the Durable Object, not a disk hidden behind a compute environment. Durable Object methods can create, inspect, transform, and coordinate files through AgentFS. HTTP, RPC, and CLI clients operate on the same state. When a workload needs Git, Python, a compiler, a test runner, or another Linux tool, AiryFS mounts that state at `/volume` in an attached Container and runs the real program there.

The Durable Object's SQLite database is the only persistent store for the volume. The complete persistent filesystem, coordination state, snapshots, uploads, jobs, and change history are self-contained within that Durable Object. AiryFS does not copy the filesystem into a Container disk, synchronize a second database, or persist file data in object storage or another service.

```text
Durable Object methods / HTTP / Workers RPC / airyfs CLI
                         |                    |
             direct filesystem API          exec
                         |                    |
                         v                    v
              Durable Object SQLite <--- Hrana ---> FUSE /volume
                only persistent source          data + invalidation
```

AgentFS provides the filesystem semantics and native TypeScript interface inside the Durable Object. AiryFS adds coordinated HTTP and RPC access, a universal TypeScript SDK, a local CLI, and on-demand real-process execution. Direct filesystem calls do not start the Container. Execution starts or reconnects the Container, mounts the same SQLite-backed volume through FUSE, and runs with `/volume` as the working directory.

## What You Can Build

- **Coding-agent workspaces:** let an agent manage source files through the Durable Object API or TypeScript SDK, invoke a real Container for Git, package managers, compilers, and tests, then return artifacts through the same volume.
- **Document and data transformation:** accept inputs through a Durable Object method or HTTP upload, inspect and organize them without starting compute, run existing conversion tools in the Container, and stream results back from the same volume.
- **Repository automation:** maintain durable per-repository state, update individual files directly, and attach disposable compute only for operations such as checkout, diff, lint, build, or test.
- **Per-user application storage with execution:** give each user or job an isolated filesystem that application code can query and mutate, while retaining the option to run general-purpose software against it.
- **Durable workflow workspaces:** preserve intermediate files across retries, Container sleep, and Container replacement without adding a separate synchronization or recovery system.
- **Static sites and artifact sharing:** publish a volume subtree as a public website with index-document and single-page-app routing, or mint expiring links to individual files, served directly from Durable Object SQLite with no Container.

## Core Properties

- **One volume, one Durable Object:** each volume name maps to an isolated Durable Object and SQLite database.
- **Self-contained durable state:** the filesystem and all AiryFS coordination records live in that Durable Object; no external storage service is required.
- **SQLite-only persistence:** all persistent file content, metadata, links, and directory entries live in Durable Object SQLite.
- **Four product surfaces, one schema:** Durable Object code, web APIs/SDK, and the CLI all read and mutate the same AgentFS tables; Container tools see those tables through FUSE.
- **Container on demand:** reads, writes, listings, and metadata operations do not require a Container. Container-backed execution, including CLI `warm`, starts or reconnects it.
- **Container compute scales to zero:** after the configured inactivity timeout, the Container sleeps and its compute charges stop; the next `exec` remounts the durable volume.
- **Ephemeral compute:** destroying or evicting the Container does not destroy the volume. The next `exec` remounts it from Durable Object SQLite.
- **Normal tools:** software inside the Container sees `/volume` as a mounted filesystem and can use standard file APIs without a AiryFS-specific SDK.

Volume bytes necessarily travel to the Container when a process reads them through FUSE. The distinction is persistence: there is no second durable copy of the volume outside the Durable Object's SQLite storage.

## When To Use AiryFS

AiryFS fits workloads that need the filesystem to be directly programmable state, externally accessible state, and an execution workspace:

- Durable Object applications that model source trees, documents, generated artifacts, or task state as files and directories.
- Agent workspaces that use the direct APIs or TypeScript SDK for file operations, then need native tools for execution.
- Build and transformation jobs that need fast ingestion and retrieval around a smaller amount of Container execution.
- Per-user, per-repository, or per-task workspaces that need isolation through one Durable Object per volume.
- Stateful automation where Container compute can disappear but the workspace and intermediate files must remain.
- Applications that need to inspect, mutate, or serve files through HTTP without paying a Container cold start.
- Workflows that want one authoritative SQLite-backed namespace rather than a Container filesystem synchronized to another persistent service.

AiryFS is not optimized for workloads dominated by thousands of sequential metadata operations. Every FUSE syscall crosses the Container-to-DO boundary and executes SQL in the Durable Object. For those workloads, use the direct API to create or transfer files in larger operations, then use `exec` for computation.

## Capabilities

| Area | Capability |
|---|---|
| Persistent storage | Files, directories, symlinks, POSIX metadata, and file chunks in Durable Object SQLite |
| Direct file access | Binary-safe streaming reads and writes without starting the Container |
| HTTP semantics | `GET`, `HEAD`, single byte ranges, content length, last-modified time, inode headers, and structured errors |
| File mutations | Atomic replacement after a complete upload, delete, copy, rename, and truncate |
| Directory operations | Create, list with metadata, remove, and recursive remove |
| Links | Create symbolic links and read link targets |
| Authentication | Optional root bearer auth, signed/expiring/revocable capabilities scoped by volume/operation/path, and per-volume passwords that mint scoped tokens without the root secret |
| Web hosting | Opt-in public static-site serving with MIME inference, index documents, and SPA fallback, plus expiring file-share links, served directly from SQLite |
| Bulk transfer | Transactional streaming directory push/pull using the dependency-free AiryFS archive format |
| Snapshots | Named full-volume capture, list, exact diff, restore, delete, and cross-volume clone |
| Large files | Persistent resumable uploads, range-resumed downloads, per-chunk and full-file SHA-256 verification |
| Execution | Buffered or live NDJSON stdout/stderr, process-group cancellation, and at-most-once command admission |
| Durable jobs | Idempotent queued commands with persisted status, binary logs, cancellation, orphan recovery, and output limits |
| Change feeds | Ordered create, modify, remove, and rename events from both direct API and FUSE writers, with retention-gap detection |
| Workers RPC | Streams, metadata, mutations, trees, uploads, snapshots, jobs, changes, usage, lifecycle, and exec |
| TypeScript SDK | Complete typed HTTP client plus watch, job-follow/wait, exec-id, and resumable Blob helpers |
| CLI | Sessions, remote cwd, smart upload/download plus file and tree transfer, snapshots, jobs, watch, password auth, session export/import, web hosting, one-command deploy, diagnostics, JSON output, and interactive shell |
| Container execution | Run shell commands with `cwd=/volume`, a five-minute timeout, streaming output, and cancellation |
| Standard tooling | Git, Python, shell utilities, and other programs included in the Container image |
| Lifecycle | Single-flight startup, FUSE readiness checks, failed-mount cleanup, TCP reconnects, and explicit Container destruction |
| Concurrency | Path-scoped direct-access locks, a volume-wide lock for FUSE SQL mutations, and journal-driven FUSE cache invalidation |
| Protocol | Hrana pipeline and cursor transport, batches and conditions, stored SQL, sequences, typed values, and PRAGMA compatibility |
| Observability | Logical filesystem usage, SQLite size, per-table row counts, Container/FUSE health, and Hrana request counters |
| Schema management | Atomic, idempotent initialization plus migrations for supported older AgentFS table layouts |
| Additional state | A simple key-value table and AgentFS tool-call tables in the same DO SQLite database |

## How AiryFS Differs

Several architectures can expose something filesystem-like. AiryFS combines properties that are usually separated.

### Compared with a direct Durable Object filesystem library

A library can map filesystem methods onto `ctx.storage.sql` and provide fast reads and writes inside the Durable Object. That solves the direct-access half of the problem, but programs in an attached Container still cannot call `open`, `stat`, or `readdir` against those files.

AiryFS adds the Container lifecycle, remote SQL transport, Hrana compatibility layer, and FUSE mount required to expose the same SQLite rows as `/volume`. It retains direct access instead of forcing every operation through the mount.

### Compared with a Container workspace or mounted volume

A Container-local workspace is convenient for execution, but it makes the Container the gateway to the files. Reading one file, checking metadata, or serving an artifact generally requires the Container to be running. Container sleep and replacement also force the application to decide where durable state actually lives.

Persisting that workspace commonly adds an external mounted filesystem, object store, network volume, or clone/synchronization process. That creates another dependency and another consistency boundary: the application must reason about upload completion, visibility of writes, rename and delete behavior, partial synchronization, retries, conflicts, and recovery after compute disappears. It can also move all file access through the Container or through a remote storage protocol even when the Durable Object only needs a small read or metadata operation.

AiryFS keeps compute disposable and makes the Durable Object's existing SQLite storage authoritative. The Container is a filesystem client, not the owner of persistent state. There is no clone-back phase and no second durable copy to reconcile. Direct operations stay inside the Durable Object; only workloads that need Linux execution start the separately billed Container and FUSE path. Container loss requires a remount, not data synchronization or filesystem recovery.

### Compared with an object-backed filesystem interface

Object storage is effective for large immutable values, but filesystem metadata operations, atomic path mutations, directories, and links require a separate consistency model. It also introduces another persistent system outside the Durable Object.

AiryFS uses SQLite transactions, indexes, and AgentFS's inode/dentry model. The complete persistent namespace remains colocated with the Durable Object that coordinates it.

### Compared with a remote development environment

A long-lived development environment typically treats the machine or its disk as the workspace. That makes direct edge access and compute-independent persistence secondary concerns.

AiryFS starts from the opposite invariant: the Durable Object owns the volume, and compute attaches only when needed. It is a storage primitive with execution, not a persistent machine exposed through an API.

## Relationship To AgentFS

[AgentFS](https://github.com/tursodatabase/agentfs) is the filesystem implementation used by AiryFS. It defines the SQLite schema and filesystem semantics for inodes, directory entries, file chunks, symlinks, overlay metadata, key-value records, and tool calls.

AiryFS uses AgentFS in two different runtimes:

- The vendored AgentFS Cloudflare adapter runs directly inside the Durable Object against `ctx.storage.sql`.
- The AgentFS Rust SDK and CLI run inside the Container and expose the same database through FUSE.

The AgentFS Cloudflare integration demonstrates direct filesystem access backed by Durable Object SQLite. AiryFS extends that model into a complete service with direct and FUSE data paths exposed through four product surfaces. It adds named volume routing, a resource-oriented HTTP API, Workers RPC methods, the TypeScript SDK, the CLI, streaming and range handling, mutation coordination, schema migration, Container startup and health management, an HTTP-to-TCP bridge, a Hrana server backed by `ctx.storage.sql`, and an on-demand FUSE mount.

AiryFS is not a replacement for AgentFS. It is the Cloudflare Durable Object and Container architecture around AgentFS that makes both technical data paths and all four product surfaces operate on one persistent database.

### Why AiryFS Patches AgentFS

AiryFS vendors pristine AgentFS `v0.6.4` at commit `3a5ed2b88e5d5a5f9b2c7fe02d012b50fd19e3c0`, then applies an ordered patch series. The upstream remote modes did not provide the direct remote libSQL connection, cache-coherence behavior, or cross-runtime open-inode semantics required when one filesystem is mutated by both Durable Object code and remote FUSE clients.

The patches are intentionally kept outside the upstream snapshot. `agentfs/build.sh` materializes a fresh tree, verifies and applies each patch in order, runs the TypeScript and Rust test suites, and builds the Linux CLI used by the Container. AiryFS prefers changes in its Worker, bridge, or Container layers when AgentFS does not need to change; the patch series is the explicit compatibility surface that remains.

| Patch | Surface | Purpose |
|---|---|---|
| `0001` | Rust SDK and CLI | Replace `turso` with open-source `libsql`, adapt APIs and row lifetimes, and remove Turso sync commands and replica synchronization support. |
| `0002` | Rust SDK and CLI | Add direct remote libSQL connections, `AgentFSOptions::with_remote`, and mount options for `--remote-url` and `--auth-token`. |
| `0003` | Rust SDK | Send remote PRAGMAs through supported execution paths and remove explicit remote transactions from `fsync`. |
| `0004` | Documentation | Record the fork-specific remote-libSQL behavior in the materialized AgentFS README. |
| `0005` | Rust workspace | Backport upstream Clippy fixes needed by the pinned source. |
| `0006` | Rust workspace | Avoid unstable `Path::file_prefix` and retain the Rust 1.88 build target. |
| `0007` | Rust CLI tests | Use the libSQL-supported `aes256cbc` cipher in encryption coverage. |
| `0008` | FUSE | Add finite entry and attribute cache TTLs and disable writeback caching in bounded-cache mode. |
| `0009` | FUSE and schema | Add mutation-journal polling, batched deferred kernel invalidation, and a separate invalidation connection. |
| `0010` | Rust SDK, FUSE, and TypeScript Cloudflare adapter | Add lease-aware open/create/release, heartbeat and stale-lease reaping, overlay inode translation, and matching unlink and rename-over behavior across the remote and direct runtimes. |

Patch `0010` is deliberately cross-runtime. A direct TypeScript unlink or streaming rename-over must preserve an inode still held by a Rust FUSE file handle, just as an operation originating in FUSE would. The Rust hooks, persistent `fs_open_inode` leases, and Cloudflare adapter changes jointly enforce that behavior.

The resulting mount command connects to the bridge inside the Container:

```bash
agentfs mount \
  --remote-url http://localhost:8080 \
  --invalidation-url http://localhost:8081 \
  --auth-token "" \
  --cache-ttl-ms 1000 \
  --foreground \
  volume /volume
```

No SQLite database file is created in the Container for the mounted volume.

## Quick Start

Volumes are created on first use, or explicitly with a selected chunk size. Choose the interface that matches where your application runs; all three operate on the same persistent volume.

### 1. Use AgentFS Inside The Durable Object

The `AiryFS` class lazily creates an AgentFS Cloudflare filesystem backed by `ctx.storage.sql`. Add methods such as the following inside the `AiryFS` class to combine its coordinated, AgentFS-backed filesystem wrappers with real Container execution without copying data between them:

```typescript
async runPython(source: string): Promise<{
  exitCode: number;
  stdout: string;
  stderr: string;
  output: string;
}> {
  // Direct path: coordinated write to Durable Object SQLite, no Container.
  await this.writeFile('/main.py', source);
  const input = await this.statPath('/main.py');
  if (input.type !== 'file') throw new Error('Expected /main.py');

  // Execution path: mount the same AgentFS tables and run a real process.
  const result = await this.exec('python3 main.py > output.txt') as {
    exitCode: number;
    stdout: string;
    stderr: string;
  };
  if (result.exitCode !== 0) throw new Error(result.stderr);

  // Direct path again: read the Container output without another exec.
  return { ...result, output: await this.readFile('/output.txt') };
}
```

This method can also be called over Workers RPC on a `AiryFS` stub. The direct operations execute against Durable Object SQLite; `exec` attaches compute to that same state. Existing AiryFS wrappers already coordinate access and append mutation-journal entries. Custom methods that call the underlying `AgentFS` instance directly must use `VolumeAccessCoordinator` for overlapping content access and record direct mutations so mounted FUSE clients invalidate stale cache entries.

The underlying AgentFS interface includes `readFile`, `writeFile`, `readdir`, `readdirPlus`, `stat`, `lstat`, `mkdir`, `rm`, `rename`, `copyFile`, `symlink`, `readlink`, `access`, `statfs`, and random-access handles through `open`; file handles provide operations such as `truncate(size)`.

### 2. Use The Web APIs

The resource-oriented HTTP API supports binary streaming, metadata, path mutations, execution, and diagnostics. Ordinary file operations do not start the Container.

```bash
BASE=https://your-worker.workers.dev
VOLUME=myproject

# Create the volume and a source directory.
curl -X PUT "$BASE/v1/volumes/$VOLUME" \
  -H "Content-Type: application/json" \
  -d '{"chunkSize":262144}'
curl -X PUT "$BASE/v1/volumes/$VOLUME/directories/src"

# Stream a file directly into Durable Object SQLite, then read it back.
curl -X PUT "$BASE/v1/volumes/$VOLUME/files/src/main.py" \
  --data-binary 'print("hello from AiryFS")'
curl "$BASE/v1/volumes/$VOLUME/files/src/main.py"

# Run a real process against the same file and inspect runtime health.
curl -X POST "$BASE/v1/volumes/$VOLUME/exec" \
  -H "Content-Type: application/json" \
  -d '{"command":"python3 src/main.py"}'
curl "$BASE/v1/volumes/$VOLUME/usage"
```

Workers RPC exposes the same Durable Object without an HTTP serialization layer. A Worker with a compatible namespace binding and RPC type can call the public `AiryFS` methods directly:

```typescript
export default {
  async fetch(_request: Request, env: Env): Promise<Response> {
    const volume = getContainer<AiryFS>(env.AiryFS, 'myproject');

    await volume.writeFile('/message.txt', 'hello from RPC');
    const message = await volume.readFile('/message.txt');
    const result = await volume.exec('wc -c message.txt');

    return Response.json({ message, result });
  },
};
```

The string methods are convenient for small text files. `readFileStream` and `writeFileStream` provide binary streaming, while metadata and mutation methods expose the rest of the filesystem surface.

### 3. Use The TypeScript SDK

The dependency-free `airyfs-sdk` package uses web-standard `fetch`, streams, `Blob`, and Web Crypto APIs. It runs in Node.js 22+, modern browsers, and Workers:

```bash
cd sdk
npm ci
npm run build
```

```typescript
import {
  AiryFSClient,
  waitForJob,
  watchChanges,
} from 'airyfs-sdk';

const client = new AiryFSClient(
  'https://your-worker.workers.dev',
  'project',
  { token: process.env.AIRYFS_TOKEN },
);

await client.makeDirectory('/src');
await client.writeFile('/src/main.ts', 'console.log("hello")\n');

const submitted = await client.submitJob('node src/main.ts', '/');
const { job } = await waitForJob(client, submitted.id, {
  onLog(entry) {
    const bytes = Uint8Array.from(atob(entry.data), character => character.charCodeAt(0));
    console.log(new TextDecoder().decode(bytes));
  },
});

const controller = new AbortController();
for await (const change of watchChanges(client, { path: '/src', signal: controller.signal })) {
  console.log(change.type, change.oldPath, change.path);
}
```

`AiryFSClient` exposes files, directories, path operations, tree archives, buffered and streaming exec, resumable upload primitives, checksums, durable jobs and logs, snapshots, change feeds, auth/capabilities, usage, diagnostics, lifecycle, and KV state. High-level helpers manage long-poll cursors, job output, exec IDs, and resumable `Blob` uploads.

### 4. Use The CLI

The TypeScript CLI requires Node.js 22 or newer. It combines the HTTP filesystem API and Container execution behind named local sessions:

```bash
./install.sh   # builds the SDK and CLI, links `airyfs` and the short `airy` alias

printf 'print("hello from AiryFS")\n' > /tmp/airyfs-main.py

airy session create work \
  --endpoint https://your-worker.workers.dev \
  --volume project
airy volume create --chunk-size 256k

airy mkdir -p /src
airy upload /tmp/airyfs-main.py /src/main.py
airy upload -r ./project /project --replace
airy cd /src
airy cat main.py

airy warm
airy exec python3 main.py
airy job submit --wait python3 main.py
airy snapshot create before-refactor --note "known good"
airy watch /src
airy status
airy shell
```

A session stores an endpoint, volume, and remote working directory under `~/.airyfs`. `AIRYFS_SESSION` and `--session` let separate terminals or scripts select different sessions. Volume names are explicit because Durable Object namespaces cannot enumerate names used to derive object IDs.

Before each arbitrary `exec`, the CLI submits a retry-safe `:` preflight that starts or reconnects the Container. Transient transport and HTTP `502`, `503`, and `504` failures may retry that no-op. The actual user command is submitted at most once after ambiguous failures; it retries only for `EXEC_BUSY`, which confirms that the server did not admit it. HTML gateway failures are normalized into concise CLI errors rather than printed as markup.

See [`cli/README.md`](cli/README.md) for the full CLI usage guide, including commands, sessions, shell behavior, and machine-readable output.

## Architecture

Each volume is one instance of the `AiryFS` Durable Object class and one attached Container instance.

```text
HTTP clients / TypeScript SDK / airyfs CLI -> Worker router +
                                                       |
Durable Object methods / Workers RPC ------------------+
                                                       |
                                                       v
                                              AiryFS Durable Object
                                                       |
                         +-----------------------------+--------------------+
                         |                                                  |
                    Direct path                                       Execution path
             AgentFS TypeScript adapter                       HranaServer executes SQL
                  ctx.storage.sql                               against ctx.storage.sql
             only persistent state                                ^              ^
                         |                                        | data         | invalidation
          HTTP / RPC / SDK / CLI commands                        v              v
                                                        Attached Container bridges
                                                         HTTP :8080 -> TCP :9000
                                                         HTTP :8081 -> TCP :9001
                                                                   |
                                                        AgentFS FUSE at /volume
                                                                   |
                                                            command execution
```

### Direct Path

The direct path calls the AgentFS TypeScript Cloudflare adapter with the Durable Object's storage context. Its asynchronous filesystem methods execute SQL against `ctx.storage.sql` without crossing a network boundary or starting the Container.

Streaming writes use a temporary AgentFS path and rename it over the destination only after the request body completes. Streaming reads fetch file chunks incrementally and support a single HTTP byte range.

### Execution Path

The first `exec` performs four steps:

1. Start the attached Container and wait for its command server.
2. Start the in-process HTTP-to-TCP bridge.
3. Open data and invalidation TCP connections from the Durable Object and start their Hrana servers.
4. Start the AgentFS FUSE daemon and wait until `/volume` is mounted.

Inside the Container, AgentFS uses `libsql::Builder::new_remote("http://localhost:8080", "")`. A filesystem operation becomes a Hrana HTTP request to the bridge. The bridge forwards a framed request over TCP to the Durable Object. The Durable Object executes the SQL against `ctx.storage.sql` and returns the result along the same path. A separate connection carries mutation-journal polling so direct path changes can invalidate FUSE kernel caches without competing with ordinary filesystem SQL.

The bridge rejects pending requests when a connection drops and applies a response timeout. On replacement, new requests switch to the new generation while already-dispatched work drains on the retired socket. Each exec request attaches a fresh data Hrana server to the running mount; a missing invalidation channel reconnects independently. Startup is single-flight, bounded, and generation-safe. A failed safe preflight or unavailable FUSE mount recycles only the disposable Container; Durable Object SQLite remains untouched.

### Persistence And Lifecycle

The Container image, process state, and files outside `/volume` are ephemeral. `/volume` is reconstructed from Durable Object SQLite whenever AgentFS remounts it.

`POST /destroy?volume=V` destroys only the Container. It does not delete the Durable Object or its SQLite data. Direct access continues to work, and a later `exec` starts a new Container and mounts the existing volume. Lifecycle cleanup closes both bridge channels and uses ownership tokens so an older request cannot clear a newer command's state.

The Durable Object requests automatic Container sleep after 30 minutes of inactivity. A direct filesystem request does not wake the Container. Once the Container sleeps, its compute charges stop and Container compute has scaled to zero; the next `exec` starts a new session and remounts the persistent volume.

## Protocol And Data Path

The direct path does not use this protocol. It exists specifically to let the AgentFS process inside the Container operate on SQLite owned by the Durable Object.

### End-To-End FUSE Read

When a command runs `cat /volume/hello.txt`, the read crosses these layers:

1. **Linux and FUSE:** `cat` calls `open`, `stat`, and `read`. The kernel recognizes `/volume` as a FUSE mount and sends requests to the AgentFS userspace daemon through `/dev/fuse`.
2. **AgentFS FUSE adapter:** the synchronous FUSE callback invokes the async AgentFS `FileSystem` implementation through its Tokio runtime.
3. **AgentFS filesystem layer:** a lookup or read becomes SQL against the AgentFS schema. A lookup resembles:

```sql
SELECT d.ino, i.mode, i.nlink, i.uid, i.gid, i.size,
       i.atime, i.mtime, i.ctime, i.rdev
FROM fs_dentry d
JOIN fs_inode i ON d.ino = i.ino
WHERE d.parent_ino = ? AND d.name = ?
```

4. **libSQL remote client:** the patched AgentFS build uses `Builder::new_remote("http://localhost:8080", "")`. Queries returning rows use `POST /v3/cursor`; other operations use `POST /v3/pipeline`.
5. **Container HTTP bridge:** the bridge accepts the Hrana request on port 8080. A cursor request is translated into a pipeline batch so the Durable Object needs only one request format.
6. **Framed TCP transport:** the bridge serializes the pipeline JSON as a 4-byte big-endian payload length followed by UTF-8 JSON, then writes it to the Durable Object connection on port 9000.
7. **Durable Object Hrana server:** `FrameBuffer` accumulates arbitrary TCP chunks and drains complete frames. `HranaServer` processes each pipeline and stream request sequentially.
8. **Durable Object SQLite:** statement arguments are converted from Hrana tagged values to Durable Object SQLite bindings. `ctx.storage.sql.exec()` executes against the same tables used by the direct AgentFS TypeScript adapter.
9. **Response transport:** rows and metadata are converted back into Hrana values, framed, and written to the Container. The bridge resolves responses in FIFO order.
10. **Cursor translation:** for `/v3/cursor`, the bridge emits the pipeline result as newline-delimited cursor entries. libSQL returns the rows to AgentFS, AgentFS answers FUSE, and the kernel returns bytes to `cat`.

A cursor response has this shape:

```text
{"baton":"airyfs-1","base_url":null}
{"type":"step_begin","step":0,"cols":[{"name":"ino"},{"name":"mode"}]}
{"type":"row","row":[{"type":"integer","value":"2"},{"type":"integer","value":"33188"}]}
{"type":"step_end","affected_row_count":0,"last_inserted_rowid":null}
```

File content follows the same path. `pread` selects one or more rows from `fs_data`; BLOB values are base64-encoded in Hrana, decoded by libSQL, and returned through FUSE. Writes reverse the flow with `INSERT`, `UPDATE`, and `DELETE` statements. Once SQLite commits a FUSE write, the direct API can read it immediately without going through the Container.

### Hrana Compatibility Surface

AiryFS implements the Hrana operations used by the AgentFS libSQL client. It is not intended to be a general-purpose libSQL server.

- `execute` runs one statement and returns columns, rows, affected row count, rows read/written, and last inserted row ID.
- `batch` evaluates ordered steps and `ok`, `error`, `not`, `and`, `or`, and `is_autocommit` conditions.
- A mutating batch holds one volume-wide write lock from its first step through its last step, preventing direct API mutations from observing an intermediate batch state.
- `sequence` executes semicolon-separated statements after filtering unsupported transaction and PRAGMA statements.
- `store_sql` and `close_sql` maintain SQL text by ID for later `execute`, `sequence`, or `describe` requests.
- `get_autocommit` reports the Durable Object SQLite behavior.
- `describe` returns the protocol response shape required by the client.
- Values support null, integer, float, text, and BLOB types. Integers outside JavaScript's safe range are rejected rather than silently rounded.
- `PRAGMA table_info(table)` is implemented through SQLite's `pragma_table_info` table-valued function so AgentFS receives accurate schema metadata.
- Unsupported PRAGMA writes return empty compatibility results.
- Explicit transaction-control statements are compatibility no-ops because Durable Object SQLite cannot keep a transaction open across separate Hrana requests.

Each active `HranaServer` records pipeline and statement counts for `/usage` and `/perf`. These are in-memory counters for the current Hrana server session, not cumulative billing or lifetime metrics. They reset when the connection or Durable Object restarts.

### Framing, Ordering, And Failure Handling

TCP does not preserve application message boundaries. `FrameBuffer` handles partial headers, partial payloads, and multiple frames delivered in one chunk. Each bridge channel admits at most 16 requests, assigns local request IDs, serializes socket writes with backpressure, and resolves the bounded Hrana pipeline in FIFO order. Request IDs are returned in `X-AiryFS-Request-ID` and never alter the Hrana wire payload.

Every bridge request has a 30-second response deadline and an 8 MiB frame limit. Queued work is removed when its HTTP client disconnects. An active canceled request is drained and discarded before later responses are resolved, preserving FIFO alignment. A write failure, timeout, oversized response, socket error, socket end, or socket close invalidates that connection generation, clears buffered bytes, and rejects every pending HTTP request. When the Durable Object reconnects, the retired generation remains isolated until its admitted requests drain or time out.

The bridge returns `503` with `Retry-After` when the Durable Object TCP connection is absent or admission is full. Pipeline transport failures become `502` responses to the libSQL client. A volume permits one active `exec`; overlapping commands receive `503 EXEC_BUSY` so one command cannot replace another command's TCP session.

See [`docs/TRANSPORT_HARDENING.md`](docs/TRANSPORT_HARDENING.md) for the transport invariants, cancellation behavior, and bounds.

## API

### Resource-Oriented HTTP API

| Method | Path | Behavior |
|---|---|---|
| `PUT` | `/v1/volumes/V` | Explicitly create a volume with optional `{"chunkSize":262144}` |
| `GET` | `/v1/volumes/V` | Return immutable volume configuration |
| `PUT` | `/v1/volumes/V/files/path` | Stream and atomically replace a file; parent must exist |
| `GET` | `/v1/volumes/V/files/path` | Stream a file; supports one `Range` |
| `HEAD` | `/v1/volumes/V/files/path` | Return file metadata headers |
| `DELETE` | `/v1/volumes/V/files/path` | Remove a file |
| `PUT` | `/v1/volumes/V/directories/path` | Create a directory |
| `GET` | `/v1/volumes/V/directories/path` | List entries with POSIX metadata |
| `DELETE` | `/v1/volumes/V/directories/path` | Remove a directory |
| `DELETE` | `/v1/volumes/V/directories/path?recursive=true` | Recursively remove a directory |
| `POST` | `/v1/volumes/V/operations/rename` | Rename `{"from":"/a","to":"/b"}` |
| `POST` | `/v1/volumes/V/operations/copy` | Copy `{"from":"/a","to":"/b"}` |
| `POST` | `/v1/volumes/V/operations/symlink` | Link `{"target":"/a","path":"/b"}` |
| `POST` | `/v1/volumes/V/operations/readlink` | Read `{"path":"/b"}` |
| `POST` | `/v1/volumes/V/operations/truncate` | Resize `{"path":"/a","size":4096}` |
| `POST` | `/v1/volumes/V/operations/checksum` | Stream a file through server-side SHA-256 |
| `GET` | `/v1/volumes/V/trees/path` | Stream a directory as a AiryFS archive |
| `PUT` | `/v1/volumes/V/trees/path?replace=true` | Transactionally import a AiryFS archive |
| `POST` | `/v1/volumes/V/exec` | Execute and return buffered stdout/stderr |
| `POST` | `/v1/volumes/V/exec?stream=true` | Stream `start`, base64 stdout/stderr, and `exit` NDJSON events |
| `POST` | `/v1/volumes/V/exec/cancel` | Cancel a streaming exec by its start-event ID |
| `POST` | `/v1/volumes/V/uploads/path` | Begin or resume a checksummed upload |
| `GET` | `/v1/volumes/V/uploads/path` | Return durable upload status and offset |
| `PATCH` | `/v1/volumes/V/uploads/path` | Append one checksummed chunk at `Upload-Offset` |
| `PUT` | `/v1/volumes/V/uploads/path` | Verify and atomically publish a complete upload |
| `DELETE` | `/v1/volumes/V/uploads/path` | Abort an upload and remove its hidden partial file |
| `POST` | `/v1/volumes/V/browser-uploads/path` | Stream a raw browser `File` body using a write capability scoped to the destination path |
| `GET` | `/v1/volumes/V/assets/SHA256` | Stream an immutable content-addressed asset with long-lived cache headers |
| `PUT` | `/v1/volumes/V/assets/SHA256` | Verify and atomically publish content matching the SHA-256 URL |
| `GET` | `/v1/volumes/V/snapshots` | List named full-volume snapshots |
| `POST` | `/v1/volumes/V/snapshots` | Create a snapshot with optional name and note |
| `GET` | `/v1/volumes/V/snapshots/ID/diff?against=live` | Diff against live state or another snapshot |
| `POST` | `/v1/volumes/V/snapshots/ID/restore` | Restore a snapshot and recycle the Container |
| `POST` | `/v1/volumes/V/snapshots/ID/clone` | Clone into another volume; root access required |
| `DELETE` | `/v1/volumes/V/snapshots/ID` | Delete snapshot metadata and payload |
| `POST` | `/v1/volumes/V/forks` | Fork the live filesystem into an empty target volume; root access required |
| `GET` | `/v1/volumes/V/jobs?status=running` | List durable jobs, optionally by status |
| `POST` | `/v1/volumes/V/jobs` | Submit an idempotent durable job |
| `GET` | `/v1/volumes/V/jobs/ID` | Return durable job state and terminal result |
| `GET` | `/v1/volumes/V/jobs/ID/logs?after=N` | Page persisted binary-safe stdout/stderr |
| `POST` | `/v1/volumes/V/jobs/ID/cancel` | Cancel queued or running work |
| `GET` | `/v1/volumes/V/schedules` | List UTC cron schedules |
| `POST` | `/v1/volumes/V/schedules` | Create an enabled schedule `{"name","cron","command","cwd"}` |
| `POST` | `/v1/volumes/V/schedules/ID/enable` | Enable and recalculate the next run |
| `POST` | `/v1/volumes/V/schedules/ID/disable` | Disable a schedule |
| `DELETE` | `/v1/volumes/V/schedules/ID` | Delete a schedule |
| `GET` | `/v1/volumes/V/changes/path?since=N&wait=25000` | Read or long-poll ordered filesystem changes |
| `GET` | `/v1/volumes/V/webhooks` | List change-feed webhook subscriptions without signing secrets |
| `POST` | `/v1/volumes/V/webhooks` | Create a durable signed webhook `{"url","pathPrefix","events"}` |
| `DELETE` | `/v1/volumes/V/webhooks/ID` | Delete a webhook and its pending deliveries |
| `POST` | `/v1/volumes/V/search` | Bounded server-side `find`, glob, or grep under a path prefix |
| `GET` | `/v1/volumes/V/tree/P` | Bounded structured directory tree; accepts `depth` and `limit` |
| `GET`, `PUT` | `/v1/volumes/V/quota` | Read or configure logical-byte and inode limits |
| `GET` | `/v1/volumes/V/auth` | Report whether deployment auth is enabled and a volume password is set |
| `POST` | `/v1/volumes/V/auth/password` | Set or rotate the volume password (root, admin, or current password) |
| `POST` | `/v1/volumes/V/auth/login` | Exchange the volume password for a scoped capability token |
| `GET` | `/v1/volumes/V/capabilities` | Return auth mode and caller identity |
| `POST` | `/v1/volumes/V/capabilities` | Mint a scoped capability using root access |
| `DELETE` | `/v1/volumes/V/capabilities/ID` | Revoke a capability using root access |
| `GET` | `/v1/volumes/V/sites` | Report the published-site status |
| `PUT` | `/v1/volumes/V/sites` | Publish or update the public web root `{"path","indexDocument","spa","directoryListing","cacheControl"}` |
| `DELETE` | `/v1/volumes/V/sites` | Unpublish the site |
| `GET` | `/v1/volumes/V/shares` | List share links |
| `POST` | `/v1/volumes/V/shares` | Create a share link `{"path","expiresInSeconds","cacheControl"}` |
| `DELETE` | `/v1/volumes/V/shares/ID` | Delete a share link |
| `GET` | `/s/V/path` | Public, unauthenticated static-site serving with MIME, index, and SPA fallback |
| `GET` | `/d/V/ID` | Public, unauthenticated share-link download |
| `GET` | `/v1/volumes/V/usage` | Return filesystem, SQLite, Container, and Hrana usage |

Filesystem failures return structured JSON with stable POSIX-style codes and appropriate HTTP statuses.

Volumes default to 256 KiB chunks. Explicit chunk sizes must be powers of two from 4 KiB through 1 MiB. Existing volumes retain their stored chunk size, and a conflicting size returns `409 CHUNK_SIZE_IMMUTABLE` after filesystem data exists. Any filesystem request implicitly creates an unconfigured volume with the default.

File writes do not create missing parent directories. Create the directory first with `PUT /v1/volumes/V/directories/path`; otherwise the write returns `ENOENT`.

File responses include:

- `Content-Type: application/octet-stream`
- `Content-Length`
- `Accept-Ranges: bytes`
- `ETag` from the inode, filesystem change sequence, and size
- `Last-Modified` from the AgentFS inode
- `X-AiryFS-Inode`
- `Content-Range` and status `206` for a valid single range

Reads honor `If-None-Match` and `If-Modified-Since` with a bodyless `304`. `If-None-Match` takes precedence when both are present. Range reads honor `If-Range`; a stale validator returns the complete representation with `200` instead of unsafe partial content.

Change-feed webhooks use a durable SQLite outbox populated by the same triggers that observe direct and FUSE/Hrana writes. Create one with `airy webhook create https://hooks.example.com/airy --path /src --event create --event modify`. AiryFS posts `{ "volume", "event" }` and includes `X-AiryFS-Delivery` plus `X-AiryFS-Signature: sha256=...`, computed over the exact body with the signing secret shown once at creation. Delivery failures retry with bounded exponential backoff. Webhook endpoints must use HTTPS; management requires `admin` access.

Content-addressed assets are immutable through the asset API and stored by SHA-256. `airy asset put ./bundle.wasm` hashes locally, streams to a temporary remote file, and publishes only after the server independently verifies the digest. Re-uploading the same digest is idempotent. `airy asset get SHA256 [local]` downloads it. Asset responses use `Cache-Control: public, max-age=31536000, immutable` and support the standard AiryFS validators and ranges.

Scheduled jobs use five-field UTC cron expressions or `@hourly`, `@daily`, `@weekly`, `@monthly`, and `@yearly` aliases. `airy schedule create build '*/15 * * * *' --cwd /site npm run build` persists the schedule and submits each occurrence through the existing durable, idempotent job queue. A crash after submission is safe: the occurrence's idempotency key is derived from the schedule and scheduled timestamp. Creating or changing schedules requires `admin` access because execution can continue after the caller's token expires.

Server-side search does not start the Container. `airy find /src --name config` uses a transactionally maintained FTS5 trigram index for basename substring lookup, including files written through FUSE. `airy glob '**/*.test.ts' /src` and `airy grep needle /src --ignore-case` traverse AgentFS directly. Grep skips binary files and files over 10 MiB, scans at most 100 MiB per request, and returns line/column metadata. Traversal modes cap work at 100,000 entries; every mode caps results at 1,000.

`airy tree /src --depth 3` renders a structured server-side walk without starting the Container. The API returns path, depth, type, and logical size for up to 100,000 entries, with explicit truncation metadata.

`airy volume fork working-copy` streams a point-in-time-consistent copy of the live filesystem into an empty target volume. The fork preserves the source chunk size, refuses to overwrite existing target files, and becomes fully independent after creation. Cross-volume forks require root authentication or an auth-disabled deployment.

`airy volume quota --bytes 10g --inodes 100000` configures persistent logical-byte and inode limits. Use `unlimited` to clear either limit. SQLite triggers enforce quotas for direct HTTP writes and Container/FUSE writes at the shared filesystem boundary; rejected HTTP writes return `507 ENOSPC`. `airy usage` reports logical usage, configured limits, remaining capacity, physical SQLite size, and Container/FUSE health.

`airy tail /logs/app.log` prints the last ten lines; `--bytes` selects a byte window and `--follow` streams appends. Follow mode composes range reads with the filesystem change feed, so it does not hold a Worker request or start the Container. `--retry` waits for a removed or rotated path to reappear.

Direct API and CLI deletes move paths into durable per-volume trash by default. `airy trash list`, `airy trash restore ID`, and `airy undo` recover deleted files, directory subtrees, and symlinks; `airy rm --permanent` and `airy trash purge ID` reclaim space immediately. Trashed content continues to count against quota until purged. Deletes performed inside the Container through FUSE remain permanent because that path exposes only opaque filesystem SQL; take a snapshot before destructive `exec` operations when recovery is required.

Volumes are mountable over WebDAV at `/dav/<volume>/`. The dependency-free adapter supports `OPTIONS`, finite `PROPFIND`, `GET`, `HEAD`, streaming `PUT`, `MKCOL`, recoverable `DELETE`, same-volume `MOVE` and bounded recursive `COPY`, no-op `PROPPATCH`, and Finder-compatible `LOCK`/`UNLOCK`. It advertises WebDAV classes 1 and 2, supports HTTP ranges and validators, hides internal trash, and enforces bearer capability scopes. When authentication is enabled, WebDAV also accepts Basic authentication with the root credential, a capability token, or the volume password.

`airy exec --pty <command>` runs interactive terminal applications against the mounted volume. The CLI obtains a 30-second single-use ticket, upgrades to a binary WebSocket, forwards raw terminal input and resize events, and restores local terminal mode on every exit path. PTY sessions share the volume's single execution slot with buffered, streaming, and durable commands.

Preview services persist a command definition in Durable Object SQLite while the process remains disposable Container compute. `airy service create web --public -- node server.js` allocates `$PORT` from 5000–5015, starts independently of foreground exec/PTY work, and publishes at `/p/<volume>/web/`. Enabled services restart lazily after Container sleep or replacement when the next proxy request arrives. Commands must listen on `$PORT`; public exposure is opt-in.

```sh
# macOS Finder: Go > Connect to Server
https://example.workers.dev/dav/my-volume/

# Command-line discovery
curl -u airy:$AIRYFS_TOKEN -X PROPFIND -H 'Depth: 1' \
  https://example.workers.dev/dav/my-volume/
```

Unsatisfiable ranges return `416` with `Content-Range: bytes */SIZE`. Directory listings include `name`, `ino`, `mode`, `nlink`, `uid`, `gid`, `size`, timestamps, and a normalized `type` of `file`, `directory`, `symlink`, or `other`.

Errors use `{ "error": { "code", "message", "path"? } }`. Known filesystem codes map to HTTP statuses, including `ENOENT` to `404`, conflicts such as `EEXIST` and `ENOTEMPTY` to `409`, `EINVAL` to `400`, `EPERM` to `403`, and `ENOSPC` to `507`. Method errors include an `Allow` header.

### Workers RPC

The `AiryFS` class exposes methods for applications that already hold a Durable Object stub:

- `readFile`, `writeFile`, `readFileStream`, and `writeFileStream`
- `statPath`, `listDir`, and `listDirDetailed`
- `makeDir`, `removePath`, `renamePath`, and `copyPath`
- `createSymlink` and `readSymlink`
- `exportTreeStream`, `importTreeStream`, and `checksum`
- `beginUpload`, `uploadStatus`, `appendUpload`, `completeUpload`, and `abortUpload`
- `createSnapshot`, `listSnapshots`, `diffSnapshot`, `restoreSnapshot`, `deleteSnapshot`, `exportSnapshotStream`, and `cloneSnapshot`
- `submitJob`, `listJobs`, `getJob`, `getJobLogs`, `cancelJob`, and `runNextJob`
- `getChanges`, `usage`, `dbInfo`, `exec`, `execStream`, `cancelExec`, and `destroyContainer`

### Compatibility Endpoints

The original query-oriented endpoints remain available:

```text
POST /fs/write?volume=V&path=/file.txt
GET  /fs/read?volume=V&path=/file.txt
GET  /fs/ls?volume=V&path=/
POST /exec?volume=V
POST /destroy?volume=V
GET  /usage?volume=V
GET  /db-info?volume=V
GET  /perf?volume=V
POST /kv/set?volume=V&key=K
GET  /kv/get?volume=V&key=K
```

The compatibility file endpoints use the same binary-safe streaming, range, atomic replacement, and access-coordination implementation as the v1 API.

### Execution Contract

`exec` validates a non-empty command, starts and mounts the Container if necessary, and sends the command to the Container command server. Commands run with:

- `cwd=/volume`
- `HOME=/root`
- A standard system `PATH`
- A 300-second process timeout, bounded by a 310-second Worker-side Container request deadline
- A 10 MiB output buffer

The response contains `exitCode`, `stdout`, and `stderr`. A dead FUSE daemon produces a structured `503` instead of running the command in an unmounted directory. Startup has a separate 60-second bound. The retry-safe `:` preflight also uses that shorter bound so an abandoned warmup cannot retain the execution lock for the full command timeout.

Streaming exec emits bounded NDJSON lines with a generated execution ID, base64 stdout/stderr chunks, and one terminal exit event. Cancellation sends `SIGTERM` to the process group and escalates to `SIGKILL`; disconnecting the streaming client also terminates the admitted process. Interactive and durable commands share one execution slot.

Durable jobs persist before scheduling and require an `Idempotency-Key` over HTTP. The queue claims one job at a time, persists binary stdout/stderr as ordered BLOB rows, caps retained output at 50 MiB, supports queued/running cancellation, and marks orphaned running jobs failed before recycling their Container. Retrying the same idempotency key returns and reschedules the existing queued job rather than duplicating execution.

The change feed uses SQLite triggers on AgentFS inode and dentry tables, so it observes both direct API mutations and writes originating through Container/FUSE. Per-volume sequence numbers order create, modify, remove, and rename events. The latest 10,000 sequence values are retained; clients receive `gap: true` when their cursor predates that window and should resynchronize before continuing.

### Usage And Health

`GET /v1/volumes/V/usage` and `GET /usage?volume=V` return:

- AgentFS logical filesystem statistics from `statfs`
- Physical Durable Object SQLite database size in bytes
- Container SDK state and Hrana connection state
- Bridge startup state
- FUSE mount and daemon-exit state
- Container working directory
- Hrana pipeline and SQL statement counters

`GET /db-info?volume=V` returns the row count for every AiryFS and AgentFS schema table. `GET /perf?volume=V` returns the current Hrana session counters alone. The Container's internal `/health` endpoint reports `bridgeStarted`, `fuseMounted`, `fuseExitCode`, and `cwd` to the Durable Object lifecycle manager.

## Data Model

AgentFS stores the filesystem as normalized SQLite tables:

- `fs_inode` stores mode, ownership, size, link count, device number, and timestamps.
- `fs_dentry` maps a parent inode and name to a child inode.
- `fs_data` stores file content in indexed chunks. Volumes default to 256 KiB and support immutable power-of-two sizes from 4 KiB through 1 MiB.
- `fs_symlink` stores symbolic-link targets.
- `fs_config` stores filesystem configuration and schema version.
- `fs_whiteout`, `fs_overlay_config`, and `fs_origin` support overlay metadata.
- `fs_mutation_journal` records direct writes, deletes, renames, copies, links, directory changes, and truncation for active FUSE cache invalidation.
- `fs_open_inode` records persistent, expiring open-handle leases so a direct unlink or streaming rename-over retains a file that a remote FUSE handle is still reading. A trigger on `fs_inode` deletion cascades chunk, symlink, and lease cleanup atomically.
- `fs_snapshot` and snapshot payload tables store immutable full-volume metadata and file chunks for diff, restore, export, and clone.
- `fs_upload` stores durable resumable-upload identity, target, expected size/checksum, and current offset; partial bytes remain in hidden AgentFS files until publish or abort.
- `fs_job` and `fs_job_log` store durable execution admission, state, terminal results, cancellation intent, and binary output.
- `fs_change_sequence` and `fs_change_feed` provide an ordered, bounded mutation feed without changing AgentFS or perturbing `last_insert_rowid()`.
- `capability_revocations` stores revoked signed capability IDs.
- `kv_store` stores application key-value records.
- `tool_calls` stores AgentFS tool-call records.

Schema initialization is idempotent. The initializer recreates missing tables after an interrupted setup and runs supported migrations inside `transactionSync`. Current migrations add missing v0.2/v0.4 inode columns, replace the older whiteout layout, add overlay configuration, and rebuild older tool-call tables with status and nullable completion fields. Arbitrary malformed or independently modified schemas are not repaired automatically.

## Performance Model

The two paths have different cost profiles by design.

| Operation | Expected behavior |
|---|---|
| Direct read, write, or listing | Runs in the Durable Object without a Container |
| First `exec` | Starts the Container, bridge, TCP session, and FUSE mount |
| Warm `exec` | Reuses the running Container and mounted volume, then reattaches the request-scoped Hrana data channel |
| FUSE file operation | Adds a Container-to-DO round trip and SQL execution |

### Billing Model

AiryFS does not make Durable Object usage free. Cloudflare bills SQLite-backed Durable Objects for requests, active compute duration, rows read and written, and stored SQL data. An inactive Durable Object that is eligible for hibernation does not incur duration charges, but its stored data remains billable. During Container execution, AiryFS keeps outbound TCP bridge connections open; active outbound connections prevent hibernation and can continue DO duration billing. See Cloudflare's [Durable Objects pricing](https://developers.cloudflare.com/durable-objects/platform/pricing/) for current included usage and rates.

Containers are billed separately for provisioned memory and disk while running and for active CPU usage. Charges start when the Container is requested or manually started and stop after it sleeps. AiryFS therefore scales Container compute to zero between execution sessions while leaving the self-contained Durable Object volume available to direct APIs. See Cloudflare's [Containers pricing](https://developers.cloudflare.com/containers/pricing/) for current rates and included usage.

Development measurements have typically shown direct file operations below 100 ms, a warm `exec` in roughly 2-10 seconds, and a cold mount around 30 seconds. Metadata-heavy programs can be much slower because every FUSE operation crosses the Container-to-Durable Object boundary; an integration Git commit has taken more than two minutes while a subsequent clean `git status` took about 21 seconds. Deployment location, Container state, command behavior, and syscall count all affect these numbers.

For best results:

- Use direct writes for file ingestion, generated source trees, and bulk updates.
- Use direct reads for inspection, API responses, and artifact retrieval.
- Use `exec` for computation that benefits from existing Linux tools.
- Avoid generating large metadata-heavy trees one syscall at a time through FUSE when the direct API can create them more efficiently.

## Consistency And Current Limits

AiryFS coordinates direct access and FUSE mutations, but it does not yet implement every POSIX or transactional guarantee.

- File-content reads, streaming reads, and direct mutations use fair path-scoped locks. Metadata reads such as stat, directory listing, and readlink do not currently hold a read lock and may interleave with mutations.
- FUSE writes use a volume-wide lock because Hrana SQL statements do not carry normalized filesystem paths.
- AiryFS remote mounts use bounded one-second entry and attribute caches, disable FUSE writeback caching in bounded mode, and poll direct-mutation journal rows every 100 milliseconds in batches of up to 256. Entry invalidations run through FUSE's deferred notification queue on a transport channel independent from ordinary FUSE SQL. Visibility remains asynchronous and the five-second deployed gate includes reconnect and exec overhead.
- File replacement through the HTTP streaming API is atomic after upload completion.
- Directory archive imports stage the complete tree and publish it under a write lock; failed publication rolls back the previous target.
- Snapshot create, diff, restore, delete, and root clone use whole-volume coordination. Restore and clone replace the live namespace and recycle attached compute before later execution.
- Resumable uploads enforce sequential offsets, 1 MiB chunks, per-chunk SHA-256, and full-file SHA-256 before atomic publication.
- Open inodes survive concurrent removal. A live remote FUSE handle pins its inode in `fs_open_inode`, so a direct unlink or streaming rename-over drops the pathname immediately but retains the inode and its data until the handle closes or its 120-second lease expires. A heartbeat renews live handles and aborts the mount if renewal fails for too long (kept below the TTL) so a handle never outlasts its lease. Cleanup of a stale lease left by a mount that vanished is lazy: its inode is reaped when a bounded mount next runs its heartbeat reap, not by a proactive alarm. See [`docs/OPEN_INODE_LEASES.md`](docs/OPEN_INODE_LEASES.md).
- Durable Object SQLite does not support an explicit transaction spanning separate Hrana requests. `BEGIN`, `COMMIT`, `ROLLBACK`, `SAVEPOINT`, and `RELEASE` are compatibility no-ops. Batch locking prevents interleaving but does not provide rollback across separate remote requests.
- Hrana integer bindings are limited to JavaScript's safe integer range because Durable Object SQLite bindings do not accept `bigint`.

## Security

Authentication is opt-in. Set `AIRYFS_AUTH_SECRET` to require `Authorization: Bearer ...` on HTTP requests. The configured value is the root administrative credential and the base secret from which each volume's capability signing key is derived. Capability signing keys are derived per volume with HKDF-SHA256, so a token minted for one volume cannot be verified against another even under the same deployment secret. Leave the secret unset only for trusted local/test deployments.

Root callers can mint expiring capabilities restricted to one volume, a subset of `read`, `write`, `exec`, and `admin`, and normalized path prefixes. Every capability request verifies its signature, expiry, volume, operation, path scope, and revocation state. Capability IDs can be revoked immediately. Cross-volume snapshot clone remains root-only because a capability is bound to one source volume.

Each volume can also carry its own password, stored in the volume's SQLite as a PBKDF2 verifier (never as plaintext). `POST /v1/volumes/V/auth/password` sets or rotates it (authorized by the root credential, an `admin` capability, or the current password), and `POST /v1/volumes/V/auth/login` exchanges the password for a volume-scoped `read,write,exec` capability without needing the root secret. This lets a volume be secured at creation time and accessed from multiple machines. `GET /v1/volumes/V/auth` reports whether auth is enabled and a password is set. Password auth requires `AIRYFS_AUTH_SECRET` to be configured, because the minted token is signed with the derived per-volume key.

The CLI stores an optional bearer token in its named session and sends it on every request. The TypeScript SDK accepts `token` and additional default headers through `AiryFSClient` options. Tokens are credentials: do not commit them, put them in URLs, or pass them through untrusted command arguments.

Browser uploads reuse capability authorization without placing credentials in URLs. `airy browser-upload /inbox/photo.jpg --expires 15m` mints a write-only capability restricted to that exact path and prints the upload endpoint and token. Browser code streams the `File` directly:

```js
await fetch(uploadUrl, {
  method: 'POST',
  headers: { Authorization: `Bearer ${token}` },
  body: file,
});
```

The endpoint supports CORS `OPTIONS` preflight and accepts a raw file body rather than `multipart/form-data`, so the Worker does not buffer the complete upload. The parent directory must already exist. Treat the generated token as a credential and revoke its capability ID when it is no longer needed.

Authentication does not make arbitrary execution safe for mutually untrusted users who share a volume. `exec` and durable jobs run shell commands in the attached Container with access to the complete mounted volume. Deployments still need an appropriate identity, command policy, request limits, and volume-isolation model for their product.

## Web Hosting

A volume can serve content publicly without a bearer token. Public serving is opt-in per volume: nothing is exposed until a site is published or a share link is minted, and both are served straight from Durable Object SQLite with no Container cold start.

Static site hosting publishes a volume subtree as a web root. Directory requests resolve the index document. `--listing` opts into generated directory indexes when no index exists. Unknown paths fall back to the index when SPA mode is enabled. Files include an inferred `Content-Type`, an optional `Cache-Control`, and `ETag`/`Last-Modified` validators for conditional browser and CDN requests.

```bash
airy upload -r ./dist /site        # push a built static site into the volume
airy site publish /site --spa --cache "public, max-age=300"
airy site publish /downloads --listing  # optional browsable file tree
# served at https://<endpoint>/s/<volume>/
airy site status
airy site unpublish
```

Deployments can snapshot the volume and transactionally replace an existing published root:

```bash
airy site publish /site
airy site deploy ./dist
# prints the rollback snapshot name
airy site rollback site-deploy-2026-07-18T12-00-00-000Z
```

`site deploy` requires an existing publication so routing configuration does not change during cutover. Tree import stages and atomically renames the replacement. `site rollback` restores the named full-volume snapshot, not only the web root, so unrelated changes made after that snapshot are also reverted.

File shares mint unguessable, optionally expiring links to individual files:

```bash
airy share /reports/q3.pdf --expires 24h
# prints https://<endpoint>/d/<volume>/<id>
airy share list
airy share rm <id>
```

Path-based URLs (`/s/<volume>/...` and `/d/<volume>/<id>`) work on any deployment, including `workers.dev`, with no DNS setup. To serve sites on their own hostnames, set the `SITES_ZONE` Worker variable (for example `sites.example.com`) and add a wildcard route (`*.sites.example.com/*`) for the Worker. Requests to `<volume>.sites.example.com` are then served as that volume's published site. Arbitrary custom domains can be layered on later through Cloudflare for SaaS custom hostnames.

Publishing exposes the selected subtree to anyone with the URL. Do not publish a volume that also holds private files outside the published web root, and prefer a dedicated public subtree.

## Deployment

### Prerequisites

- Wrangler 4.x
- Node.js 22 or newer for the Worker, Container TypeScript build, and CLI
- Docker for the Container image and AgentFS cross-compilation
- A Cloudflare account with Containers enabled

### Build AgentFS

The repository includes a pristine pinned AgentFS snapshot and the ordered AiryFS patch series under `agentfs/`. Build the patched TypeScript SDK and the `linux/amd64` Rust binary with:

```bash
./agentfs/build.sh
```

The script verifies every patch, tests the TypeScript SDK and Rust crates, uses Rust 1.88 in Docker, and writes `container/bin/agentfs`. Networks using a private TLS root can provide it to the build Container:

```bash
DOCKER_CA_CERT=/path/to/root.crt ./agentfs/build.sh
```

See [`docs/AGENTFS_PATCHES.md`](docs/AGENTFS_PATCHES.md) for upstream refresh and patch maintenance.

### Deploy

The fastest path from a clone to a working session is the CLI, which wraps the same Wrangler machinery from inside the repository:

```bash
export CLOUDFLARE_API_TOKEN=your-api-token   # and CLOUDFLARE_ACCOUNT_ID or .dev.vars

airy deploy int --allow-dirty                # deploy, set AIRYFS_AUTH_SECRET, create a session
airy init int --volume myproject --password  # deploy, create a session, and secure a volume in one step
```

`airy deploy` runs `scripts/provision.mjs`, which deploys the Worker, generates and stores `AIRYFS_AUTH_SECRET`, discovers the workers.dev URL, and creates a local session pointing at it (holding the root credential). `airy init` additionally creates a volume, sets its password, and downgrades the session to a scoped token. Both must be run from within the repository because the deploy builds the Worker and Container from source.

The checked-in Wrangler configuration defines isolated `int` and `prod` environments. Wrangler names their Workers `airyfs-int` and `airyfs-prod`; their Durable Object namespaces and Container applications remain separate. Local development uses `airyfs-local` with state under `.airyfs/local` and never binds remote resources.

Keep `CLOUDFLARE_API_TOKEN` in the shell environment. Set `CLOUDFLARE_ACCOUNT_ID` in the shell or a gitignored root `.dev.vars` (see `.dev.vars.example`). Set `AIRYFS_AUTH_SECRET` as a Worker secret in environments that require HTTP authentication. The deploy helper rejects an explicit/ambient account disagreement and passes the resolved account to Wrangler, avoiding membership-based account discovery.

```bash
cd worker
export CLOUDFLARE_API_TOKEN=your-api-token

# Local-only Worker, DO SQLite, and Container state
npm run dev

# Validate the integration deployment without publishing
npm run deploy:check -- int --allow-dirty

# Deploy integration from a dirty development tree
npm run deploy:int -- --allow-dirty

# Production requires a clean tree and an explicit guard
npm run deploy:prod -- --allow-prod
```

The deployment helper accepts only `int` and `prod`. Same-environment deploys are serialized, Container rollout is immediate, and each push uses a temporary Docker configuration without OS credential helpers. Production deployments reject `--allow-dirty`; dry runs may inspect a dirty tree and do not require `--allow-prod`.

Use the package scripts for cloud deployments. Invoking Wrangler directly bypasses the clean-tree, production-confirmation, account-consistency, locking, and Docker credential safeguards.

Wrangler builds and publishes the environment-specific Container image, then deploys the Worker and SQLite-backed Durable Object class. Integration and production can coexist in one account without sharing Worker, Durable Object, or Container identities.

The example `wrangler.jsonc` sets `max_instances` to 50. All named volumes remain directly addressable through their Durable Objects, but no more than 50 attached Container instances can run concurrently unless this deployment limit is changed.

### Verify

```bash
BASE=https://your-worker.workers.dev

curl -X PUT "$BASE/v1/volumes/test/files/hello.txt" --data-binary 'hello world'
curl "$BASE/v1/volumes/test/files/hello.txt"

curl -X POST "$BASE/v1/volumes/test/exec" \
  -H "Content-Type: application/json" \
  -d '{"command":"cat hello.txt"}'
```

## Testing

### Worker Tests

```bash
cd worker
npm test
npm run typecheck
```

The 256 Worker tests cover Hrana framing and execution, transport bounds, schema migration, locking, binary/range streaming, archives and transactional tree import, authentication and capability scope, snapshots, resumable uploads and checksums, streaming exec ownership, durable jobs, trigger-driven change feeds, open-handle leases, chunk-size boundaries, and POSIX-style HTTP errors. `npm run test:deploy` covers account selection, fixed environment targeting, production guards, and Docker configuration sanitization.

### Container Build

```bash
cd container
npm run build
npm test
```

The 18 Container tests cover bounded bridge admission, FIFO response handling, cancellation, connection-generation replacement, request limits, streaming exec events, process-group termination, disconnect cleanup, and buffered/streaming slot coordination.

### CLI Tests

```bash
cd cli
npm run typecheck
npm test
npm run build
```

The 169 CLI tests use isolated temporary configuration and local mock servers. They cover session concurrency and auth migration, streaming and resumable files, transactional tree transfer, snapshots, streaming/cancellable exec, durable jobs, change watching, safe startup retries, at-most-once command submission after ambiguous failures, concise gateway errors, shell behavior, and completion. They do not access `~/.airyfs` or a deployed endpoint.

### TypeScript SDK Tests

```bash
cd sdk
npm test
npm run typecheck
npm run build
```

The SDK contract suite exercises every HTTP resource family, bearer/default headers, structured errors, path normalization, bounded NDJSON, change-feed iteration, durable job waiting/log following, exec start IDs, and resumable Blob uploads. Its build emits strict ESM JavaScript, declarations, and source maps without runtime dependencies.

### AgentFS Tests

The vendored Rust SDK and CLI test suites run in a Linux build environment with libSQL's native dependencies as part of `./agentfs/build.sh`. Verify the generated Linux binary with Docker before deployment, for example `docker run --rm --platform linux/amd64 -v "$PWD/container/bin/agentfs:/usr/local/bin/agentfs:ro" debian:bookworm-slim agentfs --version`.

### End-To-End Tests

```bash
AIRYFS_URL=https://your-worker.workers.dev ./e2e/test.sh
AIRYFS_URL=https://your-worker.workers.dev node ./e2e/features.mjs
```

The original end-to-end flow covers direct write to FUSE read, direct mutation invalidation, FUSE write to direct read, Git on the same mixed-access volume, open-handle leases (a held FUSE read that survives a direct unlink and a streaming rename-over), and persistence across Container destruction. The feature smoke covers change feeds, tree archives, snapshots and cloning, resumable uploads, streaming execution admission and cancellation, and durable jobs.

Remote mounts use a 1-second entry and attribute TTL plus journal-driven invalidation, and lease open handles so live reads survive concurrent direct removal. See [`docs/FUSE_CACHE_TTL.md`](docs/FUSE_CACHE_TTL.md), [`docs/MUTATION_INVALIDATION.md`](docs/MUTATION_INVALIDATION.md), and [`docs/OPEN_INODE_LEASES.md`](docs/OPEN_INODE_LEASES.md) for the implementation and deployed measurements.

### Chunk-Size Benchmark

```bash
cd worker
npm run benchmark:chunks
```

See [`docs/CHUNK_SIZE_BENCHMARK.md`](docs/CHUNK_SIZE_BENCHMARK.md) for the 256 KiB default, row-amplification results, and deployed follow-up measurements.

## Project Structure

```text
airyfs/
  worker/
    src/
      index.ts                  DO class, routing, RPC, and Container lifecycle
      files-api.ts              Streaming resource API and access coordination
      hrana-server.ts           Hrana execution against Durable Object SQLite
      hrana-protocol.ts         Protocol types and frame serialization
      schema.ts                 AgentFS schema initialization and migrations
      auth.ts                   Root and scoped-capability authentication
      archive.ts                Streaming AiryFS tree archive protocol
      snapshots.ts              Full-volume capture, diff, restore, and export
      uploads.ts                Durable resumable upload sessions
      jobs.ts                   Durable queue, state machine, and persisted logs
      change-feed.ts            Trigger-driven ordered filesystem changes
      container-http-stream.ts  Unbuffered Container exec transport
      sse-stream.ts             Internal SSE to public NDJSON translation
    test/                       Vitest unit tests
    wrangler.jsonc              Worker, Container, and Durable Object config
  container/
    src/
      command-server.ts         Setup, mount, exec, and health endpoints
      bridge.ts                 HTTP/libSQL to framed TCP bridge
    Dockerfile
  agentfs/
    upstream/                    Pristine pinned AgentFS source
    patches/                     Ordered AiryFS compatibility patches
    build.sh                     Patch verification, tests, and Linux build
  cli/
    src/                         Typed API client, sessions, commands, and shell
    test/                        Unit and mock-server integration tests
  sdk/
    src/                         Universal typed client, DTOs, and async workflows
    test/                        Full API and high-level helper contract tests
  e2e/
    test.sh                     Deployed end-to-end tests
    features.mjs                Deployed new-feature smoke tests
  docs/                         Design and operational notes
  scripts/
    deploy.mjs                  Guarded int/prod deployment
```

## License

MIT
