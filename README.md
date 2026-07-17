# AiryFS

AiryFS is for applications that need a real, durable filesystem as part of a Cloudflare Durable Object:

1. **Build on a full-featured, performant filesystem from Durable Object code**, then run real Linux programs against those files through the attached Container.
2. **Expose that same filesystem to other applications through a web API or Workers RPC**, without starting the Container for ordinary file access.

This makes the filesystem an application primitive owned by the Durable Object, not a disk hidden behind a compute environment. Durable Object methods can create, inspect, transform, and coordinate files with AgentFS. When they need Git, Python, a compiler, a test runner, or another Linux tool, they can execute it against the same filesystem mounted at `/volume`. Callers that are not part of the Durable Object can consume the same state through AiryFS's HTTP API.

The Durable Object's SQLite database is the only persistent store for the volume. AiryFS does not copy the filesystem into a Container disk, synchronize a second database, or persist file data in object storage or another service.

```text
                         one named volume
                                |
                                v
                  Durable Object SQLite storage
                    (only persistent source)
                         /              \
                        /                \
        direct AgentFS access         Hrana SQL bridge
         no Container required             |
                 |                          v
      HTTP API / Workers RPC       AgentFS FUSE mount at /volume
                                            |
                                            v
                                exec in attached Container
```

AgentFS provides the filesystem semantics and native TypeScript interface inside the Durable Object. AiryFS adds on-demand real-process execution and a complete external service surface around it. Direct calls do not start the Container. Execution starts or reuses the Container, mounts the same SQLite-backed volume through FUSE, and runs with `/volume` as the working directory.

## What You Can Build

- **Coding-agent workspaces:** let an agent manage source files directly in its Durable Object, use AgentFS with a virtual shell for lightweight operations, invoke a real Container for Git, package managers, compilers, and tests, then return artifacts through the web API.
- **Document and data transformation:** accept inputs through a Durable Object method or HTTP upload, inspect and organize them without starting compute, run existing conversion tools in the Container, and stream results back from the same volume.
- **Repository automation:** maintain durable per-repository state, update individual files directly, and attach disposable compute only for operations such as checkout, diff, lint, build, or test.
- **Per-user application storage with execution:** give each user or job an isolated filesystem that application code can query and mutate, while retaining the option to run general-purpose software against it.
- **Durable workflow workspaces:** preserve intermediate files across retries, Container sleep, and Container replacement without adding a separate synchronization or recovery system.

AgentFS's Cloudflare filesystem can already be passed to integrations such as its `just-bash` adapter for virtual command execution inside a Worker or Durable Object. With AiryFS, those same AgentFS-managed files can also be mounted into the attached Container when the workload needs native binaries or full Linux process execution.

## Core Properties

- **One volume, one Durable Object:** each volume name maps to an isolated Durable Object and SQLite database.
- **SQLite-only persistence:** all persistent file content, metadata, links, and directory entries live in Durable Object SQLite.
- **Two access paths, one schema:** direct API calls and Container filesystem operations read and mutate the same AgentFS tables.
- **Container on demand:** reads, writes, listings, and metadata operations do not require a Container. Only `exec` starts the attached Container.
- **Ephemeral compute:** destroying or evicting the Container does not destroy the volume. The next `exec` remounts it from Durable Object SQLite.
- **Normal tools:** software inside the Container sees `/volume` as a mounted filesystem and can use standard file APIs without a AiryFS-specific SDK.

Volume bytes necessarily travel to the Container when a process reads them through FUSE. The distinction is persistence: there is no second durable copy of the volume outside the Durable Object's SQLite storage.

## When To Use AiryFS

AiryFS fits workloads that need the filesystem to be directly programmable state, externally accessible state, and an execution workspace:

- Durable Object applications that model source trees, documents, generated artifacts, or task state as files and directories.
- Agent workspaces that use AgentFS APIs or a virtual shell for common operations, then need native tools for the rest.
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
| File mutations | Atomic replacement after a complete upload, delete, copy, and rename |
| Directory operations | Create, list with metadata, remove, and recursive remove |
| Links | Create symbolic links and read link targets |
| Workers RPC | String and binary streams, stat, detailed listing, mutations, links, usage, database information, lifecycle, and exec |
| Container execution | Run shell commands with `cwd=/volume`, a five-minute timeout, and captured stdout/stderr |
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

AiryFS keeps compute disposable and makes the Durable Object's existing SQLite storage authoritative. The Container is a filesystem client, not the owner of persistent state. There is no clone-back phase and no second durable copy to reconcile. Direct operations stay inside the Durable Object; only workloads that actually need Linux execution pay the Container and FUSE path. Container loss requires a remount, not data synchronization or filesystem recovery.

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

The AgentFS Cloudflare integration demonstrates direct filesystem access backed by Durable Object SQLite. AiryFS extends that model into a complete dual-access service. It adds named volume routing, a resource-oriented HTTP API, Workers RPC methods, streaming and range handling, mutation coordination, schema migration, Container startup and health management, an HTTP-to-TCP bridge, a Hrana server backed by `ctx.storage.sql`, and an on-demand FUSE mount.

AiryFS is not a replacement for AgentFS. It is the Cloudflare Durable Object and Container architecture around AgentFS that makes both access paths operate on one persistent database.

### Why AiryFS Patches AgentFS

AiryFS vendors a pinned pristine snapshot of [AgentFS](https://github.com/tursodatabase/agentfs) and applies an ordered patch series for the Rust SDK and CLI used in the Container. The upstream remote database modes did not provide the arbitrary remote libSQL connection required by AiryFS. The Container must send every filesystem query to AiryFS's local bridge rather than open a local SQLite file or synchronize an embedded replica.

The patch series:

- Uses the open-source `libsql` client across the Rust SDK and CLI.
- Adds `ConnectionPool::new_remote(url, token)` and `AgentFSOptions::with_remote(url)`.
- Adds `--remote-url` and `--auth-token` to `agentfs mount`.
- Adds bounded FUSE cache TTLs and an independent `--invalidation-url` journal poller.
- Adds persistent, expiring `fs_open_inode` leases so a live remote FUSE handle keeps reading after a direct unlink or streaming rename-over.
- Adjusts PRAGMA execution for libSQL compatibility.
- Keeps the upstream AgentFS schema version and default local filesystem behavior.

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

Volumes are created on first use. Start by using AgentFS directly in Durable Object application code, then add execution or external access as needed.

### Use AgentFS Natively In The Durable Object

The repository initializes `this.fs` as an AgentFS Cloudflare filesystem backed by the Durable Object's `ctx.storage.sql`. Application methods added to the `AiryFS` class can use the complete AgentFS filesystem interface directly:

```typescript
async runPython(source: string): Promise<{
  exitCode: number;
  stdout: string;
  stderr: string;
  output: string;
}> {
  // Fast path: these calls execute directly against Durable Object SQLite.
  const releaseWrite = await this.access.acquireWrite('/main.py');
  try {
    await this.fs.writeFile('/main.py', source);
  } finally {
    releaseWrite();
  }
  const inputStats = await this.fs.stat('/main.py');
  if (!inputStats.isFile()) throw new Error('Expected /main.py to be a file');

  // Execution path: mount the same AgentFS tables and run a real process.
  const result = await this.exec('python3 main.py > output.txt') as {
    exitCode: number;
    stdout: string;
    stderr: string;
  };
  if (result.exitCode !== 0) throw new Error(result.stderr);

  // Fast path again: read the Container's output directly from the DO.
  const releaseRead = await this.access.acquireRead('/output.txt');
  let output: string;
  try {
    output = await this.fs.readFile('/output.txt', 'utf8');
  } finally {
    releaseRead();
  }
  return { ...result, output };
}
```

This method is callable over Workers RPC on a `AiryFS` stub. It does not upload files through HTTP or copy them into the Container. The AgentFS calls operate directly in the Durable Object, while `exec` temporarily attaches compute to the same persistent filesystem. The access coordinator preserves AiryFS's content-read and mutation ordering when custom methods run concurrently with RPC, HTTP, or FUSE operations.

The underlying `AgentFS` instance also supports `readdir`, `readdirPlus`, `stat`, `lstat`, `mkdir`, `rm`, `rename`, `copyFile`, `symlink`, `readlink`, `access`, `statfs`, and random-access file handles through `open`. Existing code written against AgentFS's `FileSystem` interface can use this Cloudflare implementation directly. Custom content reads and mutations should use `VolumeAccessCoordinator`, as above, when they can overlap other AiryFS access paths.

For a lightweight virtual shell that stays in the Worker runtime, AgentFS can also back its existing `just-bash` integration. AiryFS complements that path rather than replacing it: use virtual execution for supported shell operations and attach the Container when the workload needs native binaries, system packages, or full process behavior.

### Use The Durable Object Through Workers RPC

The Worker entrypoint in this repository can call the public methods already exposed by `AiryFS`. `getContainer` and `AiryFS` are already imported or declared in `worker/src/index.ts`, and `env.AiryFS` is the namespace binding in `wrangler.jsonc`:

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

The string methods are convenient for small text files. `readFileStream` and `writeFileStream` provide binary streaming, while the metadata and mutation methods expose the rest of the filesystem surface. A separate Worker needs its own binding to the AiryFS Durable Object namespace and a compatible `AiryFS` RPC type.

### Consume The Durable Object Through HTTP

The resource-oriented `/v1` API exposes the same volume to clients that do not use Workers RPC.

```bash
BASE=https://your-worker.workers.dev
VOLUME=myproject
```

#### Write And Read Without A Container

```bash
# Stream a file directly into Durable Object SQLite.
curl -X PUT "$BASE/v1/volumes/$VOLUME/files/main.py" \
  --data-binary 'print("hello from AiryFS")'

# Read it directly. This does not start the Container.
curl "$BASE/v1/volumes/$VOLUME/files/main.py"

# List the volume with POSIX metadata.
curl "$BASE/v1/volumes/$VOLUME/directories/"
```

#### Execute Against The Same Files

```bash
curl -X POST "$BASE/v1/volumes/$VOLUME/exec" \
  -H "Content-Type: application/json" \
  -d '{"command":"python3 main.py"}'
```

The first `exec` starts the attached Container, establishes the bridge, and mounts the volume. The command runs with `/volume` as its working directory. A file created by the command is immediately available through the direct API:

```bash
curl -X POST "$BASE/v1/volumes/$VOLUME/exec" \
  -H "Content-Type: application/json" \
  -d '{"command":"printf generated > output.txt"}'

curl "$BASE/v1/volumes/$VOLUME/files/output.txt"
# generated
```

## Architecture

Each volume is one instance of the `AiryFS` Durable Object class and one attached Container instance.

```text
Client or Worker RPC caller
            |
            v
Worker volume router
            |
            v
AiryFS Durable Object
  |
  |-- ctx.storage.sql
  |     AgentFS schema
  |     only persistent volume state
  |
  |-- Direct path
  |     AgentFS TypeScript Cloudflare adapter
  |     HTTP API and Workers RPC
  |     no Container startup
  |
  +-- Execution path
        HranaServer executes SQL against ctx.storage.sql
            ^
            | framed pipeline protocol over TCP
            v
        Attached Container
          |-- command server on :4000
          |-- HTTP/libSQL bridge on :8080
          |-- TCP bridge endpoint on :9000
          +-- AgentFS FUSE daemon mounts /volume
                    |
                    v
              command execution
```

### Direct Path

The direct path calls the AgentFS TypeScript Cloudflare adapter with the Durable Object's storage context. File reads, writes, listings, metadata operations, and mutations execute synchronously against `ctx.storage.sql`. The Container can remain stopped.

Streaming writes use a temporary AgentFS path and rename it over the destination only after the request body completes. Streaming reads fetch file chunks incrementally and support a single HTTP byte range.

### Execution Path

The first `exec` performs four steps:

1. Start the attached Container and wait for its command server.
2. Start the in-process HTTP-to-TCP bridge.
3. Open a TCP connection from the Durable Object and start the Hrana server.
4. Start the AgentFS FUSE daemon and wait until `/volume` is mounted.

Inside the Container, AgentFS uses `libsql::Builder::new_remote("http://localhost:8080", "")`. A filesystem operation becomes a Hrana HTTP request to the bridge. The bridge forwards a framed request over TCP to the Durable Object. The Durable Object executes the SQL against `ctx.storage.sql` and returns the result along the same path.

The bridge rejects pending requests when a connection drops and applies a response timeout. On replacement, new requests switch to the new generation while already-dispatched work drains on the retired socket. Container startup is single-flight, so concurrent first executions wait for the same mount attempt.

### Persistence And Lifecycle

The Container image, process state, and files outside `/volume` are ephemeral. `/volume` is reconstructed from Durable Object SQLite whenever AgentFS remounts it.

`POST /destroy?volume=V` destroys only the Container. It does not delete the Durable Object or its SQLite data. Direct access continues to work, and a later `exec` starts a new Container and mounts the existing volume.

The Durable Object requests automatic Container sleep after 30 minutes of inactivity. A direct filesystem request does not wake the Container. The next `exec` after sleep starts a new session and remounts the persistent volume.

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
| `POST` | `/v1/volumes/V/exec` | Execute `{"command":"python3 main.py"}` |
| `GET` | `/v1/volumes/V/usage` | Return filesystem, SQLite, Container, and Hrana usage |

Filesystem failures return structured JSON with stable POSIX-style codes and appropriate HTTP statuses.

Volumes default to 256 KiB chunks. Explicit chunk sizes must be powers of two from 4 KiB through 1 MiB. Existing volumes retain their stored chunk size, and a conflicting size returns `409 CHUNK_SIZE_IMMUTABLE` after filesystem data exists. Any filesystem request implicitly creates an unconfigured volume with the default.

File writes do not create missing parent directories. Create the directory first with `PUT /v1/volumes/V/directories/path`; otherwise the write returns `ENOENT`.

File responses include:

- `Content-Type: application/octet-stream`
- `Content-Length`
- `Accept-Ranges: bytes`
- `Last-Modified` from the AgentFS inode
- `X-AiryFS-Inode`
- `Content-Range` and status `206` for a valid single range

Unsatisfiable ranges return `416` with `Content-Range: bytes */SIZE`. Directory listings include `name`, `ino`, `mode`, `nlink`, `uid`, `gid`, `size`, timestamps, and a normalized `type` of `file`, `directory`, `symlink`, or `other`.

Errors use `{ "error": { "code", "message", "path"? } }`. Known filesystem codes map to HTTP statuses, including `ENOENT` to `404`, conflicts such as `EEXIST` and `ENOTEMPTY` to `409`, `EINVAL` to `400`, `EPERM` to `403`, and `ENOSPC` to `507`. Method errors include an `Allow` header.

### Workers RPC

The `AiryFS` class exposes methods for applications that already hold a Durable Object stub:

- `readFile`, `writeFile`, `readFileStream`, and `writeFileStream`
- `statPath`, `listDir`, and `listDirDetailed`
- `makeDir`, `removePath`, `renamePath`, and `copyPath`
- `createSymlink` and `readSymlink`
- `usage`, `dbInfo`, `exec`, and `destroyContainer`

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
- A 120-second process timeout
- A 10 MiB output buffer

The response contains `exitCode`, `stdout`, and `stderr`. A dead FUSE daemon produces `503` instead of running the command in an unmounted directory. A failed mount is terminated and can be retried on the next execution request.

### Usage And Health

`GET /v1/volumes/V/usage` and `GET /usage?volume=V` return:

- AgentFS logical filesystem statistics from `statfs`
- Physical Durable Object SQLite database size in bytes
- Container state
- Bridge startup state
- FUSE mount and daemon-exit state
- Container working directory
- Hrana pipeline and SQL statement counters

`GET /db-info?volume=V` returns the row count for every AiryFS and AgentFS schema table. `GET /perf?volume=V` returns the current Hrana session counters alone. The Container's internal `/health` endpoint reports `bridgeStarted`, `fuseMounted`, `fuseExitCode`, and `cwd` to the Durable Object lifecycle manager.

## Data Model

AgentFS stores the filesystem as normalized SQLite tables:

- `fs_inode` stores mode, ownership, size, link count, device number, and timestamps.
- `fs_dentry` maps a parent inode and name to a child inode.
- `fs_data` stores file content in indexed 4 KiB chunks.
- `fs_symlink` stores symbolic-link targets.
- `fs_config` stores filesystem configuration and schema version.
- `fs_whiteout`, `fs_overlay_config`, and `fs_origin` support overlay metadata.
- `fs_mutation_journal` records direct mutations for active FUSE cache invalidation.
- `fs_open_inode` records persistent, expiring open-handle leases so a direct unlink or streaming rename-over retains a file that a remote FUSE handle is still reading. A trigger on `fs_inode` deletion cascades chunk, symlink, and lease cleanup atomically.
- `kv_store` stores application key-value records.
- `tool_calls` stores AgentFS tool-call records.

Schema initialization is idempotent. The initializer recreates missing tables after an interrupted setup and runs supported migrations inside `transactionSync`. Current migrations add missing v0.2/v0.4 inode columns, replace the older whiteout layout, add overlay configuration, and rebuild older tool-call tables with status and nullable completion fields. Arbitrary malformed or independently modified schemas are not repaired automatically.

## Performance Model

The two paths have different cost profiles by design.

| Operation | Expected behavior |
|---|---|
| Direct read, write, or listing | Runs in the Durable Object without a Container |
| First `exec` | Starts the Container, bridge, TCP session, and FUSE mount |
| Warm `exec` | Reuses the running Container and mounted volume |
| FUSE file operation | Adds a Container-to-DO round trip and SQL execution |

Development measurements have typically shown direct file operations below 100 ms, a warm `exec` in roughly 2-10 seconds, and a cold mount around 30 seconds. Deployment location, Container state, command behavior, and syscall count affect these numbers.

For best results:

- Use direct writes for file ingestion, generated source trees, and bulk updates.
- Use direct reads for inspection, API responses, and artifact retrieval.
- Use `exec` for computation that benefits from existing Linux tools.
- Avoid generating large metadata-heavy trees one syscall at a time through FUSE when the direct API can create them more efficiently.

## Consistency And Current Limits

AiryFS coordinates direct access and FUSE mutations, but it does not yet implement every POSIX or transactional guarantee.

- File-content reads, streaming reads, and direct mutations use fair path-scoped locks. Metadata reads such as stat, directory listing, and readlink do not currently hold a read lock and may interleave with mutations.
- FUSE writes use a volume-wide lock because Hrana SQL statements do not carry normalized filesystem paths.
- AiryFS remote mounts use a bounded one-second cache and poll direct-mutation journal rows every 100 milliseconds. Entry invalidations run through FUSE's deferred notification queue on a transport channel independent from ordinary FUSE SQL. Visibility remains asynchronous and the five-second deployed gate includes reconnect and exec overhead.
- File replacement through the HTTP streaming API is atomic after upload completion.
- Open inodes survive concurrent removal. A live remote FUSE handle pins its inode in `fs_open_inode`, so a direct unlink or streaming rename-over drops the pathname immediately but retains the inode and its data until the handle closes or its 120-second lease expires. A heartbeat renews live handles and aborts the mount if renewal fails for too long (kept below the TTL) so a handle never outlasts its lease. Cleanup of a stale lease left by a mount that vanished is lazy: its inode is reaped when a bounded mount next runs its heartbeat reap, not by a proactive alarm. See [`docs/OPEN_INODE_LEASES.md`](docs/OPEN_INODE_LEASES.md).
- Durable Object SQLite does not support an explicit transaction spanning separate Hrana requests. `BEGIN`, `COMMIT`, `ROLLBACK`, `SAVEPOINT`, and `RELEASE` are compatibility no-ops. Batch locking prevents interleaving but does not provide rollback across separate remote requests.
- Hrana integer bindings are limited to JavaScript's safe integer range because Durable Object SQLite bindings do not accept `bigint`.

## Security

This repository does not currently define an authentication or authorization model. The `exec` endpoint runs arbitrary shell commands in the attached Container, and volume APIs permit reading and modifying persistent data.

Do not expose a deployment to untrusted callers without adding appropriate authentication, authorization, volume access controls, request limits, and command-execution policy for the intended product. Those controls are intentionally left as a deployment and product decision rather than embedded as a generic example mechanism.

## Deployment

### Prerequisites

- Wrangler 4.x
- Docker for the Container image and AgentFS cross-compilation
- A Cloudflare account with Containers enabled

### Build AgentFS

The repository includes a pristine pinned AgentFS snapshot and the ordered AiryFS patch series under `agentfs/`. Build the patched TypeScript SDK and the `linux/amd64` Rust binary with:

```bash
./agentfs/build.sh
```

The script verifies every patch, tests the TypeScript SDK and Rust crates, uses Rust 1.88 in Docker, and writes `container/bin/agentfs`. The compatibility entrypoint `./container/scripts/build-agentfs.sh` delegates to the same build. Networks using a private TLS root can provide it to the build Container:

```bash
DOCKER_CA_CERT=/path/to/root.crt ./agentfs/build.sh
```

See [`docs/AGENTFS_PATCHES.md`](docs/AGENTFS_PATCHES.md) for upstream refresh and patch maintenance.

### Deploy

AiryFS generates one account-pinned Wrangler config per environment. Worker names, Durable Object namespaces, and Container applications are isolated as `airyfs-<env>` and `airyfs-<env>-airyfs`; local development uses `airyfs-local` with state under `.airyfs/local` and never binds remote resources.

Keep `CLOUDFLARE_API_TOKEN` in the shell environment. Set `CLOUDFLARE_ACCOUNT_ID` in the shell or a gitignored root `.dev.vars` (see `.dev.vars.example`). The deploy helper rejects an explicit/ambient account disagreement and passes the resolved account to Wrangler, avoiding membership-based account discovery.

```bash
cd worker
export CLOUDFLARE_API_TOKEN=your-api-token

# Local-only Worker, DO SQLite, and Container state
npm run dev

# Render and validate an isolated integration deployment
npm run cloud:render -- int
npm run cloud:check -- int --allow-dirty

# Deploy integration from a dirty development tree
npm run deploy:int -- --allow-dirty

# Production requires a clean tree and an explicit guard
npm run deploy:prod -- --allow-prod
```

Generated `worker/wrangler.generated.<env>.jsonc` files and local deployment locks are ignored. Same-environment deploys are serialized, Container rollout is immediate, and each push uses a temporary Docker configuration without OS credential helpers. Production rejects `--allow-dirty`; dry runs do not require `--allow-prod`.

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

The 82 Worker tests cover Hrana framing and execution, transport frame limits, schema creation and migration, stored SQL, locking, binary streaming, ranges, atomic replacement, metadata operations, direct truncation, mutation journaling, open-handle lease retention, chunk-size boundaries, and POSIX-style HTTP errors. `npm run test:cloud` covers deployment rendering, account selection, environment validation, production guards, and Docker credential isolation.

### Container Build

```bash
cd container
npm run build
npm test
```

### AgentFS Tests

The vendored Rust SDK and CLI test suites run in a Linux build environment with libSQL's native dependencies as part of `./agentfs/build.sh`. Verify the generated Linux binary with Docker before deployment, for example `docker run --rm --platform linux/amd64 -v "$PWD/container/bin/agentfs:/usr/local/bin/agentfs:ro" debian:bookworm-slim agentfs --version`.

### End-To-End Tests

```bash
AIRYFS_URL=https://your-worker.workers.dev ./e2e/test.sh
```

The end-to-end flow covers direct write to FUSE read, direct mutation invalidation, FUSE write to direct read, Git on the same mixed-access volume, open-handle leases (a held FUSE read that survives a direct unlink and a streaming rename-over), and persistence across Container destruction.

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
    test/                       Vitest unit tests
    wrangler.jsonc              Worker, Container, and Durable Object config
  container/
    src/
      command-server.ts         Setup, mount, exec, and health endpoints
      bridge.ts                 HTTP/libSQL to framed TCP bridge
    scripts/
      build-agentfs.sh          Linux AgentFS build
    Dockerfile
  e2e/
    test.sh                     Deployed end-to-end tests
  docs/
    CHUNK_SIZE_BENCHMARK.md
```

## License

MIT
