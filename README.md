# AiryFS

AiryFS is a cloud filesystem built on Cloudflare Durable Objects. Each volume is a complete filesystem stored in one Durable Object's SQLite database, and that database is the only durable copy of the data.

Applications work with a volume in two ways. Ordinary file operations such as reads, writes, listings, and metadata run inside the Durable Object and never start any other compute. When a workload needs Git, Python, a compiler, or another native Linux tool, AiryFS starts an on-demand Container and exposes the same SQLite rows at `/volume` through FUSE. The Container is a disposable client of the volume, not its owner. There is no second durable copy to synchronize, no object store behind the mount, and no clone-back step when compute goes away.

```text
   HTTP clients, TypeScript SDK, CLI        Workers holding a DO stub
                  |                                   |
                  v                                   |
            Worker router                             |
                  |                                   |
                  +----------------+------------------+
                                   v
                        AiryFS Durable Object
                    owns the volume in its SQLite DB
                       |                      |
          file operations                    exec
          answered here against              starts the Container
          ctx.storage.sql,                   and sends it the command
          no Container involved                       |
                                                      v
                                             Attached Container
                                          runs the command in /volume,
                                          a FUSE mount whose syscalls
                                          become SQL sent back to the
                                          Durable Object's database
```

Every volume request routes through its AiryFS Durable Object. Direct operations are answered there without starting the Container. Execution attaches a Container that reads and writes the volume by sending SQL back to the same Durable Object. Once a FUSE write commits, the direct API sees it immediately. Changes made through the direct path are delivered to mounted FUSE clients asynchronously through journal-driven invalidation, with bounded cache TTLs as a fallback. Deployment-wide volume listing is the exception: it uses a separate registry Durable Object.

Four interfaces operate on the same volume.

- **AgentFS inside the Durable Object.** Application code uses AgentFS, the embedded SQLite filesystem library AiryFS builds on, directly against `ctx.storage.sql`.
- **HTTP and Workers RPC.** Remote applications stream files, inspect metadata, mutate paths, and run commands.
- **The TypeScript SDK.** Node, browser, and Worker applications use a typed client with helpers for jobs, change feeds, and resumable transfers.
- **The `airyfs` CLI.** Developers get familiar filesystem commands, named sessions, bulk transfers, snapshots, jobs, diagnostics, and an interactive shell.

The same volume also supports search, quotas, change feeds, webhooks, snapshots, recovery, static hosting, sharing, WebDAV, S3-compatible access, scoped SQL, and observability. These utilities run in the Durable Object unless they specifically require Linux execution.

## When To Use AiryFS

AiryFS fits workloads that want an isolated cloud filesystem with a small operational footprint. One Durable Object per volume, no external storage service.

- Applications that need file and directory semantics rather than an object-only keyspace.
- Per-user, per-repository, per-project, or per-task storage isolated through one Durable Object per volume.
- Durable Object applications that want colocated, directly programmable files with remote API, SDK, or CLI access.
- Agent, build, and transformation workspaces that need native tools occasionally but should not make compute the owner or gateway for stored files.
- File transfer, publishing, sharing, automation, and recovery workflows that benefit from built-in utilities.
- Systems that want one authoritative SQLite-backed namespace instead of a Container filesystem synchronized to another persistent service.

AiryFS is not optimized for workloads dominated by thousands of sequential metadata operations. Every FUSE syscall crosses the Container-to-DO boundary and executes SQL in the Durable Object. For those workloads, use the direct API to create or transfer files in bulk, then use `exec` for the computation itself. The [Performance Model](#performance-model) section has concrete numbers.

Cloudflare currently limits each SQLite-backed Durable Object to 10 GB. Files, snapshots, trash, upload staging, job logs, application tables, and AiryFS metadata all share that database, so usable file capacity is lower. See the [Durable Objects limits](https://developers.cloudflare.com/durable-objects/platform/limits/) for the current platform limit.

## How AiryFS Compares

**Using AgentFS or a similar library directly.** A library that maps filesystem methods onto `ctx.storage.sql` gives you fast reads and writes inside the Durable Object. That is exactly what the AiryFS direct path is. A library alone, however, cannot let a process in a Container call `open`, `stat`, or `readdir` against those files. AiryFS adds the Container lifecycle, remote SQL transport, and FUSE mount that expose the same rows as `/volume`, while keeping direct access so ordinary operations never pay for the mount.

**A Container workspace synced to storage.** A Container-local disk is convenient for execution, but it makes compute the gateway to the files. Reading one file or serving one artifact requires a running Container, and persisting the workspace usually means an external volume, object store, or clone-and-sync process. That second system brings its own consistency questions about upload completion, write visibility, partial synchronization, and recovery after compute disappears. AiryFS keeps the Durable Object authoritative and treats the Container as a replaceable client. Losing the Container requires a remount, not data recovery.

**Object-storage FUSE layers.** An s3fs-, JuiceFS-, or R2-backed filesystem makes object storage another persistent system and needs a separate model for directories, links, metadata, and atomic path mutations. AiryFS uses SQLite transactions, indexes, and AgentFS's inode and dentry model inside the same Durable Object that coordinates the namespace.

**Remote development environments.** A dev environment starts with a machine and treats its disk as the workspace, so direct edge access and compute-independent persistence become secondary concerns. AiryFS starts with durable storage and attaches compute only when a command needs it.

## Quick Start

Volumes are created on first use, or explicitly with a chosen chunk size. Pick the interface that matches where your application runs. All of them operate on the same persistent volume, and the examples below all use a volume named `myproject`.

### 1. Inside The Durable Object

The `AiryFS` class lazily creates an AgentFS filesystem backed by `ctx.storage.sql`. Add methods to the class to combine coordinated direct file access with real Container execution, without copying data between them.

```typescript
async runPython(source: string): Promise<{
  exitCode: number;
  stdout: string;
  stderr: string;
  output: string;
}> {
  // Direct path. A coordinated write to Durable Object SQLite, no Container.
  await this.writeFile('/main.py', source);
  const input = await this.statPath('/main.py');
  if (input.type !== 'file') throw new Error('Expected /main.py');

  // Execution path. Mount the same tables and run a real process.
  const result = await this.exec('python3 main.py > output.txt');
  if (result.exitCode !== 0) throw new Error(result.stderr);

  // Direct path again. Read the Container's output without another exec.
  return { ...result, output: await this.readFile('/output.txt') };
}
```

This method can also be called over Workers RPC on an `AiryFS` stub. The built-in AiryFS wrappers already coordinate access and append mutation-journal entries. Custom methods that call the underlying AgentFS instance directly must use `VolumeAccessCoordinator` for overlapping content access and must record direct mutations so mounted FUSE clients invalidate stale cache entries.

The underlying AgentFS interface includes `readFile`, `writeFile`, `readdir`, `readdirPlus`, `stat`, `lstat`, `mkdir`, `rm`, `rename`, `copyFile`, `symlink`, `readlink`, `access`, `statfs`, and random-access handles through `open`; file handles provide operations such as `truncate(size)`. AiryFS adds coordinated direct primitives for timestamps, permissions, true hard links, bounded append, and subtree usage where the TypeScript AgentFS interface has no equivalent.

### 2. Web APIs

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

Workers RPC exposes the same Durable Object without an HTTP serialization layer. A Worker with a compatible namespace binding can call the public `AiryFS` methods directly.

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

The string methods are convenient for small text files. `readFileStream` and `writeFileStream` provide binary streaming, and the metadata and mutation methods expose the rest of the filesystem surface.

### 3. TypeScript SDK

The dependency-free `airyfs-sdk` package uses web-standard `fetch`, streams, `Blob`, and Web Crypto APIs. It runs in Node.js 22+, modern browsers, and Workers.

```bash
cd sdk
npm ci
npm run build
```

```typescript
import {
  AiryFSClient,
  AiryFSCommandOutcomeUnknownError,
  waitForJob,
  watchChanges,
} from 'airyfs-sdk';

const client = new AiryFSClient(
  'https://your-worker.workers.dev',
  'myproject',
  { token: process.env.AIRYFS_TOKEN },
);

await client.makeDirectory('/src');
await client.writeFile('/src/main.ts', 'console.log("hello")\n');

// exec is durable by default. Reuse the key with the same command to recover
// the existing command and replay its persisted output after a client failure.
try {
  const result = await client.exec('node src/main.ts', {
    idempotencyKey: 'build-main-v1',
  });
  console.log(result.commandId, result.stdout);
} catch (error) {
  if (error instanceof AiryFSCommandOutcomeUnknownError) {
    console.error('Inspect durable command', error.commandId);
  }
  throw error;
}

const submitted = await client.submitJob('node src/main.ts', '/');
const { job } = await waitForJob(client, submitted.id, {
  onLog(entry) {
    const bytes = Uint8Array.from(atob(entry.data), character => character.charCodeAt(0));
    console.log(new TextDecoder().decode(bytes));
  },
});

const controller = new AbortController();
await client.sql('CREATE TABLE app_notes (id INTEGER PRIMARY KEY, body TEXT)');
await client.sql('INSERT INTO app_notes(body) VALUES (?)', ['remember this']);

const current = await client.getChanges({ path: '/src', since: 'latest' });
const changes = watchChanges(client, {
  path: '/src',
  since: current.cursor,
  signal: controller.signal,
})[Symbol.asyncIterator]();
const nextChange = changes.next();
await client.writeFile('/src/observed.txt', 'watch this');
const { value: change } = await nextChange;
if (change) console.log(change.type, change.oldPath, change.path);
controller.abort();
```

`AiryFSClient` exposes files, directories, metadata, timestamps, permissions, symbolic and hard links, bounded append, subtree usage, tree archives, buffered and streaming exec, resumable upload primitives, checksums, durable jobs and logs, snapshots, change feeds, auth and capabilities, usage, diagnostics, lifecycle, KV state, and scoped application SQL. High-level helpers manage long-poll cursors, job output, exec IDs, and resumable `Blob` uploads.

### 4. CLI

The TypeScript CLI requires Node.js 22 or newer. It combines the HTTP filesystem API and Container execution behind named local sessions.

```bash
./install.sh   # builds the SDK and CLI, links `airyfs` and the short `airy` alias

printf 'print("hello from AiryFS")\n' > /tmp/airyfs-main.py

airy session create work \
  --endpoint https://your-worker.workers.dev \
  --volume myproject
airy volume create --chunk-size 256k
airy volume list

airy mkdir -p /src
airy upload /tmp/airyfs-main.py /src/main.py
airy upload -r ./project /project --replace
airy cd /src
airy cat main.py

airy warm
airy exec --idempotency-key build-main-v1 python3 main.py
airy exec --no-wait python3 main.py  # explicit transient, fail-fast execution
airy job submit --wait python3 main.py
airy job list --status unknown
airy job status "$COMMAND_ID"
airy job logs --follow "$COMMAND_ID"
airy snapshot create before-refactor --note "known good"
airy sql 'SELECT body FROM app_notes WHERE id = ?' --arg 1
airy watch /src
airy status
airy shell
```

A session stores an endpoint, volume, and remote working directory under `~/.airyfs`. `AIRYFS_SESSION` and `--session` let separate terminals or scripts select different sessions. A separate registry Durable Object records volume names on first use, because Durable Object namespaces cannot enumerate the names used to derive object IDs.

CLI `exec` is durable by default. It persists one command ID before execution, retries transient submission and polling failures without changing the idempotency key, replays paginated output from durable logs, and never automatically replays an admitted command whose outcome is ambiguous. `--idempotency-key` lets a caller recover the same command explicitly; reusing a key with a different command or working directory returns `409 IDEMPOTENCY_CONFLICT`. `--no-wait` selects the lower-level transient route and fails immediately on `EXEC_BUSY`. `--container` disables direct read-only fast paths, while `--no-stream` buffers the durable result locally. `--timeout` applies only to Container startup for `--pty`; it does not cancel an admitted durable command. `SIGINT` cancels a foreground durable `exec`; interrupting `job submit --wait` only stops the local wait and leaves the submitted job running. HTML gateway failures are normalized into concise CLI errors instead of printed as markup.

For development without linking the binaries, run `npm run dev -- <arguments>` from `cli/`. A manual installation uses `npm install`, `npm run build`, and `npm link` in that directory.

#### Sessions

A named session keeps an endpoint, volume, bearer token, and current remote directory together in `~/.airyfs/config.json`; `AIRYFS_HOME` selects another local state directory. Session selection resolves in this order: global `--session`, `AIRYFS_SESSION`, then the persisted current session. There is no implicit default. Creating a session selects it, while deleting one removes only local state and never deletes its remote volume.

```bash
airy session create int --endpoint https://airyfs-int.example.com --volume scratch
airy session create prod --endpoint https://airyfs.example.com --volume project
airy session list
airy session use prod
airy session edit prod --volume another-project
airy session rename prod production
airy session delete production

# Portable session blobs include the bearer token. Treat them as credentials.
airy session export prod
airy session import airyfs1:... home
```

In a TTY, `session create` prompts for omitted values. Scripts must provide the name, endpoint, and volume. Separate terminals can stay pinned independently with commands such as `AIRYFS_SESSION=int airy shell` and `AIRYFS_SESSION=prod airy status`.

#### CLI Command Surface

Remote paths use POSIX semantics and resolve relative to the active session directory. `cd` validates a target before persisting it. Direct filesystem commands do not start the Container.

| Area | Commands |
|---|---|
| Navigation | `pwd`, `cd`, `ls`, `tree` |
| File inspection | `cat`, `head`, `tail`, `stat`, `lstat`, `file`, `readlink`, `du`, `checksum` |
| File mutation | `write`, `append`, `mkdir`, `rmdir`, `rm`, `mv`, `cp`, `ln`, `truncate`, `touch`, `chmod` |
| Transfer | `upload`, `download`, `put`, `get`, `push`, `pull` with resumable file and transactional tree modes |
| Recovery | `trash list`, `trash restore`, `trash purge`, `undo`, and snapshot create/list/diff/restore/clone/delete |
| Execution | `warm`, durable `exec`, interactive `exec --pty`, and `job submit/list/status/logs/cancel` |
| Automation | `schedule create/list/enable/disable/delete`, `watch`, and webhook create/list/delete |
| Services | `service create/list/start/stop/delete/logs` |
| Search and data | `find`, `glob`, `grep`, `sql`, and `kv set/get` |
| Publishing | `site publish/deploy/rollback/status/unpublish`, `share`, `asset`, and `browser-upload` |
| Volume operations | `volume create/info/list/fork/quota`, `usage`, `usage-history`, `metrics`, `perf`, `db-info`, `status`, and `destroy` |
| Authentication | `auth status/login/logout/passwd` and capability creation/revocation |

`cat` and `head` write raw bytes and cannot be combined with `--json` or `--quiet`; use `get` for binary files that should not go to the terminal. `upload` and `download` inspect the source and handle either one file or, with `--recursive`, a directory archive. The lower-level `put`/`get` and `push`/`pull` commands remain available.

CLI-specific `exec` options must precede the remote command. Arguments after the first command word pass through unchanged. A shell expression supplied as one argument is sent as written:

```bash
airy exec --idempotency-key tool-v1 tool --json --output result.json
airy exec 'find . -type f | sort'
```

#### Interactive Shell And Global Output

`airy shell` accepts the same commands as one-shot invocation and supports quoting, backslash escaping, history, remote-path and session completion, `help`, `clear`, `exit`, and `quit`. History is stored in `~/.airyfs/history`. It can start without an active session so session administration remains available. Commands that need to own stdin, including `write`, valueless `kv set`, interactive `destroy`, and `exec --pty`, are unavailable inside the shell.

Global options precede the command: `--session <name>` selects a session for one invocation, `--json` emits structured output where supported, `--no-color` disables ANSI styling, and `--quiet` suppresses non-error output.

```bash
airy --session int --json ls
```

## What You Can Do

- **Store and organize application files:** give each user, project, repository, or task an isolated cloud filesystem with directories, links, metadata, quotas, and transactional mutations.
- **Use files from any runtime:** access the same volume directly inside its Durable Object, over HTTP or Workers RPC, through the TypeScript SDK, or from the `airyfs` CLI.
- **Move and inspect data:** stream individual files, transfer directory trees, resume large uploads and downloads, search names and content, follow changing files, and query scoped application tables.
- **Publish and share content:** host static sites, deploy atomically with rollback, serve immutable assets, accept capability-scoped browser uploads, or mint expiring download links without starting compute.
- **Integrate existing tools:** mount a volume through WebDAV, use S3-compatible clients, or expose it to normal Linux programs at `/volume` through an on-demand Container.
- **Automate filesystem workflows:** react to ordered change feeds and webhooks, run durable or scheduled jobs, supervise preview services, and preserve intermediate files across retries and Container replacement.
- **Recover and experiment safely:** use trash and undo, snapshots, diffs, cross-volume clones, and point-in-time forks of live volumes for rollback or isolated work.
- **Operate volumes:** list registered volumes, inspect health plus durable-job and preview-service logs, export Prometheus metrics, and retain bounded filesystem, quota, and SQLite usage history.

## Capabilities

| Area | Capability |
|---|---|
| Persistent storage | Files, directories, links, POSIX metadata, and file chunks in Durable Object SQLite |
| Direct file access | Binary-safe streaming reads and writes without starting the Container |
| HTTP semantics | `GET`, `HEAD`, single byte ranges, content length, last-modified time, inode headers, and structured errors |
| File mutations | Atomic replacement, append, delete, copy, rename, truncate, touch, chmod, and true hard links |
| Directory operations | Create, list with metadata, remove, recursive remove, tree views, and logical usage |
| Authentication | Optional root bearer auth, signed expiring capabilities scoped by volume, operation, and path, and per-volume passwords that mint scoped tokens without the root secret; `admin` capabilities are always volume-wide |
| Web hosting | Opt-in static-site serving, atomic deployment with rollback, immutable assets, browser uploads, and expiring file shares |
| Bulk transfer | Transactional streaming directory push/pull plus resumable, checksummed large-file transfer |
| Recovery | Trash, restore, undo, snapshots, diffs, clones, and live volume forks |
| Automation | Ordered change feeds, path-filtered webhooks, UTC cron schedules, durable jobs, and preview services |
| Search | Server-side filename FTS, glob matching, content grep, tree views, and directory usage |
| Interoperability | WebDAV mounting and path-style S3-compatible access to each volume |
| Workers RPC | Streams, metadata, mutations, trees, uploads, snapshots, jobs, changes, usage, lifecycle, and exec |
| TypeScript SDK | Typed HTTP client plus change-watch, job-follow, exec-id, and resumable `Blob` helpers |
| CLI | Sessions, remote cwd, familiar file commands, transfers, snapshots, jobs, auth, hosting, diagnostics, JSON output, and an interactive shell |
| Container execution | Shell commands, live output, cancellation, PTY sessions, and standard Linux tooling at `/volume` |
| Concurrency | Path-scoped direct locks, a volume-wide FUSE mutation lock, change triggers, and journal-driven cache invalidation |
| Observability | Prometheus exposition plus bounded filesystem, quota, and SQLite usage history |
| Application SQL | Scoped single-statement SQLite over user-owned `app_*` tables and indexes |

## Architecture

Each volume is one instance of the `AiryFS` Durable Object class and one attached Container instance.

### Direct Path

The direct path calls the AgentFS TypeScript Cloudflare adapter with the Durable Object's storage context. Its asynchronous filesystem methods execute SQL against `ctx.storage.sql` without crossing a network boundary or starting the Container.

Streaming writes use a temporary AgentFS path and rename it over the destination only after the request body completes. Streaming reads fetch file chunks incrementally and support a single HTTP byte range.

### Execution Path

The first `exec` performs four steps.

1. Start the attached Container and wait for its command server.
2. Start the in-process HTTP-to-TCP bridge inside the Container.
3. Open data and invalidation TCP connections from the Durable Object and start a Hrana server on each. Hrana is libSQL's remote SQL protocol, and AiryFS implements the subset AgentFS needs.
4. Start the AgentFS FUSE daemon and wait until `/volume` is mounted.

```text
                Attached Container
  command execution in /volume (FUSE mount)
                     |
          AgentFS libSQL client
    HTTP :8080 (data)   HTTP :8081 (invalidation)
                     |
            HTTP-to-TCP bridge
    TCP :9000 (data)    TCP :9001 (invalidation)
                     |
             framed TCP frames
                     |
        Durable Object Hrana servers
                     |
            ctx.storage.sql
   (the same tables the direct path uses)
```

Inside the Container, AgentFS uses `libsql::Builder::new_remote("http://localhost:8080", "")`. A filesystem operation becomes a Hrana HTTP request to the bridge. The bridge forwards a framed request over TCP to the Durable Object, which executes the SQL against `ctx.storage.sql` and returns the result along the same path. The second connection carries mutation-journal polling so direct-path changes can invalidate FUSE kernel caches without competing with ordinary filesystem SQL.

The bridge rejects pending requests when a connection drops and applies a response timeout. On replacement, new requests switch to the new connection generation while already-dispatched work drains on the retired socket. Startup is single-flight, bounded, and generation-safe. Buffered execution probes a dedicated Container control endpoint; three failed probes quarantine and recycle that runtime. Streaming execution requires heartbeat or output bytes every 15 seconds. Runtime generations prevent a stale failure from destroying a replacement, and three infrastructure failures within two minutes open a 30-second circuit followed by one bounded half-open recovery attempt. Container recycling never touches Durable Object SQLite.

### Persistence And Lifecycle

The Container image, process state, and files outside `/volume` are ephemeral. `/volume` is reconstructed from Durable Object SQLite whenever AgentFS remounts it.

`POST /destroy?volume=V` destroys only the Container. It does not delete the Durable Object or its SQLite data. Direct access continues to work, and a later `exec` starts a new Container and mounts the existing volume. Lifecycle cleanup closes both bridge channels and uses ownership tokens so an older request cannot clear a newer command's state.

The Durable Object requests automatic Container sleep after 30 minutes of inactivity. A direct filesystem request does not wake the Container. Once the Container sleeps, its compute charges stop. The next `exec` starts a new session and remounts the persistent volume.

## Relationship To AgentFS

[AgentFS](https://github.com/tursodatabase/agentfs) is the embedded filesystem implementation used by AiryFS. It defines the SQLite inode, dentry, file-chunk, symlink, overlay, key-value, and tool-call semantics. The vendored TypeScript Cloudflare adapter runs in the Durable Object against `ctx.storage.sql`. The AgentFS Rust SDK and CLI run inside the Container and expose the same database through FUSE.

AiryFS is not a replacement for AgentFS. It is the Cloudflare Durable Object and Container architecture around it. Named-volume routing, the HTTP and Workers RPC surfaces, the TypeScript SDK, the CLI, streaming and range handling, mutation coordination, schema migration, Container lifecycle management, the remote SQL bridge, and the on-demand mount are all AiryFS.

### Why AiryFS Patches AgentFS

AiryFS vendors pristine AgentFS v0.6.4 at commit `3a5ed2b88e5d5a5f9b2c7fe02d012b50fd19e3c0` and applies an ordered patch series. The patches add the direct remote libSQL connection, bounded FUSE caching, cross-runtime cache invalidation, and lease-aware open-inode behavior needed when Durable Object code and remote FUSE clients mutate one filesystem. AiryFS prefers changes in its own Worker, bridge, and Container layers whenever AgentFS does not need to change, so the patch series is the explicit compatibility surface that remains.

`agentfs/build.sh` materializes a fresh tree, verifies and applies each patch in order, runs the TypeScript and Rust test suites, and builds the Linux CLI used by the Container. See [`docs/AGENTFS_PATCHES.md`](docs/AGENTFS_PATCHES.md) for the complete patch inventory, the upstream refresh procedure, and maintenance rules.

The resulting mount connects only to loopback bridges inside the attached Container.

```bash
agentfs mount \
  --remote-url http://localhost:8080 \
  --invalidation-url http://localhost:8081 \
  --auth-token "" \
  --cache-ttl-ms 1000 \
  --foreground \
  volume /volume
```

The empty token is intentional. The bridge listeners are internal to the Container, and external authentication is enforced at the Worker API boundary when deployment authentication is configured. No SQLite database file is created in the Container for the mounted volume.

## Protocol And Data Path

This protocol exists solely so the AgentFS process inside the Container can operate on SQLite owned by the Durable Object. The direct path never touches it.

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

The active data-channel `HranaServer` records pipeline and statement counts for `/usage` and `/perf`. The invalidation channel's polling statements are not included. These are in-memory counters for the current data session, not cumulative billing or lifetime metrics, and they reset when the connection or Durable Object restarts.

`/v1/volumes/V/metrics` exposes the same per-volume state in Prometheus text format, including filesystem and quota gauges, physical SQLite size, Container/FUSE/Hrana health, current-session Hrana counts, and bounded `table` row-count labels. Scrape snapshots are cached for five seconds to limit repeated row-count work. Metrics do not fan out through the deployment registry or add writes to filesystem hot paths.

### Framing, Ordering, And Failure Handling

TCP does not preserve application message boundaries. `FrameBuffer` handles partial headers, partial payloads, and multiple frames delivered in one chunk. Each bridge channel admits at most 16 requests, assigns local request IDs, serializes socket writes with backpressure, and resolves the bounded Hrana pipeline in FIFO order. Request IDs are returned in `X-AiryFS-Request-ID` and never alter the Hrana wire payload.

Every bridge request has a 30-second response deadline and an 8 MiB frame limit. Queued work is removed when its HTTP client disconnects. An active canceled request is drained and discarded before later responses are resolved, preserving FIFO alignment. A write failure, timeout, oversized response, socket error, socket end, or socket close invalidates that connection generation, clears buffered bytes, and rejects every pending HTTP request. When the Durable Object reconnects, the retired generation remains isolated until its admitted requests drain or time out.

The bridge returns `503` with `Retry-After` when the Durable Object TCP connection is absent or admission is full. Pipeline transport failures become `502` responses to the libSQL client. A volume permits one active Container command. Durable SDK and CLI commands wait in the job queue; transient HTTP execution and CLI `--no-wait` receive `503 EXEC_BUSY` when another command already holds the slot.

### Operational Diagnostics And Runtime Failure Boundary

Container health reports whether the data bridge has a Durable Object TCP connection, aggregate pending, queued, and admitted requests across active and retired generations, and Node.js process memory and resource usage. A bridge failure with admitted work emits `bridge_connection_failed`. Worker responses at status 500 or above emit `request_failed` with a bounded route label, edge request ID, status, error code, and Hrana session identity. User-controlled paths, command bodies, raw error messages, and SQL text are not logged.

A July 2026 integration investigation reproduced intermittent buffered-exec hangs under sustained filesystem activity on both `lite` and `basic` Container instances. Immediately before a hang, the bridge remained connected with no pending, queued, or admitted work. During the hang, independent probes to the bridge and command-server ports both failed with `Error proxying request to container: The operation was aborted due to timeout`, while the Durable Object remained responsive and its Hrana counters stopped advancing with no active operation or filesystem lock holder. This localizes that failure mode to an unresponsive Container process, VM, or network proxy below edge routing and the Durable Object SQL/Hrana server. A mounted-FUSE check or Hrana probe before admission cannot prevent a runtime from becoming unresponsive afterward; heartbeats, generation quarantine, durable outcomes, and the circuit breaker bound its impact instead.

## API

### Resource-Oriented HTTP API

| Method | Path | Behavior |
|---|---|---|
| `GET` | `/v1/volumes?cursor=C&limit=100` | List registered volumes by name; root access required when auth is enabled |
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
| `POST` | `/v1/volumes/V/operations/lstat` | Return no-follow metadata for `{"path":"/a"}` |
| `POST` | `/v1/volumes/V/operations/touch` | Create a file or update timestamps; optional numeric `atime` and `mtime` |
| `POST` | `/v1/volumes/V/operations/chmod` | Replace permission bits with numeric `{"path":"/a","mode":416}` |
| `POST` | `/v1/volumes/V/operations/link` | Create a true hard link `{"existing":"/a","path":"/b"}` |
| `POST` | `/v1/volumes/V/operations/append` | Append at most 1 MiB from canonical base64 `data` |
| `POST` | `/v1/volumes/V/operations/du` | Return logical bytes and distinct reachable inodes below `path` |
| `GET` | `/v1/volumes/V/trees/path` | Stream a directory as an AiryFS archive |
| `PUT` | `/v1/volumes/V/trees/path?replace=true` | Transactionally import an AiryFS archive |
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
| `POST` | `/v1/volumes/V/sql` | Execute one statement against user-owned `app_*` tables or indexes |
| S3 methods | `/s3/V[/key]` | Path-style S3 bucket and object operations with SigV4 |
| `GET` | `/v1/volumes/V/jobs?status=running` | List durable jobs, optionally by status |
| `POST` | `/v1/volumes/V/jobs` | Submit a durable job using `Idempotency-Key`; a changed command or cwd for an existing key returns `409` |
| `GET` | `/v1/volumes/V/jobs/ID` | Return durable job state and terminal result |
| `GET` | `/v1/volumes/V/jobs/ID/logs?after=N` | Page persisted binary-safe stdout/stderr |
| `POST` | `/v1/volumes/V/jobs/ID/cancel` | Cancel queued or running work |
| `GET` | `/v1/volumes/V/services/NAME/logs?after=N` | Read ephemeral, binary-safe preview service stdout/stderr |
| `GET` | `/v1/volumes/V/schedules` | List UTC cron schedules |
| `POST` | `/v1/volumes/V/schedules` | Create an enabled schedule `{"name","cron","command","cwd"}` |
| `POST` | `/v1/volumes/V/schedules/ID/enable` | Enable and recalculate the next run |
| `POST` | `/v1/volumes/V/schedules/ID/disable` | Disable a schedule |
| `DELETE` | `/v1/volumes/V/schedules/ID` | Delete a schedule |
| `GET` | `/v1/volumes/V/changes/path?since=N&wait=25000` | Read or long-poll ordered filesystem changes |
| `GET` | `/v1/volumes/V/webhooks` | List change-feed webhook subscriptions without signing secrets |
| `POST` | `/v1/volumes/V/webhooks` | Create a durable signed webhook; `url` is required, while `pathPrefix` and `events` are optional |
| `DELETE` | `/v1/volumes/V/webhooks/ID` | Delete a webhook and its pending deliveries |
| `POST` | `/v1/volumes/V/search` | Bounded server-side `find`, glob, or grep under a path prefix |
| `GET` | `/v1/volumes/V/tree/P` | Bounded structured directory tree; accepts `depth` and `limit` |
| `GET` | `/v1/volumes/V/quota` | Read logical-byte and inode limits |
| `PUT` | `/v1/volumes/V/quota` | Configure logical-byte and inode limits |
| `GET` | `/v1/volumes/V/auth` | Report whether deployment auth is enabled and a volume password is set |
| `POST` | `/v1/volumes/V/auth/password` | Set or rotate the volume password (root, admin, or current password) |
| `POST` | `/v1/volumes/V/auth/login` | Exchange the volume password for a scoped capability token |
| `GET` | `/v1/volumes/V/capabilities` | Return auth mode and caller identity |
| `POST` | `/v1/volumes/V/capabilities` | Mint a scoped capability using `admin` access |
| `DELETE` | `/v1/volumes/V/capabilities/ID` | Revoke a capability using `admin` access |
| `GET` | `/v1/volumes/V/sites` | Report the published-site status |
| `PUT` | `/v1/volumes/V/sites` | Publish or update the public web root `{"path","indexDocument","spa","directoryListing","cacheControl"}` |
| `DELETE` | `/v1/volumes/V/sites` | Unpublish the site |
| `GET` | `/v1/volumes/V/shares` | List share links |
| `POST` | `/v1/volumes/V/shares` | Create a share link `{"path","expiresInSeconds","cacheControl"}` |
| `DELETE` | `/v1/volumes/V/shares/ID` | Delete a share link |
| `GET` | `/s/V/path` | Public, unauthenticated static-site serving with MIME, index, and SPA fallback |
| `GET` | `/d/V/ID` | Public, unauthenticated share-link download |
| `GET` | `/v1/volumes/V/usage` | Return filesystem, SQLite, Container, and Hrana usage |
| `GET` | `/v1/volumes/V/usage-history` | Return newest-first five-minute usage samples with `before`/`limit` pagination |
| `GET` | `/v1/volumes/V/metrics` | Return per-volume Prometheus text exposition; read access required |

Filesystem failures return structured JSON with stable POSIX-style codes and appropriate HTTP statuses.

Direct `lstat`, `touch`, and `chmod` do not traverse symbolic links. `lstat` reports the link inode; `touch` and `chmod` reject a link operand so scoped capabilities cannot escape through a link target.

Volumes default to 256 KiB chunks. Explicit chunk sizes must be powers of two from 4 KiB through 1 MiB. Existing volumes retain their stored chunk size, and a conflicting size returns `409 CHUNK_SIZE_IMMUTABLE` after filesystem data exists. Any filesystem request implicitly creates an unconfigured volume with the default.

The volume registry is not on the filesystem data path. Each volume publishes its name and chunk size once, then records that registration in its own SQLite database. Ordinary file, S3, site, and execution requests continue routing directly to the volume Durable Object. Existing deployments populate the new registry lazily as volumes are used.

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

Server-side search does not start the Container. `airy find /src --name config` uses a transactionally maintained FTS5 trigram index for basename substring lookup, including files written through FUSE. Patterns shorter than three characters fall back to a bounded recursive scan because the trigram index cannot serve them. `airy glob '**/*.test.ts' /src` and `airy grep needle /src --ignore-case` traverse AgentFS directly. Grep skips binary files and files over 10 MiB, scans at most 100 MiB per request, and returns line/column metadata. Traversal modes cap work at 100,000 entries; every mode caps results at 1,000.

`airy tree /src --depth 3` renders a structured server-side walk without starting the Container. The API returns path, depth, type, and logical size for up to 100,000 entries, with explicit truncation metadata.

`airy volume fork working-copy` streams a point-in-time-consistent copy of the live filesystem into an empty target volume. The fork preserves the source chunk size, refuses to overwrite existing target files, and becomes fully independent after creation. Cross-volume forks require root authentication or an auth-disabled deployment.

Each volume is also available as a path-style S3 bucket at `/s3/<volume>`. The compatibility surface supports `HeadBucket`, `GetBucketLocation`, `ListObjectsV2`, and single-object `HeadObject`, `GetObject` (including ranges), `PutObject`, and `DeleteObject`. Object keys map directly to unambiguous filesystem paths; trailing slashes, empty segments, and `.`/`..` segments are rejected. `PutObject` creates missing parent directories. Listings are bounded to 100,000 filesystem entries. Multipart uploads, object metadata, ACLs, versioning, and batch deletion are not currently implemented.

Authenticated deployments use SigV4 with access key ID `airyfs`, service `s3`, and `AIRYFS_AUTH_SECRET` as the secret access key. This is the deployment-wide root credential: capabilities and volume passwords cannot authenticate S3, and one valid S3 credential can access every volume. Configure clients with region `auto`, the deployment URL plus `/s3` as their endpoint, and path-style addressing. The server accepts another region when the client signs consistently with it. Auth-disabled local/test deployments may send unsigned requests. Presigned query authentication and `STREAMING-*` payload signatures are not supported.

```sh
AWS_ACCESS_KEY_ID=airyfs \
AWS_SECRET_ACCESS_KEY="$AIRYFS_AUTH_SECRET" \
aws --endpoint-url https://airyfs.example.workers.dev/s3 --region auto s3 ls s3://my-volume
```

Scoped application SQL runs in the volume Durable Object without starting the Container. Each request accepts `{ "sql": "...", "args": [...] }`, where arguments are strings, numbers, `null`, or `{ "base64": "..." }` blobs. Statements may create and access only tables and indexes whose names begin with `app_`; AiryFS, AgentFS, SQLite system objects, PRAGMAs, views, triggers, attached databases, CTEs, and multiple statements are rejected. Results return at most 1,000 rows and require a capability granting the dedicated `sql` operation (or `admin`).

`airy volume quota --bytes 10g --inodes 100000` configures persistent logical-byte and inode limits. Use `unlimited` to clear either limit. SQLite triggers enforce quotas for direct HTTP writes and Container/FUSE writes at the shared filesystem boundary; rejected HTTP writes return `507 ENOSPC`. `airy usage` reports logical usage, configured limits, remaining capacity, physical SQLite size, and Container/FUSE health.

`airy tail /logs/app.log` prints the last ten lines; `--bytes` selects a byte window and `--follow` streams appends. Follow mode composes range reads with the filesystem change feed, so it does not hold a Worker request or start the Container. `--retry` waits for a removed or rotated path to reappear.

Direct API and CLI deletes move paths into durable per-volume trash by default. `airy trash list`, `airy trash restore ID`, and `airy undo` recover deleted files, directory subtrees, and symlinks; `airy rm --permanent` and `airy trash purge ID` reclaim space immediately. Trashed content continues to count against quota until purged. Deletes performed inside the Container through FUSE remain permanent because that path exposes only opaque filesystem SQL; take a snapshot before destructive `exec` operations when recovery is required.

Volumes are mountable over WebDAV at `/dav/<volume>/`. The dependency-free adapter supports `OPTIONS`, finite `PROPFIND`, `GET`, `HEAD`, streaming `PUT`, `MKCOL`, recoverable `DELETE`, same-volume `MOVE` and bounded recursive `COPY`, no-op `PROPPATCH`, and Finder-compatible `LOCK`/`UNLOCK`. It advertises WebDAV classes 1 and 2 for client compatibility, but lock tokens are advisory shims: they are not persisted or enforced on mutations and must not be used for concurrency control. The adapter supports HTTP ranges and validators, hides internal trash, and enforces bearer capability scopes. When authentication is enabled, WebDAV also accepts Basic authentication with the root credential, a capability token, or the volume password.

`airy exec --pty <command>` runs interactive terminal applications against the mounted volume. The CLI obtains a 30-second single-use ticket, upgrades to a binary WebSocket, forwards raw terminal input and resize events, and restores local terminal mode on every exit path. PTY sessions share the volume's single execution slot with buffered, streaming, and durable commands.

Preview services persist a command definition in Durable Object SQLite while the process remains disposable Container compute. `airy service create web --public -- node server.js` allocates `$PORT` from 5000–5015, starts independently of foreground exec/PTY work, and publishes at `/p/<volume>/web/`. Enabled services restart lazily after Container sleep or replacement when the next proxy request arrives. Commands must listen on `$PORT`; public exposure is opt-in.

`airy service logs web` reads the Container's bounded stdout/stderr buffer; `--follow` polls until interrupted and `--after` resumes from a sequence cursor. Follow mode detects process generations and reports restarts or ring-buffer gaps instead of silently losing output. Service logs are ephemeral and disappear when the Container is replaced. Reading logs requires `exec` access.

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
- `lstatPath`, `touchPath`, `chmodPath`, `linkPath`, `appendFile`, and `diskUsage`
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

## Execution Contract

The SDK and CLI run `exec` as a durable command by default. They persist the command with one idempotency key before scheduling, stream or reconstruct output from durable logs, and expose the job ID as the command ID. The lower-level HTTP `/exec` route remains available for transient immediate-admission execution and powers `--no-wait`. Commands run with:

- `cwd=/volume`
- `HOME=/root`
- A standard system `PATH`
- A 300-second process timeout, bounded by a 310-second Worker-side Container request deadline
- A 10 MiB transient buffered-exec response limit; durable execution retains up to 50 MiB in paginated logs

The SDK response contains `commandId`, `exitCode`, `stdout`, `stderr`, and `outputTruncated` when the durable 50 MiB log limit was reached. Reusing an `idempotencyKey` with the same command and working directory returns the existing command instead of executing it twice; changing either field returns `409 IDEMPOTENCY_CONFLICT`. Submission and status/log polling retry transient transport failures plus HTTP 502, 503, and 504. Polling failure does not cancel a still-running command. A dead FUSE daemon produces a structured `503` instead of running the command in an unmounted directory. Startup has a separate 60-second bound.

Streaming exec emits a durable command ID, base64 stdout/stderr chunks, and one terminal exit event. The Container emits one-second heartbeats; the Worker quarantines and recycles a runtime after 15 seconds without heartbeat or output. Cancellation sends `SIGTERM` to the process group and escalates to `SIGKILL`. Interactive and durable commands share one execution slot.

Durable jobs persist before scheduling and require an `Idempotency-Key` over HTTP. The queue claims one job at a time, persists binary stdout/stderr as ordered BLOB rows, caps retained output at 50 MiB, and supports queued/running cancellation. Clients drain log pages incrementally and perform one final log read after observing terminal state so output committed concurrently with completion is not missed. A command whose admitted outcome cannot be proven enters the terminal `unknown` state and is never automatically replayed. Retrying the same idempotency key returns the existing command rather than duplicating execution.

The CLI recognizes exact, read-only `exec` argv for `cat`, `ls`, `pwd`, and `readlink` with safe relative paths and serves them directly from the Durable Object without starting a Container. `exec --container` disables this fast path. Shell syntax, absolute/traversing paths, unrecognized options, mutations, and arbitrary programs always use Container execution.

The change feed uses SQLite triggers on AgentFS inode and dentry tables, so it observes both direct API mutations and writes originating through Container/FUSE. Per-volume sequence numbers order create, modify, remove, and rename events. The latest 10,000 sequence values are retained; clients receive `gap: true` when their cursor predates that window and should resynchronize before continuing.

## Usage And Health

`GET /v1/volumes/V/usage` and `GET /usage?volume=V` return:

- AgentFS logical filesystem statistics from `statfs`
- Physical Durable Object SQLite database size in bytes
- Container SDK state and Hrana connection state
- Bridge startup state
- FUSE mount and daemon-exit state
- Container working directory
- Hrana pipeline and SQL statement counters

`GET /db-info?volume=V` returns the row count for every AiryFS and AgentFS schema table. `GET /perf?volume=V` returns Hrana session counters, active-operation and lock state, the runtime generation, and exec-circuit state. The Container's internal `/health` endpoint also reports bridge queue state and process resource usage to the Durable Object lifecycle manager.

Usage reads update at most one `fs_usage_sample` row per five-minute bucket. `GET /v1/volumes/V/usage-history` returns newest-first samples of filesystem bytes, inode count, SQLite size, and configured quotas. AiryFS retains the latest 2,016 samples, equivalent to seven days at continuous five-minute observation; sparse observations can span longer. Sampling is demand-driven: it does not create perpetual Durable Object alarms, wake idle volumes, run on filesystem mutations, or make Prometheus scrapes write state. Requests with a `before` cursor only read stored samples.

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
- `fs_usage_sample` stores the latest 2,016 demand-driven usage observations.
- `capability_revocations` stores revoked signed capability IDs.
- `kv_store` stores application key-value records.
- `tool_calls` stores AgentFS tool-call records.

Schema initialization is idempotent. The initializer recreates missing tables after an interrupted setup and runs supported migrations inside `transactionSync`. Current migrations add missing v0.2 and v0.4 inode columns, replace the older whiteout layout, rebuild older tool-call tables with status and nullable completion fields, and add the site directory-listing setting. Arbitrary malformed or independently modified schemas are not repaired automatically.

## Performance Model

The two paths have different cost profiles by design.

| Operation | Expected behavior |
|---|---|
| Direct read, write, or listing | Runs in the Durable Object without a Container |
| First `exec` | Starts the Container, bridge, TCP session, and FUSE mount |
| Warm `exec` | Reuses the running Container and mounted volume, then reattaches the request-scoped Hrana data channel |
| FUSE file operation | Adds a Container-to-DO round trip and SQL execution |

Development measurements have typically shown direct file operations below 100 ms, a warm `exec` in roughly 2-10 seconds, and a cold mount around 30 seconds. Metadata-heavy programs can be much slower because every FUSE operation crosses the Container-to-Durable Object boundary; an integration Git commit has taken more than two minutes while a subsequent clean `git status` took about 21 seconds. Deployment location, Container state, command behavior, and syscall count all affect these numbers.

For best results:

- Use direct writes for file ingestion, generated source trees, and bulk updates.
- Use direct reads for inspection, API responses, and artifact retrieval.
- Use `exec` for computation that benefits from existing Linux tools.
- Avoid generating large metadata-heavy trees one syscall at a time through FUSE when the direct API can create them more efficiently.

## Billing Model

AiryFS does not make Durable Object usage free. Cloudflare bills SQLite-backed Durable Objects for requests, active compute duration, rows read and written, and stored SQL data. An inactive Durable Object that is eligible for hibernation does not incur duration charges, but its stored data remains billable. After execution starts the Container, AiryFS keeps outbound TCP bridge connections open through the warm-idle window until the Container sleeps, is destroyed, or disconnects. Current Cloudflare behavior lets each active outbound connection prevent Durable Object eviction for at most 15 minutes; after that, the normal 70-140 second inactivity window resumes even if the connection remains open. The object continues duration billing while the connection keeps it alive. See Cloudflare's [Durable Objects lifecycle](https://developers.cloudflare.com/durable-objects/concepts/durable-object-lifecycle/) and [pricing](https://developers.cloudflare.com/durable-objects/platform/pricing/) documentation for current behavior and rates.

Containers are billed separately for provisioned memory and disk while running and for active CPU usage. Charges start when the Container is requested or manually started and stop after it sleeps. AiryFS therefore scales Container compute to zero after the 30-minute idle threshold while leaving the self-contained Durable Object volume available to direct APIs. See Cloudflare's [Containers pricing](https://developers.cloudflare.com/containers/pricing/) for current rates and included usage.

AiryFS does not currently expose an API that deletes an entire volume Durable Object and reclaims all of its storage. `destroy` removes only the disposable Container. Operators must account for retained volume storage until a full deletion mechanism is added.

## Consistency And Current Limits

AiryFS coordinates direct access and FUSE mutations, but it does not yet implement every POSIX or transactional guarantee.

- File-content reads, streaming reads, stat, directory listing, Workers RPC `readSymlink`, and direct mutations use fair path-scoped locks. The HTTP `operations/readlink` endpoint does not currently hold a read lock and may interleave with mutations.
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

Root callers and `admin` capabilities can mint expiring capabilities restricted to one volume, a subset of `read`, `write`, `exec`, `sql`, and `admin`, and normalized path prefixes. An `admin` grant is always volume-wide and ignores path prefixes. Every capability request verifies its signature, expiry, volume, operation, path scope, and revocation state. Revocation blocks subsequent requests but does not terminate work already admitted. Cross-volume snapshot clone remains root-only because a capability is bound to one source volume.

Each volume can also carry its own password, stored in the volume's SQLite as a PBKDF2 verifier (never as plaintext). `POST /v1/volumes/V/auth/password` sets or rotates it (authorized by the root credential, an `admin` capability, or the current password), and `POST /v1/volumes/V/auth/login` exchanges the password for a volume-scoped `read,write,exec` capability without needing the root secret. This lets a volume be secured at creation time and accessed from multiple machines. `GET /v1/volumes/V/auth` reports whether auth is enabled and a password is set. Password auth requires `AIRYFS_AUTH_SECRET` to be configured, because the minted token is signed with the derived per-volume key.

The login endpoint has no built-in attempt throttling or account lockout. Use strong volume passwords and apply external rate limiting or WAF policy to internet-exposed deployments.

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

`airy deploy` runs `scripts/provision.mjs`, which deploys the Worker, generates and stores `AIRYFS_AUTH_SECRET`, discovers the workers.dev URL, and creates a local session pointing at it (holding the root credential). The Worker is published before the secret is installed, so a newly reachable deployment has a brief unauthenticated interval. Each invocation also generates a fresh root secret, invalidating existing root credentials, capabilities, and S3 credentials; only the selected local session is updated. Provisioning output currently includes the generated credential, so treat captured command output as sensitive. `airy init` additionally creates a volume, sets its password, and downgrades the session to a scoped token. Both must be run from within the repository because the deploy builds the Worker and Container from source.

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

The Worker suite covers Hrana framing and execution, transport bounds, schema migration, locking, binary/range streaming, archives and transactional tree import, authentication and capability scope, snapshots, resumable uploads and checksums, streaming exec ownership, durable jobs, trigger-driven change feeds, open-handle leases, chunk-size boundaries, and POSIX-style HTTP errors. `npm run test:deploy` covers account selection, fixed environment targeting, production guards, and Docker configuration sanitization.

### Container Build

```bash
cd container
npm run build
npm test
```

The Container suite covers bounded bridge admission, FIFO response handling, cancellation, connection-generation replacement, request limits, streaming exec events, process-group termination, disconnect cleanup, and buffered/streaming slot coordination.

### CLI Tests

```bash
cd cli
npm run typecheck
npm test
npm run build
```

The CLI suite uses isolated temporary configuration and local mock servers. It covers session concurrency and auth migration, streaming and resumable files, transactional tree transfer, snapshots, durable idempotent submission, paginated output polling, `unknown` outcomes, explicit transient execution, change watching, concise gateway errors, shell behavior, and completion. It does not access `~/.airyfs` or a deployed endpoint.

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
AIRYFS_URL=https://your-worker.workers.dev npm run test:features:deployed
AIRYFS_URL=https://your-worker.workers.dev npm run test:regression:quick
AIRYFS_URL=https://your-worker.workers.dev npm run test:regression:broad
AIRYFS_URL=https://your-worker.workers.dev npm run test:prepush:deployed
```

The original end-to-end flow covers direct write to FUSE read, direct mutation invalidation, FUSE write to direct read, Git on the same mixed-access volume, open-handle leases (a held FUSE read that survives a direct unlink and a streaming rename-over), and persistence across Container destruction. The feature smoke also covers unaligned cross-chunk I/O, random writes, directory traversal, negative lookup invalidation, concurrent reads, change feeds, tree archives, snapshots and cloning, resumable uploads, streaming execution admission and cancellation, and durable jobs.

The Container regression harness models typical non-Git agent workloads. The quick profile checks runtime availability, POSIX file lifecycle, direct API/FUSE coherence, and Node.js-to-Python data flow. The broad profile adds mixed binary I/O, concurrent metadata operations, a native `make`/`g++` build, archive handling, error semantics, streaming output, and restart persistence. The pre-push command runs the feature smoke followed by the broad profile.

Remote mounts use a 1-second entry and attribute TTL plus journal-driven invalidation, and lease open handles so live reads survive concurrent direct removal. See [`docs/FUSE_CACHE_TTL.md`](docs/FUSE_CACHE_TTL.md), [`docs/MUTATION_INVALIDATION.md`](docs/MUTATION_INVALIDATION.md), and [`docs/OPEN_INODE_LEASES.md`](docs/OPEN_INODE_LEASES.md) for the implementation and deployed measurements.

### Performance Benchmarks

```bash
cd worker
npm run benchmark:chunks
cd ..
AIRYFS_URL=https://your-worker.workers.dev npm run benchmark:deployed -- \
  --profile quick --label baseline --output benchmark-baseline.json
AIRYFS_URL=https://your-worker.workers.dev npm run benchmark:quick -- \
  --label candidate --output benchmark-candidate.json
```

The Worker benchmark isolates local SQLite chunk-size behavior. The deployed harness measures direct HTTP, Container/FUSE, Hrana amplification, metadata traversal, small files, and Git workloads. See [`docs/PERFORMANCE_BENCHMARK.md`](docs/PERFORMANCE_BENCHMARK.md) for the baseline and optimization workflow and [`docs/CHUNK_SIZE_BENCHMARK.md`](docs/CHUNK_SIZE_BENCHMARK.md) for the 256 KiB default decision.

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
    benchmark.mjs               Deployed direct and FUSE performance harness
  docs/                         Design and operational notes
  scripts/
    deploy.mjs                  Guarded int/prod deployment
```

## License

MIT
