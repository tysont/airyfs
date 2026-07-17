# Open Inode Leases

AiryFS lets two runtimes mutate one volume: the Container's remote FUSE mount and
the Worker's direct AgentFS adapter. POSIX requires that a file opened by a
process keep working after its pathname is removed. A Worker direct unlink or a
streaming rename-over would otherwise delete an inode the FUSE mount is still
reading. Persistent, expiring open-handle leases close that gap.

## Data Model

`fs_open_inode` is the sole authority, stored in the same Durable Object SQLite
database as the filesystem:

```sql
CREATE TABLE fs_open_inode (
  session_id TEXT NOT NULL,
  ino INTEGER NOT NULL,
  open_count INTEGER NOT NULL DEFAULT 0,
  expires_at INTEGER NOT NULL,
  PRIMARY KEY (session_id, ino)
);
CREATE INDEX idx_fs_open_inode_expires ON fs_open_inode(expires_at);
```

Inode deletion is the single authoritative cleanup point. A trigger cascades
chunk, symlink, and lease removal atomically:

```sql
CREATE TRIGGER trg_fs_inode_delete_cleanup
AFTER DELETE ON fs_inode
BEGIN
  DELETE FROM fs_data WHERE ino = OLD.ino;
  DELETE FROM fs_symlink WHERE ino = OLD.ino;
  DELETE FROM fs_open_inode WHERE ino = OLD.ino;
END;
```

The upstream schema version stays `0.4`; the table and trigger are additive and
version detection keys off `fs_inode` columns.

## Atomicity Over Hrana

Lease operations run against Durable Object SQLite over Hrana, where a
multi-statement transaction is fragile. Every operation is therefore either a
single self-contained statement or a sequence of independently safe statements —
no operation relies on `BEGIN`/`COMMIT`.

- Pin is one atomic upsert that inserts only when the inode still exists:
  `INSERT INTO fs_open_inode (...) SELECT ?, ?, 1, ? FROM fs_inode WHERE ino = ?
  ON CONFLICT(session_id, ino) DO UPDATE SET open_count = open_count + 1,
  expires_at = excluded.expires_at`. It affects exactly one row on success; zero
  affected rows means the inode is gone (mapped to `ENOENT`). The `SELECT`'s
  `WHERE` clause also disambiguates the upsert parse.
- Unpin is three independent statements (decrement, delete-when-zero, reap the
  inode only when it is `nlink = 0` and no live lease remains, all guarded inside
  a single `DELETE ... WHERE NOT EXISTS (...)`).
- Session cleanup and expired-orphan reaping are two independent statements each.

Because none of these use a transaction, a mid-sequence failure can only leave a
lazily reapable state (a stale lease row or an unreferenced `nlink = 0` inode);
it never deletes live data and never premature-deletes a leased inode.

## FUSE Lifecycle (Rust)

Leases apply to remote bounded mounts only — the same condition that enables the
mutation-invalidation poller (`--remote-url` with a finite `--cache-ttl-ms`).
Local single-writer mounts keep their original behavior.

- Each mount generates a persistent session UUID.
- The heartbeat's lease connection is opened synchronously before the mount is
  reported ready. If it cannot connect, the mount fails to start (the process
  exits and the container command server reports the FUSE daemon unhealthy)
  rather than run without a working lease-renewal path.
- FUSE `open` and `create` resolve a stable lease inode key, then pin
  (`open_count += 1`, `expires_at = now + TTL`) before replying, so a direct
  unlink racing the reply still observes the lease. On a lease-enabled mount the
  pin must be persisted before success is reported: if the pin fails, the handle
  is dropped and the operation fails rather than expose a handle a concurrent
  unlink could delete. `pin_open_inode` inserts only when the inode still exists,
  so a missing inode maps to `ENOENT` and any other failure maps to `EIO`
  (busy/timeout maps to `EAGAIN`). `create` and pin are not a single
  transaction; on pin failure the just-created empty file is best-effort
  unlinked so the failed create leaves no stray entry. Non-lease mounts keep the
  original no-op success.
- FUSE `release` unpins. Dropping the final hold reaps the inode when it is
  already unlinked and unleased. A failed unpin is logged and left for the
  heartbeat reaper, since it can only defer cleanup, never delete live data.
- A heartbeat (default every 10s) renews the leases of every currently open
  handle and reaps expired lease rows plus the now-unreferenced orphan inodes
  they were retaining.
- Graceful unmount drops the whole session and reaps anything it retained.

### Heartbeat failure policy

Sustained renewal failure is treated as a data-safety hazard: a live handle must
never outlast its lease and have its inode reaped from under it. The heartbeat
tracks how long renewal has been failing continuously; once that exceeds a bound
kept at or below half the TTL (default 60s abort window against a 120s TTL, and
clamped to `TTL/2` regardless of overrides), the process aborts (exit code 87).
The container command server observes the dead FUSE daemon and returns `503` for
subsequent `exec` calls; the Durable Object then restarts the container and
remounts. Aborting well before the earliest possible lease expiry guarantees no
other mount can reap an inode a live handle here still holds.

The lease TTL is 120 seconds with a 10-second heartbeat, so a live handle is
renewed roughly twelve times per lease window. TTL, heartbeat interval, and
abort window are overridable for tests via `AGENTFS_LEASE_TTL_SECS`,
`AGENTFS_LEASE_HEARTBEAT_SECS`, and `AGENTFS_LEASE_FAILURE_ABORT_SECS` (the
container forwards its environment to the mount); production defaults are never
weakened and the abort window stays clamped below the TTL.

### Cleanup timing

Reaping of a retained (unlinked, still-leased) inode happens at whichever of
these comes first:

- the last handle is closed (`release` reaps it immediately when possible), or
- the owning mount unmounts gracefully (session cleanup reaps it), or
- a bounded mount's heartbeat runs its expired-orphan reap after the lease
  expires.

There is no alarm or direct maintenance trigger. Cleanup of a stale lease left
by a mount that vanished without graceful shutdown is therefore **lazy**: its
inode is reaped only when some bounded remote mount next runs its heartbeat reap
(which reaps expired leases and orphans globally, not just its own). If no
bounded mount ever runs again, the orphan persists until one does. The Worker's
direct path only reads leases to decide retention; it does not run a reaper.

### Overlay inode namespace

The lease table is keyed by the stable underlying (delta) inode. For an overlay
mount the synthetic FUSE inode is translated once at open/create time via
`FileSystem::lease_inode`, and that resolved key is stored on the open handle and
used for pin, renew, and unpin — so lease bookkeeping never depends on a
synthetic namespace that could be rebuilt or evicted. Read-only base-layer files
are not leasable (they cannot be deleted out from under a handle). Ordinary
AgentFS mounts use the identity mapping.

## Retention Semantics (Rust and Worker)

`unlink` and rename-over, in both the Rust AgentFS implementation and the
Cloudflare TypeScript adapter, drop the directory entry and decrement `nlink`
immediately, so the pathname changes right away. When `nlink` reaches zero they
delete the inode only if no unexpired lease with a positive open count remains.
A leased inode is retained with its data; the inode is deleted (cascading to
chunks, symlink target, and leases) once the last handle closes or every lease
expires.

The design biases toward retaining too long over deleting live data: the
retention check treats any unexpired positive-count lease as a reason to keep
the inode. Correctness of that invariant takes priority at open time, so on a
lease-enabled mount a pin that cannot be persisted fails the open or create
(dropping the local handle and returning `ENOENT` for a missing inode or `EIO`
otherwise) instead of handing back a handle whose inode a concurrent unlink
could delete.

## Deployed Gates

`e2e/test.sh` adds two deployed gates on the shared mixed-access volume:

| Gate | Check |
| --- | --- |
| Open-unlink | A FUSE handle holding an open fd keeps reading after a Worker `DELETE`, and the pathname 404s immediately. |
| Open-replace | A FUSE handle keeps reading the original inode after a streaming `PUT` replaces the pathname via rename-over, while the path resolves to the new content immediately. |

Run the gates with:

```sh
AIRYFS_URL=https://airyfs-int.example.workers.dev ./e2e/test.sh
```
