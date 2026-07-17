# Remote Mutation Invalidation

AiryFS records direct filesystem mutations in Durable Object SQLite and invalidates matching entries in an active remote FUSE mount.

## Implementation

- `fs_mutation_journal` assigns a monotonic sequence to each affected parent, name, and current inode.
- Direct writes, deletes, directory operations, renames, copies, symlinks, and truncation append journal rows after the mutation succeeds.
- `agentfs/patches/0009-remote-mutation-invalidation-poller.patch` polls new rows every 100 milliseconds for bounded remote mounts.
- The poller skips historical rows when a mount starts, then queues FUSE entry invalidation for each new row. Re-lookup refreshes both the directory entry and inode attributes.
- Invalidations use the session's existing deferred notification queue. Writing notifications directly from the poller can deadlock `/dev/fuse` while the kernel waits for the session loop to process a generated request.
- `--invalidation-url` gives the poller an independent remote connection pool.
- AiryFS routes normal FUSE SQL through HTTP `:8080` and TCP `:9000`, while journal polling uses HTTP `:8081` and TCP `:9001`. Separate FIFO channels prevent reconnects or poll requests from consuming another client's response.

The journal and filesystem remain in the same Durable Object SQLite database. No additional persistent system is involved.

## Deployed Result

The integration gate primes FUSE metadata, truncates the same inode through the direct API, renames a cached entry through the direct API, then runs Git on that same mixed-access volume.

| Check | Result |
| --- | --- |
| Same-inode size refresh | 1.660 seconds, below the 5-second gate |
| Direct entry rename | New name visible through the active FUSE mount |
| Same-volume Git | Init, add, commit, and log pass after mixed direct/FUSE mutations |
| Container replacement | File and Git history survive destruction and remount |
| Deployed suite | 18/18 checks pass |

Run the gate with:

```sh
AIRYFS_URL=https://airyfs-int.example.workers.dev ./e2e/test.sh
```
