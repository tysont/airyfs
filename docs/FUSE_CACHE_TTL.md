# FUSE Cache TTL

AiryFS mounts remote AgentFS volumes with a 1-second kernel entry and attribute cache TTL. AgentFS keeps its previous infinite TTL and writeback cache by default for single-writer local mounts.

## Implementation

- `agentfs/patches/0008-configurable-fuse-cache-ttl.patch` adds `agentfs mount --cache-ttl-ms`.
- A bounded TTL applies to lookup entries, attributes, directory-plus entries, and mutation replies.
- Bounded mounts disable `FUSE_WRITEBACK_CACHE`, whose single-writer assumptions otherwise preserve stale file sizes despite attribute expiry.
- The AiryFS Container passes `--cache-ttl-ms 1000`.
- Each exec request refreshes the Hrana bridge connection so uncached FUSE requests continue across Durable Object request boundaries.

## Deployed Result

The integration test primes FUSE metadata for a 1 MiB file, truncates the same inode to 512 KiB through the direct API, and measures when FUSE reports the new size. It also renames a cached entry through the direct API and verifies the active mount resolves the new name.

| Check | Before | With bounded caching |
| --- | --- | --- |
| Same-inode size refresh | Remained at the cached size after more than 3.5 seconds | 1.79 seconds |
| Direct entry rename | Unbounded cache lifetime | Visible on the next bounded exec |
| Deployed suite | Mixed-runtime Git required a mount workaround | 27/27 checks pass with Git isolated to a fresh smoke-test volume |

Run the deployed gate with:

```sh
AIRYFS_URL=https://airyfs-int.example.workers.dev ./e2e/test.sh
```

Mutation-journal invalidation now lets metadata-heavy FUSE workloads run after direct mutations on the same volume. See [`MUTATION_INVALIDATION.md`](MUTATION_INVALIDATION.md).
