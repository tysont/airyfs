# Phase 3 Recovery Inventory

This file records the recovery boundary created after the failed Phase 4 transport experiment. Phase 0 through Phase 3 were not committed when originally completed. Do not begin Phase 4 again until the state described here passes every local and deployed Phase 3 gate and is committed.

## Recovery Snapshots

The ignored `.airyfs/recovery/` directory contains point-in-time recovery artifacts created before further recovery edits:

| Artifact | SHA-256 | Contents |
| --- | --- | --- |
| `phase3-current-tracked.patch` | `60e0d08eee91e2ff11d754b680fba347faccd41dac143dee121444e436cb644b` | Binary Git diff for all tracked changes from `88e8c50` |
| `phase3-current-status.txt` | `cc7380e0e9faa8915452127cb3aca1df5f56bfb0ee952b05542eb2d31ca9f164` | Tracked and untracked worktree inventory |
| `phase3-current-worktree.tar.gz` | `ee5f1305f9868e2ac76727d579020099a5a55ad9ede31475ca51a7384437c6a5` | Source archive excluding `.git`, secrets, dependencies, generated builds, Playwright output, and `.airyfs` itself |

The snapshots are recovery aids, not commit inputs. The source tree remains authoritative after verification.

## Verified Recovery Artifacts

The original retained image `airyfs-int-airyfs:a97b2b01` reproduced a journal-invalidation deadlock during recovery. The verified recovery routes invalidations through FUSE's existing deferred notification queue. Integration image `airyfs-int-airyfs:a4cfdfc5` passed all 18 checks after its rollout completed.

| Artifact | SHA-256 |
| --- | --- |
| `/usr/local/bin/agentfs` / `container/bin/agentfs` | `1be9a17420acc79435168dde1bfa3cef04477b4bfdf985feadcb6d0bbf8d2dda` |
| `/app/dist/bridge.js` / `container/dist/bridge.js` | `cd7b039032066d0b1697089ecaed61952fa01de1aecdae2f30a670768577abc0` |
| `/app/dist/command-server.js` / `container/dist/command-server.js` | `0a2ce6eee1fb23612d65f582e1a1dbc2419bf93f69ff618478fb81fd47f22dd5` |

The verified Worker version is `0c327cdd-976d-4ad6-8d53-cea05b73ab20`. Bound Durable Object data was not rolled back during recovery.

## Phase Inventory

Phase 0:

- Pristine AgentFS `v0.6.4` snapshot and manifest.
- Ordered patches `0001` through `0007`.
- Reproducible TypeScript SDK and Linux Rust CLI build.
- Worker consumes the locally built TypeScript SDK.

Phase 1:

- Explicit volume creation and immutable power-of-two chunk sizing.
- 256 KiB default and chunk amplification benchmark.
- Isolated local, integration, and production deployment tooling and guards.

Phase 2:

- Patch `0008-configurable-fuse-cache-ttl.patch`.
- One-second remote entry and attribute TTL.
- Writeback cache disabled for bounded mounts.
- Direct same-inode truncate operation and deployed coherence gate.

Phase 3:

- `fs_mutation_journal` schema and direct mutation recording.
- Patch `0009-remote-mutation-invalidation-poller.patch`.
- Independent data and invalidation bridge channels.
- Journal-driven FUSE entry invalidation through the session's deferred notification queue.
- Same-volume direct/FUSE/Git integration flow and Container replacement check.

## Removed Phase 4 Experiment

The failed experiment added correlated transport envelopes, cancellation messages, frame limits, serialized write backpressure, and bounded admission. It passed local tests but stalled the deployed Durable Object socket across per-exec reconnects and was removed. No `TransportEnvelope`, `MAX_TRANSPORT_FRAME_BYTES`, `requestChain`, `queuedRequests`, or transport-counter implementation remains.

## Required Recovery Gates

1. `./agentfs/build.sh`
2. Worker tests and typecheck: 72 tests expected.
3. Cloud deployment-helper tests: 7 tests expected.
4. Container TypeScript build.
5. `git diff --check` and shell syntax validation for `e2e/test.sh`.
6. Integration deployment from the recovered source.
7. Deployed integration suite: 18/18 expected, including a sub-5-second metadata refresh, same-volume Git, and persistence after Container destruction.
8. Commit the complete recovered Phase 0-3 state before beginning Phase 4.
