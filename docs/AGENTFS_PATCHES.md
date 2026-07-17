# AgentFS Vendoring

AiryFS vendors the latest stable AgentFS release as a pristine source snapshot and applies an ordered patch series at build time. The snapshot under `agentfs/upstream/` must never be edited directly.

## Build

Run:

```sh
./agentfs/build.sh
```

The script recreates the ignored `agentfs/.build/` tree, verifies and applies every patch, builds and tests the TypeScript SDK, tests both Rust crates, builds the Linux Rust CLI with stable Rust 1.88, and places the binary at `container/bin/agentfs`. Rust artifacts are retained in the ignored `agentfs/.target/` cache so repeated patch verification remains practical.

AiryFS-specific runtime behavior remains isolated in the ordered patch series. `0008-configurable-fuse-cache-ttl.patch` adds bounded remote FUSE caching without changing AgentFS's default local single-writer behavior. `0009-remote-mutation-invalidation-poller.patch` adds journal-driven invalidation with an optional independent remote URL.

The worker consumes the TypeScript package from `agentfs/.build/sdk/typescript`, so run the AgentFS build before installing worker dependencies from a clean checkout.

## Refresh Upstream

1. Find the latest stable tag with the command recorded in `agentfs/UPSTREAM.manifest`.
2. Replace `agentfs/upstream/` with a pristine archive of that exact commit.
3. Update both the tag and commit in `agentfs/UPSTREAM.manifest`.
4. Rebase each patch in order. Keep independent behavior changes in independent patches.
5. Run `./agentfs/build.sh`, worker tests and typecheck, the container build, and deployed integration tests.

## Patch Rules

- Patch files are the only place for AiryFS-specific AgentFS changes.
- Preserve upstream attribution when backporting an upstream commit.
- Run `git apply --check` through the build script after every patch change.
- Do not add generated `agentfs/.build/`, Rust targets, TypeScript `dist/`, or dependencies to Git.
