# @airyfs/flue-sandbox

A [Flue](https://flueframework.com) sandbox adapter that gives every agent a
durable [AiryFS](https://github.com/tysont/airyfs) volume as its workspace.

Flue's `SandboxApi` separates filesystem operations from `exec`. That split maps
one-to-one onto AiryFS's own separation of its **direct path** (Durable Object
SQLite, no Container, sub-100ms) from its **execution path** (a Container
attached over FUSE only for real Linux work). Eight of the nine `SandboxApi`
methods are served straight from SQLite; only `exec` wakes the Container. An
agent that spends most of its tool calls reading, writing, and listing files
never pays for compute, and the workspace survives harness restarts and
Container eviction because the only durable copy lives in the volume's DO.

## Install

```bash
npm install @airyfs/flue-sandbox airyfs-sdk
# peer: @flue/runtime
```

## Use

```ts
import { defineAgent } from "@flue/runtime";
import { airyfs } from "@airyfs/flue-sandbox";

export default defineAgent(({ env }) => ({
  model: "cloudflare/@cf/openai/gpt-oss-120b",
  sandbox: airyfs({
    endpoint: env.AIRYFS_ENDPOINT,
    token: env.AIRYFS_TOKEN, // omit for an unauthenticated endpoint
  }),
  instructions: "Your files persist between conversations.",
}));
```

`createSessionEnv({ id })` maps the harness id (agent instance id, or workflow
run id) to a volume named `agent-<slug>-<hash>` by default, so each instance
gets a durable, zero-provisioning workspace. Override with `volume: (id) => ...`
to pin or share volumes.

## Design notes

- **One path plane at `/volume`.** The adapter roots the workspace at `/volume`
  — the exact FUSE mount point inside the Container. File methods translate that
  to volume-rooted SDK paths (`/volume/src/a` → `/src/a`); `exec` runs in the
  `/volume` plane untouched. A file written via the file tools and a shell
  `cat` of the same path see identical bytes. See `src/paths.ts`.
- **Honest metadata.** `stat` returns real POSIX `size` and `mtime` (converted
  from AiryFS's Unix-seconds to a `Date`); nothing is fabricated.
- **Parent creation.** `writeFile` does *not* create parents — it lets AiryFS's
  `ENOENT` propagate so Flue's `createSandboxSessionEnv` runs its
  `mkdir(parent,{recursive})`-and-retry-once guarantee.
- **Durable exec.** Uses AiryFS durable exec (a per-call idempotency key), which
  queues behind the single execution slot rather than failing `EXEC_BUSY` — one
  fewer sharp edge in an agent loop.
- **Timeouts are best-effort.** `timeoutMs ≤ 300s` wraps the command in
  `timeout Ns` (exit 124 + a stderr note on expiry). Above 300s it runs
  unwrapped; the platform's own 300s process ceiling applies. `exec` never
  throws `SandboxOperationUnsupportedError` — a result the model can react to
  beats an exception mid-loop.
- **Runs in workerd.** `airyfs-sdk` binds the global `fetch` to `globalThis`, so
  the adapter runs unchanged inside a Cloudflare Worker (an unbound global fetch
  reference throws "Illegal invocation" in workerd). Pass a custom `fetch` via
  the factory config if you need to (e.g. a CA bundle in tests).

## Typed text operations (optional)

AiryFS also exposes typed, server-side text operations that run in the Durable
Object with no Container — `readLines`, `replaceText`, `lineStats`, `jsonQuery`
— on the underlying `airyfs-sdk` client. The Flue `SandboxApi` surface is the
fixed nine methods, so these are **not** exposed as model tools by default.

To offer them to a Flue agent, supply a `SessionToolFactory` via the sandbox
factory's `tools` option and wrap the client calls as `AgentTool`s. Note the
Flue contract: **`tools` replaces the framework's entire default tool list**, so
if you add these you must also re-declare the file/shell tools you still want —
don't clobber the defaults by accident. Most agents don't need this; the model's
`bash` tool can call the same operations, and application code can call the SDK
directly (e.g. in a workflow) for the fast, Container-free path. The
`@airyfs/agents-toolkit` package exposes all four as ready-made tools if you are
on the Agents SDK rather than Flue.

## Security

Prefer a volume-scoped `read,write,exec` AiryFS capability token over the root
secret. One agent per volume with a per-volume capability makes the isolation
unit and the credential unit coincide.

## Tests

`npm test` runs an integration suite against a live AiryFS endpoint
(`AIRYFS_ENDPOINT`, default the public int deployment; `AIRYFS_TOKEN` optional).
It exercises all nine methods, error mapping (`ENOENT`/`EEXIST`), binary
round-trips, `mtime` fidelity, the path-plane guarantee (a file written at
`/volume/x` is readable by both `cat /volume/x` and `cat x`), the Flue
mkdir-and-retry loop, durable-exec queuing, and `timeoutMs` → exit 124.
