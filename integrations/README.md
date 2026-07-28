# AiryFS integrations

Integrations that make an [AiryFS](https://github.com/tysont/airyfs) volume the
durable workspace for AI agents. Kept separate from the core AiryFS code; each
package links `airyfs-sdk` via a local `file:` path and is independently built
and tested.

| Package | For | Gives you |
|---------|-----|-----------|
| [`flue-airyfs`](./flue-airyfs) (`@airyfs/flue-sandbox`) | [Flue](https://flueframework.com) | A `SandboxApi` adapter: every agent instance gets a durable AiryFS volume as its workspace. |
| [`agents-airyfs`](./agents-airyfs) (`@airyfs/agents-toolkit`) | [Cloudflare Agents SDK](https://developers.cloudflare.com/agents/) | AiryFS as model-facing tools + `runFiber`/`stash` durable-exec composition. |

Runnable samples live in [`../examples`](../examples):

- [`flue-repo-agent`](../examples/flue-repo-agent) — Flue on the Cloudflare
  target, Workers AI models, AiryFS workspace over HTTP.
- [`agents-workspace`](../examples/agents-workspace) — a deployable Agents SDK
  Worker using the toolkit.
- [`flue-cloudflare-rpc`](../examples/flue-cloudflare-rpc) — the Flue adapter
  over Workers RPC via a cross-script Durable Object binding.

## Why the fit is structural

Both harnesses separate **filesystem operations** from **command execution**.
AiryFS separates its **direct path** (Durable Object SQLite — reads, writes,
listings, sub-100ms, no compute) from its **execution path** (a Container
attached over FUSE only when a workload needs Git, a compiler, or other native
Linux tools). The two separations line up: file tools hit SQLite for free while
the agent is otherwise idle, and only `exec`/`bash` wakes the Container, which
sleeps again after 30 minutes. The single durable copy of the workspace lives in
the volume's DO, so it survives harness restarts and Container eviction — a
durable workspace that is decoupled from conversation durability.

## Build & test

Each package builds with `tsc` and tests against a live AiryFS endpoint
(`AIRYFS_ENDPOINT`, default the public int deployment; `AIRYFS_TOKEN` optional):

```bash
cd sdk && npm install && npm run build      # build airyfs-sdk first (file: links)
cd ../integrations/flue-airyfs && npm install && npm test
cd ../agents-airyfs && npm install && npm test
```
