# agents-workspace

A deployable [Cloudflare Agents SDK](https://developers.cloudflare.com/agents/)
Worker whose durable workspace is an [AiryFS](https://github.com/tysont/airyfs)
volume, via [`@airyfs/agents-toolkit`](../../integrations/agents-airyfs).

The agent keeps **two independent durable stores**, both keyed to the same
instance name: the agent DO's own SQLite (`this.state` / `this.sql`) and the
AiryFS volume `ws-<name>` (files). Each scales to zero on its own, and the
volume survives the agent DO being evicted or redeployed.

## Run

```bash
export CLOUDFLARE_ACCOUNT_ID=... CLOUDFLARE_API_TOKEN=...
npx wrangler dev --port 8799
NAME=demo node scripts/drive.mjs
```

`drive.mjs` POSTs JSON actions to `/agents/workspace-agent/<name>`:

| action | what it exercises |
|--------|-------------------|
| `write` / `read` / `list` | direct-path file tools (no Container) |
| `bash` | a command in the Container (`cwd` `/volume`) |
| `build` | **durable execution**: `runFiber` + `execInFiber` (idempotency-keyed exec, recoverable via `onFiberRecovered`) |
| `job` | submit a durable job, then poll with the Agent's `schedule()` (request returns immediately) |
| `publish` / `share` | serve a static site / share link straight from the volume |

Every response includes the agent's own `actions` counter (its DO SQLite state),
demonstrating agent-state and workspace living side by side. Files and artifacts
are verifiable on the endpoint at `/v1/volumes/ws-<name>/...`, `/s/ws-<name>/`,
and `/d/ws-<name>/<id>`.

## Wiring

`src/index.ts` builds an `AiryFSWorkspace` per request (`ws-${this.name}`) and
turns it into tools with `createWorkspaceTools(ws, { includeArtifacts: true })`.
The `onRequest` handler invokes those tool `execute()` functions directly so the
integration is exercisable without an LLM; the same tools feed a model tool loop
when you add a tool-capable provider (see `bash`/`build`/`job` for the
durable-execution and job-handoff patterns).

Config (`wrangler.jsonc`): the `WorkspaceAgent` Durable Object (SQLite) + its
migration, an `AI` binding for extending with a model, and `AIRYFS_ENDPOINT`.
