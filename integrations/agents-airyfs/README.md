# @airyfs/agents-toolkit

AiryFS durable-workspace tools for the [Cloudflare Agents
SDK](https://developers.cloudflare.com/agents/).

Where `@airyfs/flue-sandbox` plugs AiryFS into Flue's sandbox contract, this
package targets agents built **directly** on the Agents SDK. It gives an agent a
durable [AiryFS](https://github.com/tysont/airyfs) volume as model-facing tools,
plus helpers that compose AiryFS with the Agents SDK's durable-execution
primitives.

It depends only on `airyfs-sdk` (and `zod` as a peer for tool schemas) — no hard
`agents`/`ai` dependency, so it sidesteps that ecosystem's peer-version churn.
You supply your own `tool()` when adapting the descriptors.

## Install

```bash
npm install @airyfs/agents-toolkit airyfs-sdk zod
```

## Use

```ts
import { AiryFSWorkspace, createWorkspaceTools } from "@airyfs/agents-toolkit";
import { tool } from "ai";

const ws = new AiryFSWorkspace(env.AIRYFS_ENDPOINT, `ws-${this.name}`, {
  token: env.AIRYFS_TOKEN,
});

// Adapt the framework-neutral descriptors to your AI SDK version:
const tools = Object.fromEntries(
  Object.entries(createWorkspaceTools(ws, { includeArtifacts: true }))
    .map(([name, t]) => [name, tool(t)]),
);
```

`AiryFSWorkspace` methods are also directly callable (`ws.readFile`,
`ws.writeFile`, `ws.listDir`, `ws.bash`, `ws.runJob`, `ws.publishSite`,
`ws.shareLink`), which is how the deterministic tests and the sample's HTTP
actions drive them without a model.

## What you get

- **Direct-path tools** — `read_file`, `write_file` (auto-creates parent dirs),
  `list_dir`. Served from Durable Object SQLite, no Container.
- **`bash`** — runs in the Container (`cwd` `/volume`, ~300s ceiling). Takes a
  recoverable guard snapshot first, then **prunes old `pre-exec-*` snapshots to
  a bounded count** (default 3) so a file-churning agent can't slowly fill the
  volume's capped SQLite database.
- **Durable jobs** — `run_job` / `job_status` for work beyond the 300s exec
  ceiling. `cwd` is volume-rooted (`/` is the root; the runner mounts it at
  `/volume`). The sample polls with the Agent's `schedule()` rather than blocking.
- **Artifacts** — `publish_site` and `share_link` serve straight from the volume
  (no Container), returning the public `/s/<volume>/` and `/d/<volume>/<id>`
  URLs. These call AiryFS routes the SDK doesn't wrap, via direct `fetch`.
- **`execInFiber` / `replayStashedExec`** — compose the Agents SDK's
  `runFiber`/`stash` durable execution with AiryFS's idempotent exec. The
  command's idempotency key is checkpointed in the fiber; on DO eviction your
  `onFiberRecovered(ctx)` hook replays with the *same* key, so AiryFS returns
  the existing command's persisted output instead of running it twice.

## Tests

`npm test` runs an integration suite against a live AiryFS endpoint. It executes
every tool `execute()` and workspace method directly (no LLM): write/read/list,
`bash` in the Container, `timeoutMs` → exit 124, bounded snapshot retention,
durable-job submit-and-wait, and both artifacts (asserting the published site
and share link actually serve their content over HTTP).
