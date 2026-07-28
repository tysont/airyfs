# flue-repo-agent

A [Flue](https://flueframework.com) app on the **Cloudflare target** whose
workspace is a durable [AiryFS](https://github.com/tysont/airyfs) volume via
[`@airyfs/flue-sandbox`](../../integrations/flue-airyfs). Models run on
**Workers AI** (no model API key — billing follows the Worker's account).

Flue generates an Agents-SDK Durable Object per agent/workflow; the HTTP AiryFS
adapter (pure `fetch`) runs unchanged inside that Worker. The agent DO holds
conversation state; the AiryFS volume holds files. Both are durable and scale to
zero independently.

## Contents

- `src/workflows/summarize.ts` — **the exercised demo.** A finite workflow that
  stages an input file into the AiryFS volume via `harness.fs.writeFile` (which
  runs through the adapter to real DO-backed storage), reads it back, produces a
  one-line summary with a single Workers AI generation, and writes the artifact
  back into the volume. Robust: no model-driven tool calls.
- `src/agents/workspace.ts` — a continuing agent with the same durable
  workspace, exposed at `POST /agents/workspace/:id`. See the limitation below.

## Run

```bash
cp .dev.vars.example .dev.vars   # set CLOUDFLARE_ACCOUNT_ID
export CLOUDFLARE_API_TOKEN=...  # account with Workers AI access
npx flue dev --target cloudflare --port 3599
```

```bash
# Workflow (staged + produced files land in the AiryFS volume):
curl "http://127.0.0.1:3599/workflows/summarize?wait=result" \
  -H "Content-Type: application/json" \
  -d '{"text":"AiryFS keeps each volume in one Durable Object SQLite database."}'
```

Set `AIRYFS_VOLUME` in `.dev.vars` to pin a fixed volume so you can inspect the
files it leaves behind at `${AIRYFS_ENDPOINT}/v1/volumes/<volume>/files/...`.
`drive.sh` runs the workflow and agent against a running dev server.

## Model choice

Default `cloudflare/@cf/openai/gpt-oss-120b`. It streams OpenAI-compatible
responses (with `finish_reason`) through Flue's Workers AI binding. Several other
`@cf/*` models either 400 or omit `finish_reason` on the binding path. Override
with `FLUE_MODEL`.

## Known limitation (continuing agent + tool loop)

The `workspace` agent's *tool loop* is currently gated by an upstream Flue ↔
Workers AI incompatibility: the model's tool call executes (the file lands in
the volume), but feeding the tool result back for the next turn returns
`400 Bad Request` from the AI binding for the `@cf/*` models tested. So the
single-generation **workflow** is the reliable Workers AI path here.

The adapter's full read/write/list/**exec** behavior is proven independently by
`@airyfs/flue-sandbox`'s integration suite (16 tests, including exec in the
Container). For a robust end-to-end *agent* tool loop today, point the agent at a
tool-capable provider (e.g. `anthropic/...`) with that provider's API key; the
sandbox wiring is identical.
