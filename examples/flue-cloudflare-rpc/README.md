# flue-cloudflare-rpc

A same-deployment variant of the Flue AiryFS sandbox that talks to the AiryFS
Durable Object over **Workers RPC** instead of HTTP — no fetch, no
serialization layer between the Workers.

`src/sandboxes/airyfs-rpc.ts` implements the same Flue `SandboxApi` as
[`@airyfs/flue-sandbox`](../../integrations/flue-airyfs) (identical `/volume`
path plane and semantics), but each method is a direct call on the `AiryFS`
Durable Object stub: `stub.readFile(...)`, `stub.writeFileStream(...)`,
`stub.exec(...)`. Plug it into an agent exactly like the HTTP adapter:

```ts
import { defineAgent } from "@flue/runtime";
import { airyfsRpc } from "./sandboxes/airyfs-rpc";

export default defineAgent(({ env }) => ({
  model: "cloudflare/@cf/openai/gpt-oss-120b",
  sandbox: airyfsRpc(env.AIRYFS), // volume defaults to agent-<slug>-<hash>
}));
```

## The key idea: cross-script Durable Object binding

You do **not** need to fold AiryFS's Durable Object, container image, and
migrations into your Worker. Keep AiryFS deployed as its own Worker and bind its
class cross-script:

```jsonc
// wrangler.jsonc
"durable_objects": {
  "bindings": [
    { "name": "AIRYFS", "class_name": "AiryFS", "script_name": "airyfs-int" }
  ]
}
```

`env.AIRYFS.idFromName(volume)` → `.get(id)` yields a stub to the AiryFS class in
its **defining** Worker, so the Container stays attached there and there is zero
build-system merging or migration entanglement. Point `script_name` at your
AiryFS Worker's deployed name.

## Verified

```bash
npx wrangler deploy --dry-run
#  env.AIRYFS (AiryFS, defined in airyfs-int)   Durable Object   ✓ binding resolves

export CLOUDFLARE_ACCOUNT_ID=... CLOUDFLARE_API_TOKEN=...
npx wrangler dev --remote --port 8811   # edge preview can reach the cross-script DO
curl "localhost:8811/?id=demo" -d '{"action":"write","path":"/volume/rpc.txt","content":"hi"}'
curl "localhost:8811/?id=demo" -d '{"action":"read","path":"/volume/rpc.txt"}'   # -> "hi"
curl "localhost:8811/?id=demo" -d '{"action":"bash","command":"cat /volume/rpc.txt"}'  # container reads it back
```

Live-tested end to end this way: the RPC write, read, and a Container `bash`
that reads the RPC-written file all succeed, and the file is confirmed on the
AiryFS endpoint — proving the path plane holds across the RPC boundary too.

## When to use RPC vs. HTTP

Prefer the HTTP adapter (`@airyfs/flue-sandbox`, separate AiryFS deployment) for
most cases: the direct path is already sub-100ms, so the RPC saving is marginal.
Reach for RPC when you want AiryFS's streaming RPC methods for large binary
transfer inside tools, or a single-tenant appliance-style deployment. The
cross-script binding above is the low-friction way to get it; only fold the DO
into your own Worker if you specifically need a single published artifact.

`src/index.ts` is a deterministic proof handler (no LLM); it wraps the RPC
adapter in `createSandboxSessionEnv` and exposes `write`/`read`/`bash` actions.
