import { airyfsRpc, type RpcNamespace } from "./sandboxes/airyfs-rpc.js";

interface Env {
  // Cross-script Durable Object binding to the deployed AiryFS Worker's
  // `AiryFS` class (see wrangler.jsonc `script_name`).
  AIRYFS: RpcNamespace;
}

/**
 * Deterministic proof that the RPC adapter drives the AiryFS Durable Object
 * over a cross-script binding — no LLM, no HTTP layer between the Workers. In a
 * real Flue Cloudflare-target app you would instead pass `airyfsRpc(env.AIRYFS)`
 * as an agent's `sandbox` (see README); this handler exercises the same adapter.
 *
 * POST a JSON body: { action: "write"|"read"|"bash", path?, content?, command? }
 * with `?id=<instance>` selecting the volume.
 */
export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const url = new URL(request.url);
    const id = url.searchParams.get("id") ?? "rpc-demo";
    const factory = airyfsRpc(env.AIRYFS);
    const session = await factory.createSessionEnv({ id });

    if (request.method !== "POST") {
      return Response.json({
        message: "AiryFS-over-RPC Flue sandbox (cross-script Durable Object)",
        usage: "POST {action:write|read|bash, ...} ?id=<instance>",
      });
    }

    const body = (await request.json().catch(() => ({}))) as Record<string, string>;
    try {
      let result: unknown;
      switch (body.action) {
        case "write":
          await session.writeFile(body.path!, body.content ?? "");
          result = { written: body.path };
          break;
        case "read":
          result = { content: await session.readFile(body.path!) };
          break;
        case "bash":
          result = await session.exec(body.command!);
          break;
        default:
          result = { error: "unknown action" };
      }
      return Response.json({ ok: true, id, result });
    } catch (err) {
      return Response.json(
        { ok: false, error: err instanceof Error ? err.message : String(err) },
        { status: 500 },
      );
    }
  },
};
