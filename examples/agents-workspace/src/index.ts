import { Agent, routeAgentRequest } from "agents";
import {
  AiryFSWorkspace,
  createWorkspaceTools,
  execInFiber,
  replayStashedExec,
  type StashedExec,
  type WorkspaceTool,
} from "@airyfs/agents-toolkit";

interface Env {
  AI: unknown;
  AIRYFS_ENDPOINT?: string;
  AIRYFS_TOKEN?: string;
  WorkspaceAgent: DurableObjectNamespace;
}

const DEFAULT_ENDPOINT = "https://airyfs-int.tyson-s-sandbox.workers.dev";

interface AgentState {
  /** How many workspace actions this agent instance has handled. */
  actions: number;
}

/**
 * An Agents SDK agent whose durable *workspace* is an AiryFS volume, kept
 * separate from the agent's own durable *state* (`this.sql` / `this.setState`).
 *
 * Two independent durable stores, both keyed to the same agent instance name:
 *   - the agent DO's SQLite (conversation/state/schedules), and
 *   - the AiryFS volume `ws-<name>` (files), which scales to zero on its own
 *     and survives this DO being evicted or redeployed.
 *
 * `onRequest` exposes the workspace as JSON actions so the wiring is exercisable
 * without an LLM; the same `workspace()` + `createWorkspaceTools()` feed a real
 * model tool loop when you add a tool-capable provider.
 */
export class WorkspaceAgent extends Agent<Env, AgentState> {
  initialState: AgentState = { actions: 0 };

  private workspace(): AiryFSWorkspace {
    return new AiryFSWorkspace(
      this.env.AIRYFS_ENDPOINT ?? DEFAULT_ENDPOINT,
      `ws-${this.name}`,
      { token: this.env.AIRYFS_TOKEN },
    );
  }

  /** The same tools you would hand a model, available for direct invocation. */
  private tools(): Record<string, WorkspaceTool> {
    return createWorkspaceTools(this.workspace(), { includeArtifacts: true });
  }

  async onRequest(request: Request): Promise<Response> {
    if (request.method !== "POST") {
      return Response.json({ error: "POST a JSON action" }, { status: 405 });
    }
    const body = (await request.json().catch(() => ({}))) as {
      action?: string;
      [k: string]: unknown;
    };
    const action = body.action;

    try {
      const result = await this.dispatch(action, body);
      this.setState({ actions: this.state.actions + 1 });
      return Response.json({ ok: true, actions: this.state.actions, result });
    } catch (err) {
      return Response.json(
        { ok: false, error: err instanceof Error ? err.message : String(err) },
        { status: 500 },
      );
    }
  }

  private async dispatch(action: string | undefined, body: Record<string, unknown>) {
    const ws = this.workspace();
    const tools = this.tools();
    switch (action) {
      case "write":
        return tools.write_file!.execute({ path: body.path, content: body.content } as never);
      case "read":
        return tools.read_file!.execute({ path: body.path } as never);
      case "list":
        return tools.list_dir!.execute({ path: body.path ?? "/" } as never);
      case "bash":
        return tools.bash!.execute({ command: body.command } as never);
      case "publish":
        return tools.publish_site!.execute({ path: body.path ?? "/", spa: false } as never);
      case "share":
        return tools.share_link!.execute({ path: body.path } as never);

      // Durable execution: run a command inside a fiber so it survives eviction,
      // reusing the AiryFS idempotency key on recovery (see onFiberRecovered).
      case "build":
        return this.runFiber("build", async (ctx) => {
          const { result } = await execInFiber(ctx, ws, String(body.command));
          return result;
        });

      // Durable job handoff for work beyond the ~300s exec ceiling: submit, then
      // poll via schedule() rather than blocking this request.
      case "job": {
        const submitted = await ws.runJob(String(body.command));
        await this.schedule(5, "pollJob", { id: submitted.id });
        return { submitted: submitted.id, status: submitted.status, polling: true };
      }

      default:
        return {
          message: "AiryFS-backed Agents SDK workspace",
          actions: [
            "write", "read", "list", "bash", "publish", "share", "build", "job",
          ],
          volume: `ws-${this.name}`,
        };
    }
  }

  /** Scheduled callback: advance a durable job and record the outcome in state. */
  async pollJob(payload: { id: string }) {
    const job = await this.workspace().getJob(payload.id);
    if (job.status === "queued" || job.status === "running") {
      await this.schedule(5, "pollJob", payload); // keep polling, request already returned
      return;
    }
    this.sql`INSERT INTO job_results (id, status, exit_code) VALUES (${job.id}, ${job.status}, ${job.exitCode ?? -1})`;
  }

  /**
   * Durable-execution recovery: if the DO was evicted mid-`build`, replay the
   * stashed command with its original idempotency key so AiryFS returns the
   * existing command's result instead of running it again.
   */
  async onFiberRecovered(ctx: {
    name: string;
    snapshot?: unknown;
  }): Promise<void> {
    if (ctx.name === "build" && ctx.snapshot) {
      await replayStashedExec(this.workspace(), ctx.snapshot as StashedExec);
    }
  }

  async onStart(): Promise<void> {
    this.sql`CREATE TABLE IF NOT EXISTS job_results (id TEXT PRIMARY KEY, status TEXT, exit_code INTEGER)`;
  }
}

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    return (
      (await routeAgentRequest(request, env)) ??
      new Response("Not found", { status: 404 })
    );
  },
};
