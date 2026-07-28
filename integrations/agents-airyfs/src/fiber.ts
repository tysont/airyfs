import type { AiryFSWorkspace, ExecOutcome } from "./workspace.js";

/**
 * Structural view of the Agents SDK `FiberContext` (the argument to
 * `runFiber(name, (ctx) => ...)`). We only need `stash`, so we type it
 * structurally to avoid a hard `agents` dependency. `stash(data)` performs a
 * synchronous, durable checkpoint write; on DO eviction the last stashed value
 * is handed back to the agent's `onFiberRecovered(ctx)` hook.
 */
export interface AgentFiberContext {
  stash(data: unknown): void;
}

/** The checkpoint we stash so a recovered fiber can replay the exact command. */
export interface StashedExec {
  idempotencyKey: string;
  command: string;
  timeoutMs?: number;
}

/**
 * Run a command durably from inside `this.runFiber(...)`, composing the Agents
 * SDK's durable execution with AiryFS's idempotent exec.
 *
 * We mint one idempotency key, checkpoint it with `ctx.stash(...)`, then run the
 * command under that key. If the Durable Object is evicted mid-exec, the agent's
 * `onFiberRecovered(ctx)` hook receives the stashed {@link StashedExec}; passing
 * it to {@link replayStashedExec} re-issues the command with the *same* key, so
 * AiryFS recovers the already-running (or finished) command and replays its
 * persisted output instead of running it twice.
 *
 * @example
 * async build(cmd: string) {
 *   return this.runFiber("build-and-test", async (ctx) => {
 *     const { stashed, result } = await execInFiber(ctx, this.workspace(), cmd);
 *     return result; // stashed is what onFiberRecovered() would replay
 *   });
 * }
 */
export async function execInFiber(
  ctx: AgentFiberContext,
  workspace: AiryFSWorkspace,
  command: string,
  options: { timeoutMs?: number } = {},
): Promise<{ stashed: StashedExec; result: ExecOutcome }> {
  const stashed: StashedExec = {
    idempotencyKey: crypto.randomUUID(),
    command,
    timeoutMs: options.timeoutMs,
  };
  ctx.stash(stashed);
  const result = await workspace.exec(command, {
    idempotencyKey: stashed.idempotencyKey,
    timeoutMs: stashed.timeoutMs,
  });
  return { stashed, result };
}

/**
 * Re-issue a stashed command with its original idempotency key. Call from
 * `onFiberRecovered(ctx)` with the recovered snapshot to finish work that was
 * interrupted, without re-running a command that may already have completed.
 */
export async function replayStashedExec(
  workspace: AiryFSWorkspace,
  stashed: StashedExec,
): Promise<ExecOutcome> {
  return workspace.exec(stashed.command, {
    idempotencyKey: stashed.idempotencyKey,
    timeoutMs: stashed.timeoutMs,
  });
}
