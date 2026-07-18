// ABOUTME: Coordinates the single command slot shared by buffered, streaming, and PTY execution.
// ABOUTME: Ownership checks prevent stale completion callbacks from releasing newer commands.

export interface ActiveCommand {
  id: string;
  terminate: () => void;
}

export interface ExecutionSlot {
  active: ActiveCommand | null;
}

export function createExecutionSlot(): ExecutionSlot {
  return { active: null };
}
