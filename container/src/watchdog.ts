// ABOUTME: Separate-process /proc scraper so a wedged command server can be diagnosed from outside.
// ABOUTME: Listens on its own port; the DO scrapes it via getTcpPort even when port 4000 is unresponsive.

import { createServer } from 'http';
import { readFileSync, readdirSync } from 'fs';

export const WATCHDOG_PORT = 4009;

/** Read a /proc file, returning null when the sandbox does not expose it. */
function readProc(path: string): string | null {
  try {
    return readFileSync(path, 'utf8').trim();
  } catch {
    return null;
  }
}

/** State letter (R/S/D/Z...) from /proc/<pid>/status; 'D' is uninterruptible I/O — the wedge signature. */
function procState(pid: string): string | null {
  const status = readProc(`/proc/${pid}/status`);
  if (!status) return null;
  const line = status.split('\n').find((entry) => entry.startsWith('State:'));
  return line ? line.slice('State:'.length).trim() : null;
}

/** Per-thread kernel stacks when the sandbox exposes them (often it does not). */
function threadStacks(pid: string): Record<string, string> {
  const stacks: Record<string, string> = {};
  try {
    for (const tid of readdirSync(`/proc/${pid}/task`)) {
      const stack = readProc(`/proc/${pid}/task/${tid}/stack`);
      if (stack) stacks[tid] = stack;
    }
  } catch { /* no task dir exposed */ }
  return stacks;
}

interface ProcInfo {
  pid: string;
  comm: string | null;
  state: string | null;
  wchan: string | null;
  syscall: string | null;
  stacks: Record<string, string>;
}

/** Snapshot every process the sandbox exposes, focused on the wedge discriminators. */
export function dumpProcesses(): ProcInfo[] {
  const out: ProcInfo[] = [];
  let pids: string[];
  try {
    pids = readdirSync('/proc').filter((entry) => /^[0-9]+$/.test(entry));
  } catch {
    return out;
  }
  for (const pid of pids) {
    const comm = readProc(`/proc/${pid}/comm`);
    // Focus on the processes that matter: node (command server) and agentfs daemons.
    if (comm && !/node|agentfs/.test(comm)) continue;
    out.push({
      pid,
      comm,
      state: procState(pid),
      wchan: readProc(`/proc/${pid}/wchan`),
      syscall: readProc(`/proc/${pid}/syscall`),
      stacks: threadStacks(pid),
    });
  }
  return out;
}

export function createWatchdogServer() {
  return createServer((req, res) => {
    if (req.url === '/procdump') {
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ at: Date.now(), processes: dumpProcesses() }));
      return;
    }
    res.writeHead(200, { 'Content-Type': 'text/plain' });
    res.end('watchdog');
  });
}

// Run standalone: a separate OS process with its own event loop that survives a
// wedge of the command-server process.
if (process.argv[1] && process.argv[1].endsWith('watchdog.js')) {
  createWatchdogServer().listen(WATCHDOG_PORT, '0.0.0.0', () => {
    process.stdout.write(`[watchdog] listening on ${WATCHDOG_PORT}\n`);
  });
}
