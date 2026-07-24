// ABOUTME: HTTP server that manages the Container lifecycle and executes commands.
// ABOUTME: POST /setup starts the bridge, /mount starts FUSE, /exec runs buffered, /exec/stream streams internal SSE.

import { createServer, type IncomingMessage, type Server, type ServerResponse } from 'http';
import { exec, spawn, type ChildProcess } from 'child_process';
import { mkdirSync, appendFileSync } from 'fs';
import { fileURLToPath } from 'url';
import type { Bridge } from './bridge.js';
import { createExecutionSlot, type ExecutionSlot } from './execution-slot.js';
import { createPtyServer, PTY_PORT } from './pty-server.js';
import { createServiceServer, SERVICE_CONTROL_PORT } from './service-server.js';

const PORT = 4000;
const MOUNT_POINT = '/volume';
/** Cap decoded stdin so a single request cannot exhaust container memory. */
const MAX_STDIN_BYTES = 10 * 1024 * 1024;
const MOUNT_POLL_INTERVAL_MS = 1000;
const MOUNT_POLL_MAX_ATTEMPTS = 30;
const EXEC_TIMEOUT_MS = 300_000;
const EXEC_MAX_BUFFER = 10 * 1024 * 1024;
/** Grace between SIGTERM and SIGKILL when terminating a streaming process group. */
const KILL_GRACE_MS = 2000;
/** A later write flushes low-output events through the Container TCP proxy. */
const SSE_HEARTBEAT_INTERVAL_MS = 1000;

/** Approximate exit codes for a process killed by a signal (128 + signal number). */
const SIGNAL_EXIT_CODES: Record<string, number> = {
  SIGHUP: 1,
  SIGINT: 2,
  SIGKILL: 9,
  SIGTERM: 15,
};

/** A single in-flight command; buffered and streaming execution share this one slot. */
/** Terminal and incremental events emitted by the streaming exec endpoint. */
type ExecEvent =
  | { type: 'start'; id: string }
  | { type: 'stdout'; id: string; data: string }
  | { type: 'stderr'; id: string; data: string }
  | { type: 'exit'; id: string; exitCode: number; signal?: string; timedOut?: boolean };

// Surface why the command server exits: a silent process death reads as
// "container not listening" at the DO and triggers a recycle. Logging here
// distinguishes a crash (uncaughtException/unhandledRejection) from a clean exit.
let processInstrumented = false;
function instrumentProcess(): void {
  if (processInstrumented) return;
  processInstrumented = true;
  process.on('uncaughtException', (error) => {
    process.stderr.write(`[fatal] uncaughtException: ${error instanceof Error ? error.stack : error}\n`);
  });
  process.on('unhandledRejection', (reason) => {
    process.stderr.write(`[fatal] unhandledRejection: ${reason instanceof Error ? reason.stack : reason}\n`);
  });
  process.on('exit', (code) => process.stderr.write(`[fatal] process exit code=${code}\n`));
}

function sleep(ms: number): Promise<void> {
  return new Promise((r) => setTimeout(r, ms));
}

function isMountedAt(point: string): Promise<boolean> {
  return new Promise((resolve) => {
    exec(`mountpoint -q ${point}`, (err) => resolve(!err));
  });
}

function isMounted(): Promise<boolean> {
  return isMountedAt(MOUNT_POINT);
}

/** Environment for user commands; identical for buffered and streaming execution. */
function execEnv(): NodeJS.ProcessEnv {
  return {
    ...process.env,
    HOME: '/root',
    PATH: '/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin',
  };
}

async function readBody(req: IncomingMessage): Promise<string> {
  let body = '';
  for await (const chunk of req) body += chunk;
  return body;
}

function jsonResponse(res: ServerResponse, status: number, value: unknown, headers?: Record<string, string>): void {
  res.writeHead(status, { 'Content-Type': 'application/json', ...headers });
  res.end(JSON.stringify(value));
}

/** Signals a rejected stdin payload so the caller can answer 400 instead of 500. */
class StdinError extends Error {}

/**
 * Decode the optional base64 `stdin` field. Returns null when absent so callers
 * can still close stdin (EOF) without feeding any bytes.
 */
function decodeStdin(value: unknown): Buffer | null {
  if (value === undefined || value === null) return null;
  if (typeof value !== 'string') throw new StdinError('"stdin" must be a base64 string when provided');
  const decoded = Buffer.from(value, 'base64');
  // Buffer.from silently drops invalid input; re-encoding catches non-base64.
  if (decoded.toString('base64').replace(/=+$/, '') !== value.replace(/=+$/, '')) {
    throw new StdinError('"stdin" must be canonical base64');
  }
  if (decoded.byteLength > MAX_STDIN_BYTES) {
    throw new StdinError(`stdin exceeds ${MAX_STDIN_BYTES} bytes`);
  }
  return decoded;
}

/** Feed optional bytes to a child's stdin then close it so readers see EOF. */
function writeStdin(child: ChildProcess, data: Buffer | null): void {
  const stdin = child.stdin;
  if (!stdin) return;
  // A broken pipe (command never read stdin and already exited) is harmless.
  stdin.on('error', () => { /* ignore EPIPE */ });
  if (data && data.byteLength > 0) stdin.end(data);
  else stdin.end();
}

/**
 * Build the command server without listening. The caller decides when (and
 * whether) to bind a port, which keeps the module importable from tests.
 */
export function createCommandServer(slot: ExecutionSlot = createExecutionSlot()): Server {
  instrumentProcess();
  let cwd = '/tmp';
  let bridgeStarted = false;
  let bridge: Bridge | null = null;
  let fuseProcess: ChildProcess | null = null;
  let fuseExitCode: number | null = null;
  const guestProcesses: ChildProcess[] = [];
  // Exactly one command runs at a time across buffered and streaming endpoints.

  /** Check whether the FUSE daemon is alive. Returns error message if dead. */
  function fuseDaemonError(): string | null {
    if (!fuseProcess) return 'FUSE daemon not started';
    if (fuseExitCode !== null) return `FUSE daemon exited with code ${fuseExitCode}`;
    return null;
  }

  /**
   * Graft guest volumes over their stub directories, parent-first, fully isolated
   * from the primary command server. Every failure mode — spawn error, crash,
   * hang, throw — is contained so a guest can only degrade its own subtree.
   */
  async function mountGuests(
    guestMounts: Array<{ mountpoint: string; targetVolume: string; dataHttpPort: number; invalidationHttpPort: number; authToken: string }>,
  ): Promise<void> {
    const { buildGuestMountCommand } = await import('./mounts.js');
    for (const guest of guestMounts) {
      try {
        const guestPoint = `${MOUNT_POINT}${guest.mountpoint}`;
        const logPath = `/tmp/guest${guest.mountpoint.replace(/\//g, '_')}.log`;
        const append = (text: string): void => { try { appendFileSync(logPath, text); } catch { /* ignore */ } };
        try { mkdirSync(guestPoint, { recursive: true }); } catch { /* exists via primary FS */ }
        const command = buildGuestMountCommand(guest);
        append(`$ ${command}\n`);
        const child = exec(command, { env: process.env });
        guestProcesses.push(child);
        child.stdout?.on('data', (d: string) => append(String(d)));
        child.stderr?.on('data', (d: string) => append(String(d)));
        child.on('error', (err) => append(`[spawn error] ${err}\n`));
        child.on('exit', (code) => append(`[exit ${code}]\n`));
        let mounted = false;
        for (let i = 0; i < MOUNT_POLL_MAX_ATTEMPTS; i++) {
          await sleep(MOUNT_POLL_INTERVAL_MS);
          mounted = await isMountedAt(guestPoint);
          if (mounted) break;
        }
        if (!mounted) {
          // A registered-but-unserved FUSE mount blocks every access; tear it
          // down lazily so the stub stays usable rather than hanging.
          try { child.kill('SIGTERM'); } catch { /* already gone */ }
          exec(`fusermount -u -z ${guestPoint} 2>/dev/null || umount -l ${guestPoint} 2>/dev/null`);
          append('[not mounted after poll; torn down]\n');
        } else {
          append('[mounted]\n');
        }
      } catch (err) {
        process.stderr.write(`[guest-mount] ${guest.mountpoint} error: ${err}\n`);
      }
    }
  }

  /** Reject a request when a command is already running, matching /exec semantics. */
  function rejectIfBusy(res: ServerResponse): boolean {
    if (slot.active) {
      jsonResponse(res, 503, { error: 'Another command is already running' }, { 'Retry-After': '1' });
      return true;
    }
    return false;
  }

  /** Reject a request when execution requires FUSE but the daemon is unavailable. */
  function rejectIfFuseUnavailable(res: ServerResponse): boolean {
    const fuseErr = fuseDaemonError();
    if (cwd === MOUNT_POINT && fuseErr) {
      jsonResponse(res, 503, { error: `FUSE unavailable: ${fuseErr}` });
      return true;
    }
    return false;
  }

  function handleExec(req: IncomingMessage, res: ServerResponse): void {
    void (async () => {
      if (rejectIfBusy(res)) return;
      if (rejectIfFuseUnavailable(res)) return;

      const body = await readBody(req);
      let command: string;
      let stdin: Buffer | null;
      try {
        const parsed = JSON.parse(body);
        command = parsed.command;
        if (typeof command !== 'string' || !command.trim()) {
          jsonResponse(res, 400, { error: 'Missing "command" string in request body' });
          return;
        }
        stdin = decodeStdin(parsed.stdin);
      } catch (err) {
        if (err instanceof StdinError) {
          jsonResponse(res, 400, { error: err.message });
          return;
        }
        jsonResponse(res, 400, { error: 'Invalid JSON in request body' });
        return;
      }

      const child = exec(command, {
        cwd,
        timeout: EXEC_TIMEOUT_MS,
        env: execEnv(),
        maxBuffer: EXEC_MAX_BUFFER,
      }, (error, stdout, stderr) => {
        // Only clear the slot if this command still owns it; a late callback must
        // never release a slot a newer command has taken.
        if (slot.active === execution) slot.active = null;
        jsonResponse(res, 200, {
          exitCode: error ? ((error as { code?: number }).code ?? 1) : 0,
          stdout: stdout || '',
          stderr: stderr || '',
        });
      });

      const execution = {
        id: `buffered-${Date.now()}`,
        terminate: () => { try { child.kill('SIGTERM'); } catch { /* already gone */ } },
      };
      slot.active = execution;
      // Always close stdin so commands that read it observe EOF instead of hanging
      // until the exec timeout; feed the supplied bytes first when present.
      writeStdin(child, stdin);
    })();
  }

  function handleExecStream(req: IncomingMessage, res: ServerResponse): void {
    void (async () => {
      if (rejectIfBusy(res)) return;
      if (rejectIfFuseUnavailable(res)) return;

      const body = await readBody(req);
      let command: string;
      let requestedId: unknown;
      let stdin: Buffer | null;
      try {
        const parsed = JSON.parse(body);
        command = parsed.command;
        requestedId = parsed.id;
        if (typeof command !== 'string' || !command.trim()) {
          jsonResponse(res, 400, { error: 'Missing "command" string in request body' });
          return;
        }
        if (requestedId !== undefined && typeof requestedId !== 'string') {
          jsonResponse(res, 400, { error: '"id" must be a string when provided' });
          return;
        }
        stdin = decodeStdin(parsed.stdin);
      } catch (err) {
        if (err instanceof StdinError) {
          jsonResponse(res, 400, { error: err.message });
          return;
        }
        jsonResponse(res, 400, { error: 'Invalid JSON in request body' });
        return;
      }

      const id = typeof requestedId === 'string' && requestedId.length > 0
        ? requestedId
        : `exec-${Date.now()}-${Math.random().toString(36).slice(2, 10)}`;

      // Own its own process group so cancellation can signal descendants too.
      const child = spawn('/bin/sh', ['-c', command], {
        cwd,
        env: execEnv(),
        detached: true,
        stdio: ['pipe', 'pipe', 'pipe'],
      });
      // Close stdin (after feeding supplied bytes) so readers see EOF promptly.
      writeStdin(child, stdin);

      let timedOut = false;
      let finalized = false;
      let killTimer: NodeJS.Timeout | null = null;
      let heartbeat: NodeJS.Timeout | null = null;

      const writeEvent = (event: ExecEvent): void => {
        if (!res.writableEnded) res.write(`data: ${JSON.stringify(event)}\n\n`);
      };

      const terminate = (): void => {
        const pid = child.pid;
        if (pid === undefined) return;
        try { process.kill(-pid, 'SIGTERM'); } catch { /* already gone */ }
        if (!killTimer) {
          killTimer = setTimeout(() => {
            try { process.kill(-pid, 'SIGKILL'); } catch { /* already gone */ }
            killTimer = null;
          }, KILL_GRACE_MS);
          killTimer.unref();
        }
      };

      const timeout = setTimeout(() => {
        timedOut = true;
        terminate();
      }, EXEC_TIMEOUT_MS);

      // Cleanup runs exactly once and only releases the slot it still owns.
      const finalize = (): void => {
        if (finalized) return;
        finalized = true;
        clearTimeout(timeout);
        if (heartbeat) clearInterval(heartbeat);
        // If termination started, leave the unref'ed escalation timer alive so
        // descendants that ignored SIGTERM are still killed after the shell exits.
        res.off('close', onResponseClose);
        if (slot.active === execution) slot.active = null;
      };

      function onResponseClose(): void {
        // Client disconnected (or the stream errored) before the command finished:
        // terminate the process group. The exit handler will finalize the slot.
        if (!res.writableEnded) terminate();
      }

      const execution = { id, terminate };
      slot.active = execution;
      res.on('close', onResponseClose);

      res.writeHead(200, {
        // Valid SSE framing makes the Container proxy flush low-volume commands.
        // The Worker translates these data frames back to public NDJSON.
        'Content-Type': 'text/event-stream',
        'Cache-Control': 'no-cache',
        'X-Accel-Buffering': 'no',
      });
      res.flushHeaders();
      writeEvent({ type: 'start', id });
      heartbeat = setInterval(() => {
        if (!res.writableEnded) res.write('event: heartbeat\ndata: {}\n\n');
      }, SSE_HEARTBEAT_INTERVAL_MS);
      heartbeat.unref();

      child.stdout?.on('data', (chunk: Buffer) => {
        writeEvent({ type: 'stdout', id, data: chunk.toString('base64') });
      });
      child.stderr?.on('data', (chunk: Buffer) => {
        writeEvent({ type: 'stderr', id, data: chunk.toString('base64') });
      });
      child.on('error', () => {
        finalize();
        writeEvent({ type: 'exit', id, exitCode: 1 });
        if (!res.writableEnded) res.end();
      });
      child.on('close', (code, signal) => {
        finalize();
        const exitCode = code ?? (signal ? 128 + (SIGNAL_EXIT_CODES[signal] ?? 0) : 1);
        writeEvent({
          type: 'exit',
          id,
          exitCode,
          ...(signal ? { signal } : {}),
          ...(timedOut ? { timedOut: true } : {}),
        });
        if (!res.writableEnded) res.end();
      });
    })();
  }

  function handleExecCancel(req: IncomingMessage, res: ServerResponse): void {
    void (async () => {
      const body = await readBody(req);
      let id: unknown;
      try {
        id = JSON.parse(body).id;
      } catch {
        jsonResponse(res, 400, { error: 'Invalid JSON in request body' });
        return;
      }
      if (typeof id !== 'string' || !id) {
        jsonResponse(res, 400, { error: 'Missing "id" string in request body' });
        return;
      }
      // Only cancel the command that still owns the slot. A stale id for a command
      // that already finished must never terminate a newer command.
      const canceled = slot.active !== null && slot.active.id === id;
      if (canceled) slot.active!.terminate();
      jsonResponse(res, 200, { ok: true, canceled });
    })();
  }

  return createServer(async (req, res) => {
    const pathname = new URL(req.url ?? '/', 'http://localhost').pathname;
    if (req.method === 'GET' && pathname === '/ping') {
      jsonResponse(res, 200, { status: 'ok' });
      return;
    }

    if (req.method === 'GET' && pathname === '/health') {
      const mounted = await isMounted();
      if (mounted && cwd !== MOUNT_POINT) cwd = MOUNT_POINT;
      const bridgeStatus = bridge?.data.status() ?? { connected: false, pending: 0, queued: 0, admitted: 0 };
      jsonResponse(res, 200, {
        status: 'ok',
        bridgeStarted,
        bridgeConnected: bridgeStatus.connected,
        bridgePending: bridgeStatus.pending,
        bridgeQueued: bridgeStatus.queued,
        bridgeAdmitted: bridgeStatus.admitted,
        fuseMounted: mounted,
        fuseExitCode,
        cwd,
        processMemory: process.memoryUsage(),
        processResources: process.resourceUsage(),
      });
      return;
    }

    if (req.method === 'POST' && req.url === '/setup') {
      // Always drain the request body, even on the already-started fast path, so
      // the Container HTTP proxy does not stall waiting for the body to be read.
      const raw = await readBody(req);
      if (bridgeStarted) {
        jsonResponse(res, 200, { ok: true, guests: bridge?.guests.length ?? 0 });
        return;
      }
      try {
        const { startBridge } = await import('./bridge.js');
        const parsed = raw ? JSON.parse(raw) as { guestChannels?: unknown } : {};
        const guestChannels = Array.isArray(parsed.guestChannels) ? parsed.guestChannels : [];
        bridge = await startBridge(guestChannels);
        bridgeStarted = true;
        jsonResponse(res, 200, { ok: true, guests: bridge.guests.length });
      } catch (err) {
        jsonResponse(res, 500, { ok: false, error: `Bridge startup failed: ${err instanceof Error ? err.message : err}` });
      }
      return;
    }

    if (req.method === 'POST' && req.url === '/mount') {
      const rawMount = await readBody(req);
      if (fuseProcess && fuseExitCode === null) {
        const mounted = await isMounted();
        res.writeHead(mounted ? 200 : 409, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ ok: mounted, mounted, error: mounted ? undefined : 'Mount already in progress' }));
        return;
      }

      try { mkdirSync(MOUNT_POINT, { recursive: true }); } catch { /* exists */ }

      const { buildPrimaryMountCommand, orderMountsByDepth } = await import('./mounts.js');
      const mountBody = rawMount ? JSON.parse(rawMount) as { mounts?: unknown; unavailableMounts?: unknown } : {};
      const guestMounts = orderMountsByDepth(
        Array.isArray(mountBody.mounts) ? mountBody.mounts as Array<{ mountpoint: string; targetVolume: string; dataHttpPort: number; invalidationHttpPort: number; authToken: string }> : [],
      );
      const unavailableMounts = Array.isArray(mountBody.unavailableMounts)
        ? (mountBody.unavailableMounts as unknown[]).filter((m): m is string => typeof m === 'string')
        : [];

      fuseExitCode = null;
      const child = exec(buildPrimaryMountCommand(), { env: process.env });
      fuseProcess = child;

      child.stdout?.on('data', (d: string) => process.stdout.write(`[fuse] ${d}`));
      child.stderr?.on('data', (d: string) => process.stderr.write(`[fuse] ${d}`));
      child.on('exit', (code) => {
        if (fuseProcess === child) fuseExitCode = code ?? 1;
      });

      let mounted = false;
      for (let i = 0; i < MOUNT_POLL_MAX_ATTEMPTS; i++) {
        await sleep(MOUNT_POLL_INTERVAL_MS);
        if (fuseExitCode !== null) {
          jsonResponse(res, 500, { ok: false, error: `agentfs exited with code ${fuseExitCode}` });
          return;
        }
        mounted = await isMounted();
        if (mounted) break;
      }

      if (!mounted) {
        child.kill('SIGTERM');
        if (fuseProcess === child) {
          fuseProcess = null;
          fuseExitCode ??= 1;
        }
        jsonResponse(res, 504, { ok: false, mounted: false, error: 'FUSE mount did not complete within 30 seconds' });
        return;
      }

      // Mounts whose guest FUSE is not wired this run keep the DO-written
      // AIRYFS-MOUNT-UNAVAILABLE.txt marker in their stub. Make the stub
      // read-only (a kernel-only bind remount — no daemon, so it does not touch
      // the guest-daemon path that wedges the container) so a blind write in
      // exec fails with EROFS instead of landing in shadowed local state.
      for (const mountpoint of unavailableMounts) {
        const stub = `${MOUNT_POINT}${mountpoint}`;
        exec(
          `mount --bind ${stub} ${stub} && mount -o remount,bind,ro ${stub} ${stub}`,
          (err, stdout, stderr) => {
            process.stderr.write(`[unavailable-mount] seal ${stub}: ${err ? `ERR ${err.message} ${stderr}` : 'ok'}\n`);
          },
        );
      }

      // The primary mount is live: report success immediately. Guest mounts are
      // grafted in a fully isolated background task so a guest that hangs, exits,
      // or throws can never block or crash the primary command server.
      cwd = MOUNT_POINT;
      jsonResponse(res, 200, { ok: true, mounted: true, guests: guestMounts.map((g) => g.mountpoint) });
      void mountGuests(guestMounts).catch((err) => {
        process.stderr.write(`[guest-mount] background task failed: ${err instanceof Error ? err.stack : err}\n`);
      });
      return;
    }

    if (req.method === 'POST' && req.url === '/exec') {
      handleExec(req, res);
      return;
    }

    if (req.method === 'POST' && req.url === '/exec/stream') {
      handleExecStream(req, res);
      return;
    }

    if (req.method === 'POST' && req.url === '/exec/cancel') {
      handleExecCancel(req, res);
      return;
    }

    jsonResponse(res, 404, { error: 'Not found' });
  });
}

// Start listening only when executed directly, never when imported by a test.
if (process.argv[1] && fileURLToPath(import.meta.url) === process.argv[1]) {
  const slot = createExecutionSlot();
  const server = createCommandServer(slot);
  server.listen(PORT, '0.0.0.0');
  createPtyServer(slot, () => MOUNT_POINT).listen(PTY_PORT, '0.0.0.0');
  createServiceServer().listen(SERVICE_CONTROL_PORT, '0.0.0.0');
}
