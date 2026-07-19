// ABOUTME: Supervises long-lived preview service processes independently of foreground execution.
// ABOUTME: Injects allocated ports, captures bounded logs, and terminates complete process groups.

import { spawn, type ChildProcess } from 'node:child_process';
import { createServer, type IncomingMessage, type Server, type ServerResponse } from 'node:http';

export const SERVICE_CONTROL_PORT = 4002;
const MAX_LOG_BYTES = 1024 * 1024;
const KILL_GRACE_MS = 2000;

interface LogEntry { seq: number; stream: 'stdout' | 'stderr'; data: string; timestamp: number; bytes: number }
interface ServiceProcess {
  name: string;
  generation: string;
  port: number;
  child: ChildProcess;
  startedAt: number;
  exitCode: number | null;
  logs: LogEntry[];
  logBytes: number;
  nextSeq: number;
  killTimer?: NodeJS.Timeout;
}

export function createServiceServer(): Server {
  const services = new Map<string, ServiceProcess>();
  return createServer((request, response) => {
    void handle(request, response, services).catch((error) => json(response, 500, { error: error instanceof Error ? error.message : String(error) }));
  });
}

async function handle(request: IncomingMessage, response: ServerResponse, services: Map<string, ServiceProcess>): Promise<void> {
  const url = new URL(request.url ?? '/', 'http://localhost');
  if (request.method === 'GET' && url.pathname === '/services') {
    json(response, 200, [...services.values()].map(status));
    return;
  }
  const logMatch = url.pathname.match(/^\/services\/([^/]+)\/logs$/);
  if (request.method === 'GET' && logMatch) {
    const service = services.get(decodeURIComponent(logMatch[1]));
    if (!service) return json(response, 404, { error: 'Service not found' });
    const requestedGeneration = url.searchParams.get('generation');
    const reset = requestedGeneration !== null && requestedGeneration !== service.generation;
    const after = reset ? 0 : Number(url.searchParams.get('after') ?? 0);
    const earliestSeq = service.logs[0]?.seq ?? null;
    json(response, 200, {
      generation: service.generation,
      reset,
      earliestSeq,
      truncated: !reset && earliestSeq !== null && after < earliestSeq - 1,
      entries: service.logs.filter((entry) => entry.seq > after).map(({ bytes: _, ...entry }) => entry),
    });
    return;
  }
  if (request.method === 'POST' && url.pathname === '/services/start') {
    const body = await readJson(request);
    if (typeof body.name !== 'string' || !body.name || typeof body.command !== 'string' || !body.command) {
      return json(response, 400, { error: 'name and command are required' });
    }
    if (!Number.isSafeInteger(body.port) || (body.port as number) < 1 || (body.port as number) > 65535) {
      return json(response, 400, { error: 'port must be a valid integer' });
    }
    const existing = services.get(body.name);
    if (existing && existing.exitCode === null) return json(response, 200, status(existing));
    const environment = typeof body.env === 'object' && body.env !== null ? body.env as Record<string, unknown> : {};
    if (Object.values(environment).some((value) => typeof value !== 'string')) {
      return json(response, 400, { error: 'env values must be strings' });
    }
    const service = startService(body.name, body.command, body.port as number, typeof body.cwd === 'string' ? body.cwd : '/volume', environment as Record<string, string>);
    services.set(service.name, service);
    json(response, 201, status(service));
    return;
  }
  if (request.method === 'POST' && url.pathname === '/services/stop') {
    const body = await readJson(request);
    const service = typeof body.name === 'string' ? services.get(body.name) : undefined;
    if (!service) return json(response, 404, { error: 'Service not found' });
    terminate(service);
    json(response, 200, { ...status(service), stopping: true });
    return;
  }
  json(response, 404, { error: 'Not found' });
}

function startService(name: string, command: string, port: number, cwd: string, extraEnv: Record<string, string>): ServiceProcess {
  const child = spawn('/bin/sh', ['-c', command], {
    cwd,
    env: {
      ...process.env,
      HOME: '/root',
      PATH: '/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin',
      PORT: String(port),
      ...extraEnv,
    },
    detached: true,
    stdio: ['ignore', 'pipe', 'pipe'],
  });
  const service: ServiceProcess = {
    name, generation: crypto.randomUUID(), port, child, startedAt: Date.now(), exitCode: null, logs: [], logBytes: 0, nextSeq: 1,
  };
  child.stdout?.on('data', (chunk: Buffer) => appendLog(service, 'stdout', chunk));
  child.stderr?.on('data', (chunk: Buffer) => appendLog(service, 'stderr', chunk));
  child.on('close', (code) => {
    service.exitCode = code ?? 1;
    if (service.killTimer) clearTimeout(service.killTimer);
  });
  return service;
}

function appendLog(service: ServiceProcess, stream: LogEntry['stream'], chunk: Buffer): void {
  const entry: LogEntry = {
    seq: service.nextSeq++, stream, data: chunk.toString('base64'), timestamp: Date.now(), bytes: chunk.byteLength,
  };
  service.logs.push(entry);
  service.logBytes += entry.bytes;
  while (service.logBytes > MAX_LOG_BYTES && service.logs.length > 1) service.logBytes -= service.logs.shift()!.bytes;
}

function terminate(service: ServiceProcess): void {
  const pid = service.child.pid;
  if (pid === undefined || service.exitCode !== null) return;
  try { process.kill(-pid, 'SIGTERM'); } catch { return; }
  service.killTimer ??= setTimeout(() => {
    try { process.kill(-pid, 'SIGKILL'); } catch { /* already exited */ }
  }, KILL_GRACE_MS);
  service.killTimer.unref();
}

function status(service: ServiceProcess): Record<string, unknown> {
  return {
    name: service.name,
    port: service.port,
    pid: service.child.pid ?? null,
    running: service.exitCode === null,
    exitCode: service.exitCode,
    startedAt: service.startedAt,
  };
}

async function readJson(request: IncomingMessage): Promise<Record<string, unknown>> {
  let text = '';
  for await (const chunk of request) text += chunk;
  try { return JSON.parse(text) as Record<string, unknown>; } catch { throw new Error('Invalid JSON'); }
}

function json(response: ServerResponse, status: number, value: unknown): void {
  response.writeHead(status, { 'Content-Type': 'application/json' });
  response.end(JSON.stringify(value));
}
