// ABOUTME: Runs interactive terminal sessions over a length-prefixed binary TCP protocol.
// ABOUTME: Uses node-pty for terminal semantics, resize handling, signals, and merged output.

import { createServer, type Server, type Socket } from 'node:net';
import { existsSync } from 'node:fs';
import * as pty from 'node-pty';
import type { ActiveCommand, ExecutionSlot } from './execution-slot.js';

export const PTY_PORT = 4001;
export const PTY_STDIN = 0x00;
export const PTY_RESIZE = 0x01;
export const PTY_SIGNAL = 0x02;
export const PTY_STDOUT = 0x10;
export const PTY_EXIT = 0x11;
export const PTY_READY = 0x12;
const MAX_FRAME_BYTES = 1024 * 1024;
const KILL_GRACE_MS = 2000;

export function encodePtyFrame(payload: Uint8Array): Buffer {
  if (payload.byteLength > MAX_FRAME_BYTES) throw new Error('PTY frame too large');
  const frame = Buffer.allocUnsafe(4 + payload.byteLength);
  frame.writeUInt32BE(payload.byteLength, 0);
  frame.set(payload, 4);
  return frame;
}

export interface PtyProcess {
  write(data: string): void;
  resize(cols: number, rows: number): void;
  kill(signal?: string): void;
  onData(listener: (data: string) => void): { dispose(): void };
  onExit(listener: (event: { exitCode: number; signal?: number }) => void): { dispose(): void };
}

export type PtyFactory = (shell: string, args: string[], options: pty.IPtyForkOptions) => PtyProcess;

export function createPtyServer(
  slot: ExecutionSlot,
  cwd: () => string,
  factory: PtyFactory = (shell, args, options) => pty.spawn(shell, args, options),
): Server {
  return createServer((socket) => handleConnection(socket, slot, cwd(), factory));
}

function handleConnection(socket: Socket, slot: ExecutionSlot, cwd: string, factory: PtyFactory): void {
  if (slot.active) {
    socket.destroy(new Error('Another command is already running'));
    return;
  }
  const shell = existsSync('/bin/bash') ? '/bin/bash' : '/bin/sh';
  const env = Object.fromEntries(Object.entries({
    ...process.env,
    HOME: '/root',
    TERM: 'xterm-256color',
    PATH: '/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin',
  }).filter((entry): entry is [string, string] => entry[1] !== undefined));
  let terminal: PtyProcess;
  try {
    terminal = factory(shell, shell.endsWith('bash') ? ['-l'] : [], {
      name: 'xterm-256color', cols: 80, rows: 24, cwd, env,
    });
  } catch (error) {
    socket.destroy(error instanceof Error ? error : new Error(String(error)));
    return;
  }
  let buffered = Buffer.alloc(0);
  let killTimer: NodeJS.Timeout | undefined;
  let finalized = false;

  const terminate = (): void => {
    try { terminal.kill('SIGHUP'); } catch { /* already exited */ }
    killTimer ??= setTimeout(() => {
      try { terminal.kill('SIGKILL'); } catch { /* already exited */ }
    }, KILL_GRACE_MS);
    killTimer.unref();
  };
  const execution: ActiveCommand = { id: `pty-${crypto.randomUUID()}`, terminate };
  slot.active = execution;
  const finalize = (): void => {
    if (finalized) return;
    finalized = true;
    if (slot.active === execution) slot.active = null;
  };

  socket.write(encodePtyFrame(Uint8Array.of(PTY_READY)));
  terminal.onData((data) => {
    const bytes = Buffer.from(data);
    socket.write(encodePtyFrame(Buffer.concat([Buffer.from([PTY_STDOUT]), bytes])));
  });
  terminal.onExit(({ exitCode, signal }) => {
    if (killTimer) clearTimeout(killTimer);
    const payload = Buffer.alloc(6);
    payload[0] = PTY_EXIT;
    payload.writeInt32BE(exitCode, 1);
    payload[5] = signal ?? 0;
    if (!socket.destroyed) socket.end(encodePtyFrame(payload));
    finalize();
  });
  socket.on('data', (chunk) => {
    buffered = Buffer.concat([buffered, chunk]);
    while (buffered.byteLength >= 4) {
      const length = buffered.readUInt32BE(0);
      if (length > MAX_FRAME_BYTES) return socket.destroy(new Error('PTY frame too large'));
      if (buffered.byteLength < 4 + length) return;
      const payload = buffered.subarray(4, 4 + length);
      buffered = buffered.subarray(4 + length);
      handleFrame(terminal, payload);
    }
  });
  socket.on('close', () => {
    if (!finalized) terminate();
  });
  socket.on('error', () => undefined);
}

function handleFrame(terminal: PtyProcess, payload: Uint8Array): void {
  if (payload.byteLength === 0) return;
  if (payload[0] === PTY_STDIN) terminal.write(Buffer.from(payload.subarray(1)).toString());
  else if (payload[0] === PTY_RESIZE && payload.byteLength === 5) {
    const view = new DataView(payload.buffer, payload.byteOffset, payload.byteLength);
    const cols = view.getUint16(1, false);
    const rows = view.getUint16(3, false);
    if (cols > 0 && rows > 0) terminal.resize(cols, rows);
  } else if (payload[0] === PTY_SIGNAL && payload.byteLength > 1) {
    const signal = new TextDecoder().decode(payload.subarray(1));
    if (signal) terminal.kill(signal);
  }
}
