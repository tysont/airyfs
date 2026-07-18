// ABOUTME: Tests the interactive PTY server over its binary framed TCP protocol.
// ABOUTME: Verifies terminal input/output, resize propagation, exit frames, and slot release.

import { connect } from 'node:net';
import { afterEach, describe, it } from 'node:test';
import assert from 'node:assert/strict';
import type { Server } from 'node:net';
import { createExecutionSlot } from './execution-slot.js';
import { createPtyServer, encodePtyFrame, PTY_EXIT, PTY_READY, PTY_RESIZE, PTY_STDIN, PTY_STDOUT } from './pty-server.js';
import type { PtyProcess } from './pty-server.js';

let server: Server | undefined;
afterEach(() => new Promise<void>((resolve) => server?.close(() => resolve()) ?? resolve()));

describe('PTY server', () => {
  it('runs a resized interactive shell and releases the shared slot', async () => {
    const slot = createExecutionSlot();
    let dataListener = (_data: string): void => undefined;
    let exitListener = (_event: { exitCode: number; signal?: number }): void => undefined;
    let size = '';
    const terminal: PtyProcess = {
      write(data) {
        dataListener(`${size}\r\n${data.includes('pty-ok') ? 'pty-ok' : ''}\r\n`);
        queueMicrotask(() => exitListener({ exitCode: 0 }));
      },
      resize(cols, rows) { size = `${rows} ${cols}`; },
      kill() { exitListener({ exitCode: 129, signal: 1 }); },
      onData(listener) { dataListener = listener; return { dispose() {} }; },
      onExit(listener) { exitListener = listener; return { dispose() {} }; },
    };
    server = createPtyServer(slot, () => process.cwd(), () => terminal);
    await new Promise<void>((resolve) => server!.listen(0, '127.0.0.1', resolve));
    const address = server.address();
    if (!address || typeof address === 'string') throw new Error('PTY server did not bind');
    const socket = connect(address.port, '127.0.0.1');
    await new Promise<void>((resolve, reject) => socket.once('connect', resolve).once('error', reject));

    const frames: Buffer[] = [];
    let buffered = Buffer.alloc(0);
    socket.on('data', (chunk) => {
      buffered = Buffer.concat([buffered, chunk]);
      while (buffered.length >= 4 && buffered.length >= 4 + buffered.readUInt32BE(0)) {
        const length = buffered.readUInt32BE(0);
        frames.push(buffered.subarray(4, 4 + length));
        buffered = buffered.subarray(4 + length);
      }
    });
    await waitFor(() => frames.some((frame) => frame[0] === PTY_READY));
    const resize = Buffer.alloc(5);
    resize[0] = PTY_RESIZE;
    resize.writeUInt16BE(100, 1);
    resize.writeUInt16BE(40, 3);
    socket.write(encodePtyFrame(resize));
    socket.write(encodePtyFrame(Buffer.concat([Buffer.from([PTY_STDIN]), Buffer.from('stty size; printf pty-ok; exit\n')])));
    await waitFor(() => frames.some((frame) => frame[0] === PTY_EXIT));

    const output = Buffer.concat(frames.filter((frame) => frame[0] === PTY_STDOUT).map((frame) => frame.subarray(1))).toString();
    assert.match(output, /40 100/);
    assert.match(output, /pty-ok/);
    assert.equal(slot.active, null);
    socket.destroy();
  });
});

async function waitFor(predicate: () => boolean): Promise<void> {
  const deadline = Date.now() + 5000;
  while (!predicate()) {
    if (Date.now() >= deadline) throw new Error('Timed out waiting for PTY frame');
    await new Promise((resolve) => setTimeout(resolve, 10));
  }
}
