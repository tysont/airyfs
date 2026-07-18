// ABOUTME: Tests preview service supervision, port injection, logs, status, and process cleanup.
// ABOUTME: Confirms services run independently from the foreground execution slot.

import assert from 'node:assert/strict';
import { afterEach, describe, it } from 'node:test';
import type { Server } from 'node:http';
import { createExecutionSlot } from './execution-slot.js';
import { createServiceServer } from './service-server.js';

let server: Server | undefined;
afterEach(() => new Promise<void>((resolve) => server?.close(() => resolve()) ?? resolve()));

describe('preview service server', () => {
  it('starts, logs, lists, and stops without consuming the execution slot', async () => {
    const slot = createExecutionSlot();
    server = createServiceServer();
    await new Promise<void>((resolve) => server!.listen(0, '127.0.0.1', resolve));
    const address = server.address();
    if (!address || typeof address === 'string') throw new Error('Service server did not bind');
    const base = `http://127.0.0.1:${address.port}`;
    const started = await json(base, '/services/start', {
      name: 'web', command: 'printf "port=$PORT\\n"; sleep 30', port: 5000, cwd: process.cwd(), env: {},
    });
    assert.equal(started.status, 201);
    assert.equal(slot.active, null);
    await waitFor(async () => ((await getJson(base, '/services/web/logs')).entries as unknown[]).length > 0);
    const logs = await getJson(base, '/services/web/logs');
    assert.equal(Buffer.from((logs.entries as Array<{ data: string }>)[0].data, 'base64').toString(), 'port=5000\n');
    const listed = await getJson(base, '/services') as unknown as Array<{ name: string; running: boolean }>;
    assert.deepEqual(listed.map(({ name, running }) => ({ name, running })), [{ name: 'web', running: true }]);
    assert.equal((await json(base, '/services/stop', { name: 'web' })).status, 200);
    await waitFor(async () => !(await getJson(base, '/services') as unknown as Array<{ running: boolean }>)[0].running);
  });
});

async function json(base: string, path: string, body: unknown): Promise<{ status: number; value: Record<string, unknown> }> {
  const response = await fetch(`${base}${path}`, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) });
  return { status: response.status, value: await response.json() as Record<string, unknown> };
}

async function getJson(base: string, path: string): Promise<Record<string, unknown>> {
  return await (await fetch(`${base}${path}`)).json() as Record<string, unknown>;
}

async function waitFor(predicate: () => Promise<boolean>): Promise<void> {
  const deadline = Date.now() + 5000;
  while (!await predicate()) {
    if (Date.now() >= deadline) throw new Error('Timed out waiting for service state');
    await new Promise((resolve) => setTimeout(resolve, 20));
  }
}
