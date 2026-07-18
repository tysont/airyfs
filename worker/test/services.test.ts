// ABOUTME: Tests durable preview service definitions, validation, and bounded port allocation.
// ABOUTME: Verifies desired-state toggles and deterministic port reuse without Container compute.

import Database from 'better-sqlite3';
import { beforeEach, describe, expect, it } from 'vitest';
import { initSchema } from '../src/schema';
import { createService, deleteService, listServices, readService, setServiceEnabled } from '../src/services';
import { createTestStorage } from './support/storage';

describe('preview services', () => {
  let storage: ReturnType<typeof createTestStorage>;
  beforeEach(() => {
    storage = createTestStorage(new Database(':memory:'));
    initSchema(storage.sql);
  });

  it('persists definitions and desired state', () => {
    const service = createService(storage.sql, { name: 'web', command: 'npm run dev', cwd: '/app', env: { MODE: 'dev' }, public: true });
    expect(service).toMatchObject({ name: 'web', cwd: '/app', port: 5000, enabled: true, public: true, env: { MODE: 'dev' } });
    expect(setServiceEnabled(storage.sql, 'web', false).enabled).toBe(false);
    expect(readService(storage.sql, 'web').command).toBe('npm run dev');
  });

  it('allocates and reuses the lowest free port', () => {
    createService(storage.sql, { name: 'one', command: 'one' });
    expect(createService(storage.sql, { name: 'two', command: 'two' }).port).toBe(5001);
    deleteService(storage.sql, 'one');
    expect(createService(storage.sql, { name: 'three', command: 'three' }).port).toBe(5000);
    expect(listServices(storage.sql).map((service) => service.name)).toEqual(['three', 'two']);
  });

  it('rejects invalid definitions and exhausted ports', () => {
    expect(() => createService(storage.sql, { name: '../bad', command: 'x' })).toThrow(/name must/);
    for (let index = 0; index < 16; index++) createService(storage.sql, { name: `s${index}`, command: 'x' });
    expect(() => createService(storage.sql, { name: 'overflow', command: 'x' })).toThrow(/allocated/);
  });
});
