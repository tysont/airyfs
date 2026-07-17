// ABOUTME: Verifies immutable AgentFS volume chunk sizes across boundary writes.
// ABOUTME: Confirms larger chunks reduce SQLite rows without changing file bytes.

import { Buffer } from 'buffer';
import Database from 'better-sqlite3';
import { describe, expect, it } from 'vitest';
import { AgentFS } from 'agentfs-sdk/cloudflare';
import { configureChunkSize, initSchema } from '../src/schema';
import { createTestStorage } from './support/storage';

describe('per-volume chunk size', () => {
  for (const chunkSize of [4 * 1024, 64 * 1024, 256 * 1024]) {
    it(`round-trips boundary sizes with ${chunkSize / 1024} KiB chunks`, async () => {
      const db = new Database(':memory:');
      const storage = createTestStorage(db);
      initSchema(storage.sql);
      configureChunkSize(storage.sql, chunkSize);
      const fs = AgentFS.create(storage);

      for (const size of [chunkSize - 1, chunkSize, chunkSize + 1, chunkSize * 3 + 17]) {
        const payload = Buffer.alloc(size, size % 251);
        await fs.writeFile('/payload.bin', payload);
        expect(await fs.readFile('/payload.bin')).toEqual(payload);

        const rows = db.prepare('SELECT count(*) FROM fs_data').pluck().get() as number;
        expect(rows).toBe(Math.ceil(size / chunkSize));
      }
    });
  }

  it('uses 64 times fewer SQLite rows for 1 MiB at 256 KiB than 4 KiB', async () => {
    const rowCount = async (chunkSize: number): Promise<number> => {
      const db = new Database(':memory:');
      const storage = createTestStorage(db);
      initSchema(storage.sql);
      configureChunkSize(storage.sql, chunkSize);
      const fs = AgentFS.create(storage);
      await fs.writeFile('/payload.bin', Buffer.alloc(1024 * 1024));
      return db.prepare('SELECT count(*) FROM fs_data').pluck().get() as number;
    };

    expect(await rowCount(4096)).toBe(256);
    expect(await rowCount(256 * 1024)).toBe(4);
  });
});
