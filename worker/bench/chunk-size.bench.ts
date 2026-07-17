// ABOUTME: Compares AgentFS sequential I/O across candidate immutable chunk sizes.
// ABOUTME: Measures local SQLite only; deployed FUSE benchmarking remains authoritative.

import { Buffer } from 'buffer';
import Database from 'better-sqlite3';
import { bench, describe } from 'vitest';
import { AgentFS } from 'agentfs-sdk/cloudflare';
import { configureChunkSize, initSchema } from '../src/schema';
import { createTestStorage } from '../test/support/storage';

const PAYLOAD = Buffer.alloc(1024 * 1024, 0x5a);

function createFilesystem(chunkSize: number): AgentFS {
  const storage = createTestStorage(new Database(':memory:'));
  initSchema(storage.sql);
  configureChunkSize(storage.sql, chunkSize);
  return AgentFS.create(storage);
}

describe('1 MiB sequential write and read', () => {
  for (const chunkSize of [4 * 1024, 64 * 1024, 256 * 1024]) {
    const fs = createFilesystem(chunkSize);
    bench(`${chunkSize / 1024} KiB chunks`, async () => {
      await fs.writeFile('/payload.bin', PAYLOAD);
      await fs.readFile('/payload.bin');
    });
  }
});
