// ABOUTME: Reusable streaming SHA-256 for a volume path, hashed in bounded chunks.
// ABOUTME: Never buffers a whole file; used by the checksum operation and upload completion.

import { createHash } from 'node:crypto';
import type { FileSystem } from 'agentfs-sdk/cloudflare';
// Type-only import (erased at runtime) keeps this module free of a value cycle
// with files-api, which imports sha256Path for the checksum operation.
import type { VolumeAccessCoordinator } from './files-api';

const HASH_CHUNK_SIZE = 256 * 1024;

export interface ChecksumResult {
  algorithm: 'sha256';
  checksum: string;
  size: number;
  ino: number;
}

interface ErrnoLike extends Error {
  code?: string;
  path?: string;
}

function notAFile(path: string, isDirectory: boolean): ErrnoLike {
  const code = isDirectory ? 'EISDIR' : 'EINVAL';
  const error = new Error(
    `${code}: checksum requires a regular file, got '${path}'`
  ) as ErrnoLike;
  error.code = code;
  error.path = path;
  return error;
}

/**
 * Compute the lowercase-hex SHA-256 of a regular file, reading it in bounded
 * chunks so no whole-file buffer is ever held. When `access` is provided a path
 * read lock is held for the hash; callers that already hold a lock (for example
 * upload completion, which hashes the temp file under its write lock) omit it.
 */
export async function sha256Path(
  fs: FileSystem,
  path: string,
  access?: VolumeAccessCoordinator
): Promise<ChecksumResult> {
  const release = access ? await access.acquireRead(path) : () => undefined;
  try {
    const stats = await fs.stat(path);
    if (!stats.isFile()) throw notAFile(path, stats.isDirectory());

    const hash = createHash('sha256');
    if (stats.size > 0) {
      const handle = await fs.open(path);
      let offset = 0;
      while (offset < stats.size) {
        const chunk = await handle.pread(offset, Math.min(HASH_CHUNK_SIZE, stats.size - offset));
        if (chunk.byteLength === 0) {
          throw new Error(`File shrank while it was being checksummed: ${path}`);
        }
        hash.update(chunk);
        offset += chunk.byteLength;
      }
    }
    return { algorithm: 'sha256', checksum: hash.digest('hex'), size: stats.size, ino: stats.ino };
  } finally {
    release();
  }
}
