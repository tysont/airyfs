// ABOUTME: Resumable, checksummed file upload and download flows for the CLI put/get --resume paths.
// ABOUTME: Streams in 1 MiB chunks, never buffers a whole file, and keeps partial state on failure.

import { createReadStream, createWriteStream } from 'node:fs';
import { open, readFile, rename, rm, stat, writeFile } from 'node:fs/promises';
import { createHash } from 'node:crypto';
import { Readable } from 'node:stream';
import { pipeline } from 'node:stream/promises';
import { AiryFSApiError } from './errors.js';
import type { AiryFSClient } from './client.js';
import type { UploadCompleteResult } from './types.js';

/** Bytes per PATCH/range chunk; matches the Worker's per-chunk bound. */
export const RESUME_CHUNK_BYTES = 1024 * 1024;

export interface TransferProgress {
  transferred: number;
  total: number;
}

/** Stream a file's contents through SHA-256 without holding it all in memory. */
async function sha256File(path: string): Promise<string> {
  const hash = createHash('sha256');
  for await (const chunk of createReadStream(path)) hash.update(chunk as Uint8Array);
  return hash.digest('hex');
}

async function fileSize(path: string): Promise<number | null> {
  try {
    return (await stat(path)).size;
  } catch (error) {
    if (error instanceof Error && 'code' in error && error.code === 'ENOENT') return null;
    throw error;
  }
}

async function pathExists(path: string): Promise<boolean> {
  try {
    await stat(path);
    return true;
  } catch (error) {
    if (error instanceof Error && 'code' in error && error.code === 'ENOENT') return false;
    throw error;
  }
}

// ---------------------------------------------------------------------------
// Upload
// ---------------------------------------------------------------------------

/**
 * Upload `localPath` to `remotePath` resumably. Computes the full-file SHA-256,
 * begins or resumes the server session, then appends 1 MiB chunks from the
 * server's offset, each with its own SHA-256. A single offset conflict per chunk
 * is reconciled with one status GET; a repeat conflict at the same offset fails.
 * Returns the published file metadata.
 */
export async function resumableUpload(
  client: AiryFSClient,
  localPath: string,
  size: number,
  remotePath: string,
  onProgress?: (progress: TransferProgress) => void,
): Promise<UploadCompleteResult> {
  const checksum = await sha256File(localPath);
  const begun = await client.beginUpload(remotePath, size, checksum);
  let offset = begun.offset;
  if (offset > size) {
    throw new Error(`Server upload offset ${offset} exceeds local file size ${size}`);
  }
  onProgress?.({ transferred: offset, total: size });

  const handle = await open(localPath, 'r');
  try {
    let reconciledAt = -1;
    while (offset < size) {
      const length = Math.min(RESUME_CHUNK_BYTES, size - offset);
      const buffer = Buffer.alloc(length);
      const { bytesRead } = await handle.read(buffer, 0, length, offset);
      if (bytesRead !== length) {
        throw new Error(
          `Local file changed during upload: expected ${length} bytes at ${offset}, read ${bytesRead}`,
        );
      }
      const chunkSha = createHash('sha256').update(buffer).digest('hex');
      try {
        const status = await client.appendUpload(remotePath, offset, chunkSha, buffer);
        offset = status.offset;
        reconciledAt = -1;
        onProgress?.({ transferred: offset, total: size });
      } catch (error) {
        // Reconcile a stale offset exactly once per chunk, then retry.
        if (
          error instanceof AiryFSApiError &&
          error.code === 'UPLOAD_OFFSET_MISMATCH' &&
          reconciledAt !== offset
        ) {
          reconciledAt = offset;
          const status = await client.uploadStatus(remotePath);
          offset = status.offset;
          if (offset > size) {
            throw new Error(`Server upload offset ${offset} exceeds local file size ${size}`);
          }
          onProgress?.({ transferred: offset, total: size });
          continue;
        }
        throw error;
      }
    }
  } finally {
    await handle.close();
  }

  return client.completeUpload(remotePath);
}

// ---------------------------------------------------------------------------
// Download
// ---------------------------------------------------------------------------

interface Sidecar {
  inode: string;
  size: number;
}

export interface DownloadResult {
  size: number;
  checksum: string;
  resumed: boolean;
}

async function readSidecar(path: string): Promise<Sidecar | null> {
  let raw: string;
  try {
    raw = await readFile(path, 'utf8');
  } catch (error) {
    if (error instanceof Error && 'code' in error && error.code === 'ENOENT') return null;
    throw error;
  }
  try {
    const parsed = JSON.parse(raw) as Partial<Sidecar>;
    if (typeof parsed.inode === 'string' && typeof parsed.size === 'number') {
      return { inode: parsed.inode, size: parsed.size };
    }
  } catch {
    // Corrupt sidecar: treat as absent so the partial safely restarts.
  }
  return null;
}

/**
 * Download `remotePath` to `localPath` resumably via a `.airyfs-partial` file and a
 * `.airyfs-partial.json` sidecar recording the remote inode and size. Resumes only
 * when the sidecar and partial still match the current remote inode/size,
 * otherwise it safely restarts. Verifies the completed size and both the remote
 * and locally computed SHA-256 before atomically renaming the partial over the
 * destination and deleting the sidecar. A changed inode or checksum mismatch
 * keeps the partial state and fails clearly.
 */
export async function resumableDownload(
  client: AiryFSClient,
  remotePath: string,
  localPath: string,
  options: { force?: boolean },
  onProgress?: (progress: TransferProgress) => void,
): Promise<DownloadResult> {
  if (!options.force && await pathExists(localPath)) {
    throw new Error(`Local path already exists: ${localPath} (use --force to overwrite)`);
  }

  const partial = `${localPath}.airyfs-partial`;
  const sidecarPath = `${partial}.json`;

  const head = await client.headFile(remotePath);
  const remoteSize = Number(head.headers.get('Content-Length') ?? '0');
  const remoteInode = head.headers.get('X-AiryFS-Inode') ?? '';
  if (!Number.isSafeInteger(remoteSize) || remoteSize < 0) {
    throw new Error(`Invalid remote size for ${remotePath}`);
  }

  const sidecar = await readSidecar(sidecarPath);
  const partialSize = await fileSize(partial);

  let offset: number;
  let resumed: boolean;
  if (
    sidecar &&
    sidecar.inode === remoteInode &&
    sidecar.size === remoteSize &&
    partialSize !== null &&
    partialSize <= remoteSize
  ) {
    offset = partialSize;
    resumed = offset > 0;
  } else {
    // Restart the partial safely: the previous attempt no longer matches.
    await rm(partial, { force: true });
    await rm(sidecarPath, { force: true });
    await writeFile(partial, '');
    await writeFile(sidecarPath, JSON.stringify({ inode: remoteInode, size: remoteSize }));
    offset = 0;
    resumed = false;
  }
  onProgress?.({ transferred: offset, total: remoteSize });

  if (offset < remoteSize) {
    let response: Response | null;
    try {
      response = await client.readFile(remotePath, `bytes=${offset}-`);
    } catch (error) {
      // Offset already at the end from the server's view; verify below.
      if (error instanceof AiryFSApiError && error.status === 416) response = null;
      else throw error;
    }
    if (response) {
      const responseInode = response.headers.get('X-AiryFS-Inode') ?? '';
      if (responseInode !== remoteInode) {
        await response.body?.cancel().catch(() => undefined);
        throw new Error(
          `Remote file changed during download (inode ${remoteInode} -> ${responseInode}); partial state kept`,
        );
      }
      if (response.body) {
        await pipeline(
          Readable.fromWeb(response.body as never),
          createWriteStream(partial, { flags: 'a' }),
        );
      }
    }
  }

  const finalSize = await fileSize(partial);
  if (finalSize !== remoteSize) {
    throw new Error(
      `Downloaded size ${finalSize} does not match remote size ${remoteSize}; partial state kept`,
    );
  }
  onProgress?.({ transferred: remoteSize, total: remoteSize });

  const remote = await client.checksum(remotePath);
  if (String(remote.ino) !== remoteInode) {
    throw new Error(
      `Remote file changed during download (inode ${remoteInode} -> ${remote.ino}); partial state kept`,
    );
  }
  const localChecksum = await sha256File(partial);
  if (localChecksum !== remote.checksum) {
    throw new Error(
      `Checksum mismatch for ${remotePath} (${localChecksum} != ${remote.checksum}); partial state kept`,
    );
  }

  await rename(partial, localPath);
  await rm(sidecarPath, { force: true });
  return { size: remoteSize, checksum: remote.checksum, resumed };
}
