// ABOUTME: Immutable SHA-256-addressed assets stored under AiryFS's hidden system subtree.
// ABOUTME: Streams to a temporary file and verifies the digest before atomic publication.

import type { FileSystem } from 'agentfs-sdk/cloudflare';
import { sha256Path } from './checksum';
import { fileResponse, HttpError, VolumeAccessCoordinator, writeFileStream } from './files-api';

const ASSET_ROOT = '/.airyfs/assets/sha256';

export interface AssetInfo {
  algorithm: 'sha256';
  checksum: string;
  size: number;
  created: boolean;
}

export function validateAssetHash(value: string): string {
  const hash = value.toLowerCase();
  if (!/^[0-9a-f]{64}$/.test(hash)) {
    throw new HttpError(400, 'INVALID_ASSET_HASH', 'Asset id must be a 64-character SHA-256 hex digest');
  }
  return hash;
}

export function assetPath(hash: string): string {
  return `${ASSET_ROOT}/${validateAssetHash(hash)}`;
}

async function ensureDirectory(fs: FileSystem, path: string): Promise<void> {
  try {
    await fs.mkdir(path);
  } catch (error) {
    if (!(error instanceof Error && 'code' in error && error.code === 'EEXIST')) throw error;
  }
}

async function ensureAssetRoot(fs: FileSystem): Promise<void> {
  await ensureDirectory(fs, '/.airyfs');
  await ensureDirectory(fs, '/.airyfs/assets');
  await ensureDirectory(fs, ASSET_ROOT);
}

async function exists(fs: FileSystem, path: string): Promise<boolean> {
  try {
    return (await fs.stat(path)).isFile();
  } catch (error) {
    if (error instanceof Error && 'code' in error && error.code === 'ENOENT') return false;
    throw error;
  }
}

export async function putAsset(
  fs: FileSystem,
  access: VolumeAccessCoordinator,
  rawHash: string,
  body: ReadableStream<Uint8Array> | null,
): Promise<AssetInfo> {
  const checksum = validateAssetHash(rawHash);
  const target = assetPath(checksum);
  await ensureAssetRoot(fs);
  if (await exists(fs, target)) {
    const stats = await fs.stat(target);
    return { algorithm: 'sha256', checksum, size: stats.size, created: false };
  }

  const temporary = `${ASSET_ROOT}/.upload-${crypto.randomUUID()}`;
  try {
    await writeFileStream(fs, temporary, body, access);
    const actual = await sha256Path(fs, temporary, access);
    if (actual.checksum !== checksum) {
      throw new HttpError(
        409,
        'ASSET_CHECKSUM_MISMATCH',
        `Uploaded content SHA-256 ${actual.checksum} does not match asset id ${checksum}`,
      );
    }

    const release = await access.acquireWrite([temporary, target]);
    try {
      if (await exists(fs, target)) {
        await fs.rm(temporary, { force: true });
        const stats = await fs.stat(target);
        return { algorithm: 'sha256', checksum, size: stats.size, created: false };
      }
      await fs.rename(temporary, target);
      return { algorithm: 'sha256', checksum, size: actual.size, created: true };
    } finally {
      release();
    }
  } catch (error) {
    await fs.rm(temporary, { force: true }).catch(() => undefined);
    throw error;
  }
}

export async function getAsset(
  fs: FileSystem,
  access: VolumeAccessCoordinator,
  hash: string,
  request: Request,
  versionForInode?: (ino: number) => number,
): Promise<Response> {
  const response = await fileResponse(fs, assetPath(hash), request, access, versionForInode);
  const headers = new Headers(response.headers);
  headers.set('Cache-Control', 'public, max-age=31536000, immutable');
  return new Response(response.body, { status: response.status, headers });
}
