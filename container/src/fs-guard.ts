// ABOUTME: Enforces the invariant that the command-server event loop never makes a synchronous fs call on a FUSE path.
// ABOUTME: A sync call under the mount root blocks the loop that hosts the bridge the FUSE daemons need — the wedge root cause.

import {
  mkdirSync as nodeMkdirSync,
  appendFileSync as nodeAppendFileSync,
  readFileSync as nodeReadFileSync,
  writeFileSync as nodeWriteFileSync,
  type PathLike,
} from 'fs';

const MOUNT_ROOT = '/volume';

/**
 * A synchronous fs call on a path *under* the mount root is routed to a FUSE
 * daemon and blocks the event loop that hosts its bridge — the exact deadlock
 * that wedged the container. The mount root itself is allowed (it is created as
 * a plain directory before anything is mounted over it). Any violation is a bug:
 * fail loudly with a stack trace at the call site instead of stalling silently.
 */
function assertNotUnderMount(fn: string, path: PathLike): void {
  const p = typeof path === 'string' ? path : path.toString();
  if (p.startsWith(`${MOUNT_ROOT}/`)) {
    throw new Error(
      `[fs-guard] synchronous ${fn} on FUSE path "${p}" blocks the command-server event loop `
      + `(it hosts the FUSE bridge); use fs/promises instead`,
    );
  }
}

export function mkdirSync(path: PathLike, options?: Parameters<typeof nodeMkdirSync>[1]): ReturnType<typeof nodeMkdirSync> {
  assertNotUnderMount('mkdirSync', path);
  return nodeMkdirSync(path, options);
}

export function appendFileSync(path: Parameters<typeof nodeAppendFileSync>[0], data: Parameters<typeof nodeAppendFileSync>[1]): void {
  if (typeof path === 'string' || path instanceof URL) assertNotUnderMount('appendFileSync', path as PathLike);
  nodeAppendFileSync(path, data);
}

export function readFileSync(path: Parameters<typeof nodeReadFileSync>[0], options: Parameters<typeof nodeReadFileSync>[1]): string | Buffer {
  if (typeof path === 'string' || path instanceof URL) assertNotUnderMount('readFileSync', path as PathLike);
  return nodeReadFileSync(path, options);
}

export function writeFileSync(path: Parameters<typeof nodeWriteFileSync>[0], data: Parameters<typeof nodeWriteFileSync>[1]): void {
  if (typeof path === 'string' || path instanceof URL) assertNotUnderMount('writeFileSync', path as PathLike);
  nodeWriteFileSync(path, data);
}
