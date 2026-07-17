// ABOUTME: Resolves local CLI path arguments into absolute remote volume paths.
// ABOUTME: Encodes each path segment independently for use in v1 resource URLs.

import { posix } from 'node:path';

export function resolveRemotePath(cwd: string, input = '.'): string {
  return posix.resolve('/', cwd, input || '.');
}

export function encodeRemotePath(path: string): string {
  const normalized = resolveRemotePath('/', path);
  if (normalized === '/') return '';
  return normalized.split('/').filter(Boolean).map(encodeURIComponent).join('/');
}

export function remoteBasename(path: string): string {
  return posix.basename(resolveRemotePath('/', path));
}

export function remoteDirname(path: string): string {
  return posix.dirname(resolveRemotePath('/', path));
}
