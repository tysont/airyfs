// ABOUTME: Unit tests for the pure snapshots HTTP-routing helpers (access, error mapping, body parsing).
// ABOUTME: Covers required-access scoping, SnapshotError -> HttpError mapping, and optional JSON bodies.

import { describe, expect, it } from 'vitest';
import { HttpError } from '../src/files-api';
import { SnapshotError, SnapshotExistsError, SnapshotNotFoundError } from '../src/snapshots';
import {
  isRootTarget,
  mapSnapshotError,
  readOptionalJsonObject,
  snapshotAccess,
} from '../src/snapshot-routing';

describe('snapshotAccess', () => {
  it('scopes list and create on the collection', () => {
    expect(snapshotAccess('GET', '/')).toEqual({ operation: 'read', paths: ['/'] });
    expect(snapshotAccess('POST', '/')).toEqual({ operation: 'write', paths: ['/'] });
  });

  it('requires read for diff and admin for delete/restore/clone', () => {
    expect(snapshotAccess('GET', '/abc/diff')).toEqual({ operation: 'read', paths: ['/'] });
    expect(snapshotAccess('DELETE', '/abc')).toEqual({ operation: 'admin', paths: ['/'] });
    expect(snapshotAccess('POST', '/abc/restore')).toEqual({ operation: 'admin', paths: ['/'] });
    expect(snapshotAccess('POST', '/abc/clone')).toEqual({ operation: 'admin', paths: ['/'] });
  });

  it('always scopes to the whole volume root', () => {
    for (const path of ['/', '/abc', '/abc/diff', '/abc/restore', '/abc/clone']) {
      expect(snapshotAccess('GET', path).paths).toEqual(['/']);
    }
  });
});

describe('mapSnapshotError', () => {
  it('maps not-found to 404, exists to 409, and other snapshot errors to 400', () => {
    expect(mapSnapshotError(new SnapshotNotFoundError('x'))).toMatchObject({ status: 404, code: 'SNAPSHOT_NOT_FOUND' });
    expect(mapSnapshotError(new SnapshotExistsError('x'))).toMatchObject({ status: 409, code: 'SNAPSHOT_EXISTS' });
    expect(mapSnapshotError(new SnapshotError('INVALID_NAME', 'bad'))).toMatchObject({ status: 400, code: 'INVALID_NAME' });
  });

  it('passes HttpError and unrelated errors through unchanged', () => {
    const http = new HttpError(405, 'METHOD_NOT_ALLOWED', 'nope');
    expect(mapSnapshotError(http)).toBe(http);
    const other = new Error('boom');
    expect(mapSnapshotError(other)).toBe(other);
  });
});

describe('isRootTarget', () => {
  it('recognizes root paths regardless of separators', () => {
    expect(isRootTarget('/')).toBe(true);
    expect(isRootTarget('')).toBe(true);
    expect(isRootTarget('///')).toBe(true);
    expect(isRootTarget('/app')).toBe(false);
    expect(isRootTarget('/a/b')).toBe(false);
  });
});

describe('readOptionalJsonObject', () => {
  const request = (body?: string): Request =>
    new Request('http://localhost', body === undefined ? { method: 'POST' } : { method: 'POST', body });

  it('returns an empty object for an empty body', async () => {
    expect(await readOptionalJsonObject(request())).toEqual({});
    expect(await readOptionalJsonObject(request('   '))).toEqual({});
  });

  it('parses a JSON object body', async () => {
    expect(await readOptionalJsonObject(request('{"name":"nightly","note":"x"}')))
      .toEqual({ name: 'nightly', note: 'x' });
  });

  it('rejects malformed JSON and non-objects', async () => {
    await expect(readOptionalJsonObject(request('{bad'))).rejects.toMatchObject({ status: 400, code: 'INVALID_JSON' });
    await expect(readOptionalJsonObject(request('[1,2]'))).rejects.toMatchObject({ status: 400, code: 'INVALID_JSON' });
    await expect(readOptionalJsonObject(request('"str"'))).rejects.toMatchObject({ status: 400, code: 'INVALID_JSON' });
  });
});
