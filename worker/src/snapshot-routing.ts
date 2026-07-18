// ABOUTME: Pure HTTP-routing helpers for the snapshots resource, free of any Container/DO deps.
// ABOUTME: Isolated so authorization, error mapping, and body parsing are unit-testable in isolation.

import type { AccessRequirement } from './auth';
import { HttpError } from './files-api';
import { SnapshotError, SnapshotExistsError, SnapshotNotFoundError } from './snapshots';

/**
 * Required access for a snapshots request. All snapshot operations are scoped to
 * the whole volume ('/'): list/diff need read, create needs write, and
 * restore/delete/clone need admin. The clone route additionally rejects
 * capability identities (enforced in the route handler) so only root or an
 * auth-disabled caller can clone across volumes.
 */
export function snapshotAccess(method: string, path: string): AccessRequirement {
  const segments = path.split('/').filter(Boolean);
  if (segments.length === 0) {
    return { operation: method === 'GET' ? 'read' : 'write', paths: ['/'] };
  }
  if (segments.length >= 2 && segments[1] === 'diff') {
    return { operation: 'read', paths: ['/'] };
  }
  // Item DELETE, restore, and clone are administrative.
  return { operation: 'admin', paths: ['/'] };
}

/** Map a SnapshotError to a stable HTTP status; pass HttpErrors and others through. */
export function mapSnapshotError(error: unknown): unknown {
  if (error instanceof HttpError) return error;
  if (error instanceof SnapshotNotFoundError) return new HttpError(404, error.code, error.message);
  if (error instanceof SnapshotExistsError) return new HttpError(409, error.code, error.message);
  if (error instanceof SnapshotError) return new HttpError(400, error.code, error.message);
  return error;
}

/** True when a tree path normalizes to the volume root. */
export function isRootTarget(path: string): boolean {
  return `/${path.split('/').filter(Boolean).join('/')}` === '/';
}

/** Parse an optional JSON object body; an empty body yields `{}`. */
export async function readOptionalJsonObject(request: Request): Promise<Record<string, unknown>> {
  const text = await request.text();
  if (!text.trim()) return {};
  let parsed: unknown;
  try {
    parsed = JSON.parse(text);
  } catch {
    throw new HttpError(400, 'INVALID_JSON', 'Request body must be valid JSON');
  }
  if (typeof parsed !== 'object' || parsed === null || Array.isArray(parsed)) {
    throw new HttpError(400, 'INVALID_JSON', 'Request body must be a JSON object');
  }
  return parsed as Record<string, unknown>;
}
