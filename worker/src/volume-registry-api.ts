// ABOUTME: Implements the deployment-wide volume-list HTTP authorization boundary.
// ABOUTME: Keeps root-only registry access testable without a Workers runtime harness.

import { authenticate } from './auth';
import { errorResponse, HttpError } from './files-api';
import type { VolumePage } from './volume-registry-storage';

export async function handleVolumeRegistryRequest(
  request: Request,
  authSecret: string | undefined,
  list: (after: string, limit: number) => Promise<VolumePage>,
): Promise<Response> {
  try {
    if (request.method !== 'GET') {
      throw new HttpError(405, 'METHOD_NOT_ALLOWED', 'Method not allowed', { Allow: 'GET' });
    }
    if (authSecret) {
      const identity = await authenticate(authSecret, request.headers.get('Authorization'), '');
      if (identity.kind !== 'root') {
        throw new HttpError(403, 'FORBIDDEN', 'Volume listing requires root access');
      }
    }
    const url = new URL(request.url);
    const after = url.searchParams.get('cursor') ?? '';
    const rawLimit = url.searchParams.get('limit') ?? '100';
    const limit = Number(rawLimit);
    if (!Number.isSafeInteger(limit) || limit < 1 || limit > 1000) {
      throw new HttpError(400, 'INVALID_ARGUMENT', 'limit must be an integer from 1 to 1000');
    }
    return Response.json(await list(after, limit));
  } catch (error) {
    return errorResponse(error);
  }
}
