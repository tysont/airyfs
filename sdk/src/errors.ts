// ABOUTME: Typed transport and HTTP errors returned by the AiryFS SDK.
// ABOUTME: Bounds proxy error bodies and preserves stable Worker error codes.

export class AiryFSApiError extends Error {
  constructor(
    readonly status: number,
    readonly code: string,
    message: string,
    readonly path?: string,
  ) {
    super(message);
    this.name = 'AiryFSApiError';
  }
}

export class AiryFSTransportError extends Error {
  constructor(readonly origin: string, message: string) {
    super(`Could not reach ${origin}: ${message}`);
    this.name = 'AiryFSTransportError';
  }
}

/** The command was admitted, but its terminal outcome cannot be proven. */
export class AiryFSCommandOutcomeUnknownError extends Error {
  constructor(readonly commandId: string, message = 'Command outcome is unknown') {
    super(`${message} (command ${commandId})`);
    this.name = 'AiryFSCommandOutcomeUnknownError';
  }
}

const MAX_ERROR_BODY_BYTES = 64 * 1024;

export async function responseError(response: Response): Promise<AiryFSApiError> {
  const body = await readBoundedBody(response, MAX_ERROR_BODY_BYTES);
  const contentType = (response.headers.get('Content-Type') || '').toLowerCase();
  if (contentType.includes('text/html') || /^\s*<(?:!doctype\s+html|html)\b/i.test(body)) {
    const title = /<title>([^<]+)<\/title>/i.exec(body)?.[1]?.trim();
    return new AiryFSApiError(
      response.status,
      `HTTP_${response.status}`,
      response.statusText || title || `HTTP ${response.status}`,
    );
  }
  if (body) {
    try {
      const parsed = JSON.parse(body) as { error?: { code?: unknown; message?: unknown; path?: unknown } };
      if (parsed.error && typeof parsed.error.message === 'string') {
        return new AiryFSApiError(
          response.status,
          typeof parsed.error.code === 'string' ? parsed.error.code : `HTTP_${response.status}`,
          parsed.error.message,
          typeof parsed.error.path === 'string' ? parsed.error.path : undefined,
        );
      }
    } catch {
      // Legacy routes can return plain text.
    }
  }
  return new AiryFSApiError(
    response.status,
    `HTTP_${response.status}`,
    body.trim() || response.statusText || `HTTP ${response.status}`,
  );
}

async function readBoundedBody(response: Response, limit: number): Promise<string> {
  if (!response.body) return '';
  const reader = response.body.getReader();
  const decoder = new TextDecoder();
  let body = '';
  let remaining = limit;
  while (remaining > 0) {
    const { done, value } = await reader.read();
    if (done) return body + decoder.decode();
    const chunk = value.subarray(0, remaining);
    body += decoder.decode(chunk, { stream: chunk.length === value.length });
    remaining -= chunk.length;
    if (chunk.length < value.length) break;
  }
  await reader.cancel();
  return body + decoder.decode();
}
