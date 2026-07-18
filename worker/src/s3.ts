// ABOUTME: Path-style S3 compatibility for AiryFS volumes and filesystem objects.
// ABOUTME: Verifies SigV4 and maps bucket/list/object operations onto AgentFS.

import type { FileSystem, Stats } from 'agentfs-sdk/cloudflare';
import { fileResponse, HttpError, VolumeAccessCoordinator, writeFileStream } from './files-api';
import { sha256Path } from './checksum';

export const S3_ACCESS_KEY_ID = 'airyfs';
const MAX_KEYS = 1000;
const MAX_ENTRIES = 100_000;
const encoder = new TextEncoder();
const decoder = new TextDecoder('utf-8', { fatal: true, ignoreBOM: false });

export interface S3Route { volume: string; key: string | null }

export interface S3Options {
  request: Request;
  route: S3Route;
  fs: FileSystem;
  access: VolumeAccessCoordinator;
  authSecret?: string;
  onMutation(paths: string[]): Promise<void>;
  versionForInode(ino: number): number;
}

export function parseS3Route(pathname: string): S3Route | null {
  if (pathname !== '/s3' && !pathname.startsWith('/s3/')) return null;
  const parts = pathname.split('/');
  if (!parts[2]) return null;
  const volume = decodeURIComponent(parts[2]);
  const decodedKey = parts.length > 3 ? parts.slice(3).map(decodeURIComponent).join('/') : '';
  const key = decodedKey === '' ? null : decodedKey;
  return { volume, key };
}

export async function handleS3Request(options: S3Options): Promise<Response> {
  const requestId = crypto.randomUUID();
  try {
    if (options.authSecret) await verifySigV4(options.request, options.authSecret);
    const response = options.route.key === null
      ? await bucketRequest(options)
      : await objectRequest(options);
    response.headers.set('x-amz-request-id', requestId);
    response.headers.set('x-amz-id-2', requestId);
    response.headers.set('x-amz-bucket-region', 'auto');
    return response;
  } catch (error) {
    return s3Error(error, options.route.key, requestId);
  }
}

async function bucketRequest(options: S3Options): Promise<Response> {
  const { request, fs } = options;
  if (request.method === 'HEAD') {
    await fs.stat('/');
    return new Response(null, { status: 200 });
  }
  if (request.method === 'GET') {
    const url = new URL(request.url);
    if (url.searchParams.has('location')) {
      return xmlResponse('<LocationConstraint xmlns="http://s3.amazonaws.com/doc/2006-03-01/">auto</LocationConstraint>');
    }
    return listObjects(options, url);
  }
  throw new S3Error(405, 'MethodNotAllowed', 'The specified method is not allowed against this resource.');
}

async function objectRequest(options: S3Options): Promise<Response> {
  const { request, route, fs, access } = options;
  validateKey(route.key!);
  const path = `/${route.key}`;
  if (request.method === 'GET' || request.method === 'HEAD') {
    return fileResponse(fs, path, request, access, options.versionForInode);
  }
  if (request.method === 'PUT') {
    const parent = path.slice(0, path.lastIndexOf('/')) || '/';
    const createdDirectories: string[] = [];
    try {
      if (parent !== '/') await ensureDirectories(fs, access, parent, createdDirectories);
      const expectedHash = request.headers.get('x-amz-content-sha256');
      if (expectedHash?.startsWith('STREAMING-')) {
        throw new S3Error(501, 'NotImplemented', 'Streaming SigV4 payload framing is not supported.');
      }
      await writeFileStream(fs, path, request.body, access, expectedHash && expectedHash !== 'UNSIGNED-PAYLOAD'
        ? async (temp) => {
          if (!/^[0-9a-f]{64}$/.test(expectedHash)) throw new S3Error(400, 'InvalidRequest', 'Invalid x-amz-content-sha256 header.');
          if ((await sha256Path(fs, temp)).checksum !== expectedHash) throw new S3Error(400, 'BadDigest', 'The Content-MD5 or checksum you specified did not match what we received.');
        }
        : undefined);
    } catch (error) {
      await removeCreatedDirectories(fs, access, createdDirectories);
      if (createdDirectories.length > 0) await options.onMutation(createdDirectories);
      throw error;
    }
    await options.onMutation([...createdDirectories, path]);
    const stats = await fs.stat(path);
    return new Response(null, { status: 200, headers: { ETag: objectEtag(stats, options.versionForInode(stats.ino)) } });
  }
  if (request.method === 'DELETE') {
    const release = await access.acquireWrite(path);
    try {
      await fs.unlink(path).catch((error: unknown) => {
        if (!isNotFound(error)) throw error;
      });
    } finally {
      release();
    }
    await options.onMutation([path]);
    return new Response(null, { status: 204 });
  }
  throw new S3Error(405, 'MethodNotAllowed', 'The specified method is not allowed against this resource.');
}

async function listObjects(options: S3Options, url: URL): Promise<Response> {
  const prefix = url.searchParams.get('prefix') ?? '';
  const delimiter = url.searchParams.get('delimiter');
  const continuation = decodeToken(url.searchParams.get('continuation-token')) || url.searchParams.get('start-after') || '';
  const requestedMax = Number(url.searchParams.get('max-keys') ?? MAX_KEYS);
  const maxKeys = Number.isSafeInteger(requestedMax) && requestedMax >= 0
    ? Math.min(requestedMax, MAX_KEYS)
    : MAX_KEYS;
  const release = await options.access.acquireRead('/');
  let objects: Array<{ key: string; stats: Stats }>;
  try {
    objects = await walkFiles(options.fs);
  } finally {
    release();
  }

  const entries = new Map<string, { kind: 'object'; stats: Stats } | { kind: 'prefix' }>();
  for (const object of objects) {
    if (!object.key.startsWith(prefix)) continue;
    const suffix = object.key.slice(prefix.length);
    const delimiterIndex = delimiter ? suffix.indexOf(delimiter) : -1;
    if (delimiter && delimiterIndex >= 0) {
      const commonPrefix = prefix + suffix.slice(0, delimiterIndex + delimiter.length);
      entries.set(commonPrefix, { kind: 'prefix' });
    } else entries.set(object.key, { kind: 'object', stats: object.stats });
  }
  const sorted = [...entries.entries()]
    .filter(([key]) => !continuation || compareUtf8(key, continuation) > 0)
    .sort(([left], [right]) => compareUtf8(left, right));
  const page = sorted.slice(0, maxKeys);
  const truncated = maxKeys > 0 && sorted.length > page.length;
  const nextToken = truncated && page.length > 0 ? encodeToken(page[page.length - 1][0]) : null;
  const contents: string[] = [];
  const prefixes: string[] = [];
  const encoded = url.searchParams.get('encoding-type') === 'url';
  const outputKey = (key: string): string => encoded ? awsEncode(key).replace(/%2F/g, '/') : key;
  for (const [key, entry] of page) {
    if (entry.kind === 'prefix') prefixes.push(`<CommonPrefixes><Prefix>${xml(outputKey(key))}</Prefix></CommonPrefixes>`);
    else contents.push(`<Contents><Key>${xml(outputKey(key))}</Key><LastModified>${new Date(entry.stats.mtime * 1000).toISOString()}</LastModified><ETag>${xml(objectEtag(entry.stats, options.versionForInode(entry.stats.ino)))}</ETag><Size>${entry.stats.size}</Size><StorageClass>STANDARD</StorageClass></Contents>`);
  }
  return xmlResponse(`<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Name>${xml(options.route.volume)}</Name><Prefix>${xml(outputKey(prefix))}</Prefix>${delimiter ? `<Delimiter>${xml(outputKey(delimiter))}</Delimiter>` : ''}${encoded ? '<EncodingType>url</EncodingType>' : ''}<MaxKeys>${maxKeys}</MaxKeys><KeyCount>${page.length}</KeyCount><IsTruncated>${truncated}</IsTruncated>${contents.join('')}${prefixes.join('')}${nextToken ? `<NextContinuationToken>${xml(nextToken)}</NextContinuationToken>` : ''}</ListBucketResult>`);
}

async function walkFiles(fs: FileSystem): Promise<Array<{ key: string; stats: Stats }>> {
  const queue = ['/'];
  const files: Array<{ key: string; stats: Stats }> = [];
  let visited = 0;
  while (queue.length > 0) {
    const directory = queue.shift()!;
    const entries = await fs.readdirPlus(directory);
    entries.sort((left, right) => compareUtf8(left.name, right.name));
    for (const entry of entries) {
      if (directory === '/' && entry.name === '.airyfs-trash') continue;
      if (++visited > MAX_ENTRIES) throw new S3Error(503, 'SlowDown', 'The bucket contains too many entries to list.');
      const path = directory === '/' ? `/${entry.name}` : `${directory}/${entry.name}`;
      if (entry.stats.isDirectory()) queue.push(path);
      else if (!entry.stats.isSymbolicLink()) files.push({ key: path.slice(1), stats: entry.stats });
    }
  }
  return files;
}

async function ensureDirectories(
  fs: FileSystem,
  access: VolumeAccessCoordinator,
  path: string,
  created: string[],
): Promise<void> {
  let current = '';
  for (const segment of path.split('/').filter(Boolean)) {
    current += `/${segment}`;
    const release = await access.acquireWrite(current);
    try {
      await fs.mkdir(current);
      created.push(current);
    } catch (error) {
      if (!(error instanceof Error && 'code' in error && (error as Error & { code?: string }).code === 'EEXIST')) throw error;
    } finally {
      release();
    }
  }
}

async function removeCreatedDirectories(fs: FileSystem, access: VolumeAccessCoordinator, paths: string[]): Promise<void> {
  for (const path of paths.slice().reverse()) {
    const release = await access.acquireWrite(path);
    try {
      if ((await fs.readdir(path).catch(() => ['occupied'])).length === 0) {
        await fs.rm(path, { recursive: true, force: false }).catch(() => undefined);
      }
    } finally { release(); }
  }
}

function validateKey(key: string): void {
  const segments = key.split('/');
  if (key.endsWith('/') || segments.some((segment) => segment === '' || segment === '.' || segment === '..')) {
    throw new S3Error(400, 'InvalidObjectName', 'Object keys must map unambiguously to filesystem paths.');
  }
}

function compareUtf8(left: string, right: string): number {
  const a = encoder.encode(left);
  const b = encoder.encode(right);
  const length = Math.min(a.length, b.length);
  for (let index = 0; index < length; index++) {
    if (a[index] !== b[index]) return a[index] - b[index];
  }
  return a.length - b.length;
}

export async function verifySigV4(request: Request, secret: string, now = Date.now()): Promise<void> {
  const authorization = request.headers.get('Authorization') ?? '';
  const match = /^AWS4-HMAC-SHA256 Credential=([^/]+)\/(\d{8})\/([^/]+)\/s3\/aws4_request, SignedHeaders=([^,]+), Signature=([0-9a-f]{64})$/.exec(authorization);
  if (!match || match[1] !== S3_ACCESS_KEY_ID) throw new S3Error(403, 'InvalidAccessKeyId', 'The AWS access key ID you provided does not exist in our records.');
  const [, , date, region, signedHeaderText, signature] = match;
  const timestamp = request.headers.get('x-amz-date');
  if (!timestamp || !timestamp.startsWith(date)) throw new S3Error(403, 'AuthorizationHeaderMalformed', 'The authorization header is malformed.');
  const parsedTime = Date.parse(`${timestamp.slice(0, 4)}-${timestamp.slice(4, 6)}-${timestamp.slice(6, 8)}T${timestamp.slice(9, 11)}:${timestamp.slice(11, 13)}:${timestamp.slice(13, 15)}Z`);
  if (!Number.isFinite(parsedTime) || Math.abs(now - parsedTime) > 15 * 60 * 1000) throw new S3Error(403, 'RequestTimeTooSkewed', 'The difference between the request time and the current time is too large.');

  const url = new URL(request.url);
  const signedHeaders = signedHeaderText.split(';');
  const canonicalHeaders = signedHeaders.map((name) => {
    const value = name === 'host' ? request.headers.get('Host') ?? url.host : request.headers.get(name);
    if (value === null) throw new S3Error(403, 'SignatureDoesNotMatch', `Missing signed header: ${name}`);
    return `${name}:${value.trim().replace(/\s+/g, ' ')}\n`;
  }).join('');
  const payloadHash = request.headers.get('x-amz-content-sha256') ?? await sha256Hex('');
  const canonicalRequest = [
    request.method,
    canonicalUri(url.pathname),
    canonicalQuery(url),
    canonicalHeaders,
    signedHeaderText,
    payloadHash,
  ].join('\n');
  const scope = `${date}/${region}/s3/aws4_request`;
  const stringToSign = `AWS4-HMAC-SHA256\n${timestamp}\n${scope}\n${await sha256Hex(canonicalRequest)}`;
  const dateKey = await hmac(encoder.encode(`AWS4${secret}`), date);
  const regionKey = await hmac(dateKey, region);
  const serviceKey = await hmac(regionKey, 's3');
  const signingKey = await hmac(serviceKey, 'aws4_request');
  const expected = hex(await hmac(signingKey, stringToSign));
  if (!constantTimeEqual(signature, expected)) throw new S3Error(403, 'SignatureDoesNotMatch', 'The request signature we calculated does not match the signature you provided.');
}

function canonicalUri(pathname: string): string {
  return pathname.split('/').map((part) => awsEncode(decodeURIComponent(part))).join('/');
}

function canonicalQuery(url: URL): string {
  return [...url.searchParams.entries()]
    .map(([key, value]) => [awsEncode(key), awsEncode(value)] as const)
    .sort(([ak, av], [bk, bv]) => ak.localeCompare(bk) || av.localeCompare(bv))
    .map(([key, value]) => `${key}=${value}`)
    .join('&');
}

function awsEncode(value: string): string {
  return encodeURIComponent(value).replace(/[!'()*]/g, (character) => `%${character.charCodeAt(0).toString(16).toUpperCase()}`);
}

async function sha256Hex(value: string): Promise<string> {
  return hex(new Uint8Array(await crypto.subtle.digest('SHA-256', encoder.encode(value))));
}

async function hmac(key: Uint8Array, value: string): Promise<Uint8Array> {
  const cryptoKey = await crypto.subtle.importKey('raw', key, { name: 'HMAC', hash: 'SHA-256' }, false, ['sign']);
  return new Uint8Array(await crypto.subtle.sign('HMAC', cryptoKey, encoder.encode(value)));
}

function hex(bytes: Uint8Array): string {
  return [...bytes].map((byte) => byte.toString(16).padStart(2, '0')).join('');
}

function constantTimeEqual(left: string, right: string): boolean {
  if (left.length !== right.length) return false;
  let difference = 0;
  for (let index = 0; index < left.length; index++) difference |= left.charCodeAt(index) ^ right.charCodeAt(index);
  return difference === 0;
}

function objectEtag(stats: Stats, version: number): string {
  return `"${stats.ino.toString(16)}-${version.toString(16)}-${stats.size.toString(16)}"`;
}

function encodeToken(key: string): string {
  let binary = '';
  for (const byte of encoder.encode(key)) binary += String.fromCharCode(byte);
  return btoa(binary);
}
function decodeToken(token: string | null): string {
  if (!token) return '';
  try {
    const binary = atob(token);
    return decoder.decode(Uint8Array.from(binary, (character) => character.charCodeAt(0)));
  } catch {
    throw new S3Error(400, 'InvalidArgument', 'The continuation token provided is incorrect.');
  }
}

function xml(value: string): string {
  return value.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;').replace(/'/g, '&apos;');
}

function xmlResponse(body: string, status = 200): Response {
  return new Response(`<?xml version="1.0" encoding="UTF-8"?>${body}`, { status, headers: { 'Content-Type': 'application/xml' } });
}

class S3Error extends Error {
  constructor(readonly status: number, readonly code: string, message: string) { super(message); }
}

function s3Error(error: unknown, key: string | null, requestId: string): Response {
  let status = 500;
  let code = 'InternalError';
  let message = 'We encountered an internal error. Please try again.';
  if (error instanceof S3Error || error instanceof HttpError) {
    status = error.status;
    code = error instanceof S3Error ? error.code : error.code === 'ENOENT' ? 'NoSuchKey' : error.code;
    message = error.message;
  } else if (isNotFound(error)) {
    status = 404;
    code = key === null ? 'NoSuchBucket' : 'NoSuchKey';
    message = key === null ? 'The specified bucket does not exist.' : 'The specified key does not exist.';
  } else if (error instanceof Error && 'code' in error && (error as Error & { code?: string }).code === 'EISDIR') {
    status = 404;
    code = 'NoSuchKey';
    message = 'The specified key does not exist.';
  }
  const response = xmlResponse(`<Error><Code>${xml(code)}</Code><Message>${xml(message)}</Message>${key === null ? '' : `<Key>${xml(key)}</Key>`}<RequestId>${requestId}</RequestId></Error>`, status);
  response.headers.set('x-amz-request-id', requestId);
  response.headers.set('x-amz-id-2', requestId);
  response.headers.set('x-amz-bucket-region', 'auto');
  return response;
}

function isNotFound(error: unknown): boolean {
  return error instanceof Error && 'code' in error && (error as Error & { code?: string }).code === 'ENOENT';
}
