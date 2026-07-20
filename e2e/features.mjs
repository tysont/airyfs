// ABOUTME: Deployed smoke test for AiryFS authentication, archives, snapshots, uploads, jobs, exec, and changes.
// ABOUTME: Uses the public TypeScript SDK against an explicitly selected integration endpoint.

import {
  AiryFSApiError,
  AiryFSClient,
  resumableUploadBlob,
  waitForJob,
} from '../sdk/dist/index.js';
import { pythonCommand } from './benchmark-lib.mjs';

const endpoint = process.env.AIRYFS_URL;
if (!endpoint) throw new Error('AIRYFS_URL is required');
const token = process.env.AIRYFS_TOKEN;
const clientOptions = token ? { token } : {};

const suffix = `${Date.now()}-${crypto.randomUUID().slice(0, 8)}`;
const volume = `features-${suffix}`;
const cloneVolume = `features-clone-${suffix}`;
const forkVolume = `features-fork-${suffix}`;
const client = new AiryFSClient(endpoint, volume, clientOptions);
const clone = new AiryFSClient(endpoint, cloneVolume, clientOptions);
const fork = new AiryFSClient(endpoint, forkVolume, clientOptions);
let passed = 0;

try {
  await client.createVolume(256 * 1024);
  equal((await client.authStatus()).auth, token ? 'root' : 'disabled', 'integration authentication mode');

  const initial = await client.getChanges();
  await client.makeDirectory('/src');
  await client.writeFile('/src/main.txt', 'one');
  const directChanges = await client.getChanges({ since: initial.cursor, path: '/src' });
  assert(directChanges.events.some((event) => event.path === '/src/main.txt'), 'direct change feed');

  await client.importTree('/bulk', archive([{ path: 'nested.txt', body: 'archive-data' }]), true);
  equal(await client.readFileText('/bulk/nested.txt'), 'archive-data', 'transactional tree import');
  assert((await client.exportTree('/bulk')).body !== null, 'tree export stream');

  const snapshot = await client.createSnapshot('before-change', 'deployed feature smoke');
  await client.writeFile('/src/main.txt', 'two');
  assert((await client.diffSnapshot(snapshot.id)).some((entry) => entry.path === '/src/main.txt'), 'snapshot diff');
  await client.restoreSnapshot(snapshot.id);
  equal(await client.readFileText('/src/main.txt'), 'one', 'snapshot restore');
  await client.cloneSnapshot(snapshot.id, cloneVolume);
  equal(await clone.readFileText('/src/main.txt'), 'one', 'snapshot clone');
  await client.forkVolume(forkVolume);
  equal(await fork.readFileText('/src/main.txt'), 'one', 'live volume fork');

  const largeBytes = new Uint8Array(1024 * 1024 + 3);
  crypto.getRandomValues(largeBytes.subarray(0, 65_536));
  const checksum = await sha256(largeBytes);
  await resumableUploadBlob(client, '/large.bin', new Blob([largeBytes]), checksum);
  equal((await client.checksum('/large.bin')).checksum, checksum, 'resumable upload checksum');

  const crossChunk = new Uint8Array(2 * 256 * 1024 + 123);
  for (let index = 0; index < crossChunk.length; index++) crossChunk[index] = index % 251;
  await client.writeFile('/cross-chunk.bin', crossChunk.slice().buffer);
  const sliceOffset = 256 * 1024 - 137;
  const sliceLength = 8192;
  const fuseRead = await client.exec(pythonCommand(`
import hashlib, json, os
handle = os.open('/volume/cross-chunk.bin', os.O_RDONLY)
try:
    whole = b''
    while len(whole) < ${crossChunk.length}:
        chunk = os.read(handle, 65537)
        if not chunk: break
        whole += chunk
    part = os.pread(handle, ${sliceLength}, ${sliceOffset})
finally:
    os.close(handle)
print(json.dumps({'whole': hashlib.sha256(whole).hexdigest(), 'part': hashlib.sha256(part).hexdigest(), 'size': len(whole)}))
`));
  const fuseReadResult = JSON.parse(fuseRead.stdout.trim());
  equal(fuseReadResult.size, crossChunk.length, 'FUSE sequential binary read length');
  equal(fuseReadResult.whole, await sha256(crossChunk), 'FUSE sequential binary read checksum');
  equal(
    fuseReadResult.part,
    await sha256(crossChunk.slice(sliceOffset, sliceOffset + sliceLength)),
    'FUSE unaligned cross-chunk pread checksum',
  );
  const bridgeBefore = await client.perf();
  assert(bridgeBefore.sessionId !== null, 'Hrana bridge session established');

  const randomWriteLength = 2 * 256 * 1024 + 8192;
  const expectedRandomWrite = new Uint8Array(randomWriteLength);
  expectedRandomWrite.fill(65, 4093, 4093 + 8192);
  expectedRandomWrite.fill(66, 256 * 1024 - 31, 256 * 1024 - 31 + 4096);
  await client.writeFile('/random-write.bin', new Uint8Array(randomWriteLength).buffer);
  const fuseWriteRegression = await client.exec(pythonCommand(`
import json, os
handle = os.open('/volume/random-write.bin', os.O_RDWR)
try:
    assert os.pwrite(handle, b'A' * 8192, 4093) == 8192
    assert os.pwrite(handle, b'B' * 4096, ${256 * 1024 - 31}) == 4096
    os.fsync(handle)
finally:
    os.close(handle)
print(json.dumps({'ok': True}))
`));
  assert(JSON.parse(fuseWriteRegression.stdout).ok, 'FUSE unaligned random writes complete');
  assert(
    bytesEqual(await client.readFileBytes('/random-write.bin'), expectedRandomWrite),
    'direct API sees exact FUSE random-write bytes',
  );
  const bridgeAfter = await client.perf();
  equal(bridgeAfter.sessionId, bridgeBefore.sessionId, 'Hrana bridge session reused across execs');
  assert(bridgeAfter.pipelineRequests > bridgeBefore.pipelineRequests, 'Hrana bridge counters advance across execs');

  const scanFiles = Array.from({ length: 257 }, (_, index) => ({
    path: `file-${String(index).padStart(4, '0')}.txt`,
    body: 'x',
  }));
  await client.importTree('/scan', archive(scanFiles), true);
  const scan = await client.exec(pythonCommand(`
import json, os
entries = []
with os.scandir('/volume/scan') as iterator:
    for entry in iterator:
        entries.append((entry.name, entry.stat(follow_symlinks=False).st_size))
print(json.dumps({'count': len(entries), 'unique': len(set(name for name, _ in entries)), 'bytes': sum(size for _, size in entries), 'names': sorted(name for name, _ in entries)}))
`));
  const scanResult = JSON.parse(scan.stdout);
  equal(scanResult.count, scanFiles.length, 'FUSE directory traversal entry count');
  equal(scanResult.unique, scanFiles.length, 'FUSE directory traversal has no duplicates');
  equal(scanResult.bytes, scanFiles.length, 'FUSE readdir attributes match file sizes');
  equal(JSON.stringify(scanResult.names), JSON.stringify(scanFiles.map((file) => file.path)), 'FUSE directory traversal names');

  const missing = await client.exec('test ! -e /volume/appeared.txt');
  equal(missing.exitCode, 0, 'FUSE primes a negative lookup');
  await client.writeFile('/appeared.txt', 'appeared');
  equal((await client.exec('cat /volume/appeared.txt')).stdout, 'appeared', 'direct create invalidates FUSE negative lookup');

  const concurrentFiles = Array.from({ length: 8 }, (_, index) => ({
    path: `file-${index}.bin`,
    body: String(index).repeat(64 * 1024),
  }));
  await client.importTree('/concurrent', archive(concurrentFiles), true);
  const concurrent = await client.exec(pythonCommand(`
import concurrent.futures, json, pathlib
paths = list(pathlib.Path('/volume/concurrent').glob('*.bin'))
def read(path):
    data = path.read_bytes()
    return path.name, len(data), __import__('hashlib').sha256(data).hexdigest()
with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
    results = list(executor.map(read, paths))
print(json.dumps({'files': len(results), 'bytes': sum(size for _, size, _ in results), 'hashes': sorted((name, digest) for name, _, digest in results)}))
`));
  const concurrentResult = JSON.parse(concurrent.stdout);
  equal(concurrentResult.files, concurrentFiles.length, 'concurrent FUSE read count');
  equal(concurrentResult.bytes, concurrentFiles.length * 64 * 1024, 'concurrent FUSE read bytes');
  const expectedConcurrentHashes = await Promise.all(concurrentFiles.map(async (file) => [file.path, await sha256(new TextEncoder().encode(file.body))]));
  equal(JSON.stringify(concurrentResult.hashes), JSON.stringify(expectedConcurrentHashes), 'concurrent FUSE read contents');

  const stream = await client.execStream('sleep 30; printf should-not-complete');
  const iterator = stream[Symbol.asyncIterator]();
  const start = await iterator.next();
  assert(!start.done && start.value.type === 'start', 'streaming exec start event');
  let busy = false;
  try {
    await client.exec('printf overlap');
  } catch (error) {
    busy = error instanceof AiryFSApiError && error.code === 'EXEC_BUSY';
  }
  assert(busy, 'streaming exec single-flight admission');
  await client.cancelExec(start.value.id);
  let canceled = false;
  for await (const event of { [Symbol.asyncIterator]: () => iterator }) {
    if (event.type === 'exit') canceled = event.exitCode !== 0;
  }
  assert(canceled, 'streaming exec cancellation');

  const output = [];
  const submitted = await client.submitJob('printf job-output', '/', `job-${suffix}`);
  const completed = await waitForJob(client, submitted.id, {
    interval: 250,
    onLog: (entry) => output.push(new TextDecoder().decode(base64(entry.data))),
  });
  equal(completed.job.status, 'succeeded', 'durable job completion');
  equal(output.join(''), 'job-output', 'durable job persisted output');

  const beforeFuse = await client.getChanges();
  const fuseWrite = await client.exec('printf from-fuse > /volume/from-fuse.txt');
  equal(fuseWrite.exitCode, 0, 'FUSE write command');
  const fuseChanges = await client.getChanges({ since: beforeFuse.cursor, path: '/from-fuse.txt' });
  assert(fuseChanges.events.some((event) => event.path === '/from-fuse.txt'), 'FUSE-origin change feed');

  await client.deleteSnapshot(snapshot.id);
  console.log(`Feature smoke passed: ${passed} checks on ${volume}`);
} finally {
  await Promise.allSettled([client.destroyContainer(), clone.destroyContainer()]);
}

function archive(files) {
  const chunks = [Uint8Array.from([...new TextEncoder().encode('AIRYFS'), 1]), frame({ t: 'd', p: '' })];
  for (const file of files) {
    const body = new TextEncoder().encode(file.body);
    chunks.push(frame({ t: 'f', p: file.path, s: body.byteLength }), body);
  }
  chunks.push(new Uint8Array(4));
  return new Blob(chunks);
}

function frame(header) {
  const body = new TextEncoder().encode(JSON.stringify(header));
  const result = new Uint8Array(4 + body.byteLength);
  new DataView(result.buffer).setUint32(0, body.byteLength, false);
  result.set(body, 4);
  return result;
}

async function sha256(bytes) {
  const digest = await crypto.subtle.digest('SHA-256', bytes);
  return [...new Uint8Array(digest)].map((byte) => byte.toString(16).padStart(2, '0')).join('');
}

function base64(value) {
  return Uint8Array.from(atob(value), (character) => character.charCodeAt(0));
}

function assert(condition, name) {
  if (!condition) throw new Error(`FAIL: ${name}`);
  passed++;
  console.log(`PASS: ${name}`);
}

function bytesEqual(left, right) {
  if (left.byteLength !== right.byteLength) return false;
  for (let index = 0; index < left.byteLength; index++) {
    if (left[index] !== right[index]) return false;
  }
  return true;
}

function equal(actual, expected, name) {
  assert(Object.is(actual, expected), `${name} (${JSON.stringify(actual)})`);
}
