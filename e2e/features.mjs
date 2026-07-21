// ABOUTME: Deployed smoke test for AiryFS authentication, archives, snapshots, uploads, jobs, exec, and changes.
// ABOUTME: Uses the public TypeScript SDK against an explicitly selected integration endpoint.

import {
  AiryFSApiError,
  AiryFSClient,
  resumableUploadBlob,
  waitForJob,
} from '../sdk/dist/index.js';
import { counterDelta, pythonCommand } from './benchmark-lib.mjs';

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
  assert(Number.isSafeInteger(bridgeBefore.sessionEpoch), 'Hrana bridge session epoch exposed');

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
  equal(
    fuseWriteRegression.exitCode,
    0,
    `FUSE unaligned random-write command succeeds; stderr=${fuseWriteRegression.stderr.trim()}`,
  );
  assert(JSON.parse(fuseWriteRegression.stdout).ok, 'FUSE unaligned random writes complete');
  assert(
    bytesEqual(await client.readFileBytes('/random-write.bin'), expectedRandomWrite),
    'direct API sees exact FUSE random-write bytes',
  );
  const bridgeAfter = await client.perf();
  equal(bridgeAfter.sessionId, bridgeBefore.sessionId, 'Hrana bridge session reused across execs');
  equal(bridgeAfter.sessionEpoch, bridgeBefore.sessionEpoch, 'Hrana bridge epoch reused across execs');
  assert(bridgeAfter.pipelineRequests > bridgeBefore.pipelineRequests, 'Hrana bridge counters advance across execs');

  const truncateOriginal = new Uint8Array(2 * 256 * 1024 + 123);
  for (let index = 0; index < truncateOriginal.length; index++) truncateOriginal[index] = index % 251;
  await client.writeFile('/truncate.bin', truncateOriginal.slice().buffer);
  const truncateTo = async (size) => client.exec(pythonCommand(`
import os
handle = os.open('/volume/truncate.bin', os.O_RDWR)
try:
    os.ftruncate(handle, ${size})
finally:
    os.close(handle)
`));
  const shrinkSize = 256 * 1024 + 17;
  equal((await truncateTo(shrinkSize)).exitCode, 0, 'FUSE cross-chunk truncate succeeds');
  assert(bytesEqual(await client.readFileBytes('/truncate.bin'), truncateOriginal.slice(0, shrinkSize)), 'direct API sees exact FUSE truncate bytes');
  const extendedSize = 2 * 256 * 1024 + 4096;
  equal((await truncateTo(extendedSize)).exitCode, 0, 'FUSE sparse extension succeeds');
  const extendedExpected = new Uint8Array(extendedSize);
  extendedExpected.set(truncateOriginal.subarray(0, shrinkSize));
  assert(bytesEqual(await client.readFileBytes('/truncate.bin'), extendedExpected), 'FUSE sparse extension zero-fills new range');
  equal((await truncateTo(0)).exitCode, 0, 'FUSE truncate to zero succeeds');
  equal((await client.readFileBytes('/truncate.bin')).byteLength, 0, 'direct API sees zero-length FUSE truncate');

  const createFamily = await client.exec(pythonCommand(`
import json, os, stat
base = '/volume/create-family'
os.mkdir(base)
os.mkdir(f'{base}/directory', 0o750)
os.mknod(f'{base}/node', stat.S_IFREG | 0o640)
os.symlink('node', f'{base}/symlink')
os.link(f'{base}/node', f'{base}/hardlink')
directory = os.stat(f'{base}/directory')
node = os.stat(f'{base}/node')
hardlink = os.stat(f'{base}/hardlink')
print(json.dumps({
    'directory_mode': stat.S_IMODE(directory.st_mode),
    'directory_is_dir': stat.S_ISDIR(directory.st_mode),
    'node_mode': stat.S_IMODE(node.st_mode),
    'node_is_file': stat.S_ISREG(node.st_mode),
    'same_inode': node.st_ino == hardlink.st_ino,
    'nlink': node.st_nlink,
    'symlink_target': os.readlink(f'{base}/symlink'),
}))
`));
  equal(createFamily.exitCode, 0, `FUSE create-family command succeeds; stderr=${createFamily.stderr.trim()}`);
  const createFamilyResult = JSON.parse(createFamily.stdout);
  assert(createFamilyResult.directory_is_dir, 'FUSE mkdir creates a directory');
  equal(createFamilyResult.directory_mode, 0o750, 'FUSE mkdir preserves mode');
  assert(createFamilyResult.node_is_file, 'FUSE mknod creates a regular file');
  equal(createFamilyResult.node_mode, 0o640, 'FUSE mknod preserves mode');
  assert(createFamilyResult.same_inode && createFamilyResult.nlink === 2, 'FUSE hard link shares inode and link count');
  equal(createFamilyResult.symlink_target, 'node', 'FUSE symlink preserves target');

  const renameFamily = await client.exec(pythonCommand(`
import errno, json, os
base = '/volume/rename-family'
os.mkdir(base)
with open(f'{base}/source', 'w') as handle: handle.write('source')
with open(f'{base}/destination', 'w') as handle: handle.write('destination')
held = os.open(f'{base}/destination', os.O_RDONLY)
os.rename(f'{base}/source', f'{base}/destination')
path_body = open(f'{base}/destination').read()
held_body = os.read(held, 64).decode()
os.close(held)
os.mkdir(f'{base}/parent')
os.mkdir(f'{base}/parent/child')
cycle_errno = None
try:
    os.rename(f'{base}/parent', f'{base}/parent/child/cycle')
except OSError as error:
    cycle_errno = error.errno
os.mkdir(f'{base}/empty')
os.mkdir(f'{base}/nonempty')
with open(f'{base}/nonempty/child', 'w') as handle: handle.write('x')
nonempty_errno = None
try:
    os.rename(f'{base}/empty', f'{base}/nonempty')
except OSError as error:
    nonempty_errno = error.errno
os.mkdir(f'{base}/empty-source')
os.mkdir(f'{base}/empty-destination')
links_before = os.stat(base).st_nlink
os.rename(f'{base}/empty-source', f'{base}/empty-destination')
links_after = os.stat(base).st_nlink
os.rename(f'{base}/destination', f'{base}/destination')
print(json.dumps({
    'path_body': path_body,
    'held_body': held_body,
    'cycle_errno': cycle_errno,
    'nonempty_errno': nonempty_errno,
    'empty_replaced': os.path.isdir(f'{base}/empty-destination') and not os.path.exists(f'{base}/empty-source'),
    'directory_links_removed': links_before - links_after,
    'same_path_exists': os.path.isfile(f'{base}/destination'),
}))
`));
  equal(renameFamily.exitCode, 0, `FUSE rename-family command succeeds; stderr=${renameFamily.stderr.trim()}`);
  const renameFamilyResult = JSON.parse(renameFamily.stdout);
  equal(renameFamilyResult.path_body, 'source', 'FUSE rename-over exposes source at destination');
  equal(renameFamilyResult.held_body, 'destination', 'FUSE rename-over retains open destination inode');
  equal(renameFamilyResult.cycle_errno, 22, 'FUSE rename rejects directory cycles with EINVAL');
  equal(renameFamilyResult.nonempty_errno, 39, 'FUSE rename rejects nonempty directory replacement');
  assert(renameFamilyResult.empty_replaced, 'FUSE rename replaces an empty directory');
  equal(renameFamilyResult.directory_links_removed, 1, 'FUSE empty-directory replacement updates parent links');
  assert(renameFamilyResult.same_path_exists, 'FUSE same-path rename is a no-op');

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

  await client.writeFile('/cache-visibility.txt', 'before');
  await client.exec('cat /volume/cache-visibility.txt >/dev/null; stat /volume/cache-visibility.txt >/dev/null');
  await client.writeFile('/cache-visibility.txt', 'after-overwrite');
  const overwriteVisible = await client.exec(pythonCommand(`
import json, os
path = '/volume/cache-visibility.txt'
print(json.dumps({'body': open(path).read(), 'size': os.stat(path).st_size}))
`));
  equal(
    JSON.stringify(JSON.parse(overwriteVisible.stdout)),
    JSON.stringify({ body: 'after-overwrite', size: 15 }),
    'direct overwrite invalidates FUSE data and attributes',
  );

  await client.exec('stat /volume/cache-visibility.txt >/dev/null');
  await client.truncate('/cache-visibility.txt', 5);
  const truncateVisible = await client.exec(pythonCommand(`
import json, os
path = '/volume/cache-visibility.txt'
print(json.dumps({'body': open(path).read(), 'size': os.stat(path).st_size}))
`));
  equal(
    JSON.stringify(JSON.parse(truncateVisible.stdout)),
    JSON.stringify({ body: 'after', size: 5 }),
    'direct truncate invalidates FUSE data and attributes',
  );

  await client.exec('stat /volume/cache-visibility.txt >/dev/null');
  await client.chmod('/cache-visibility.txt', 0o600);
  equal(
    Number((await client.exec("stat -c '%a' /volume/cache-visibility.txt")).stdout.trim()),
    600,
    'direct chmod invalidates FUSE attributes',
  );

  await client.exec('stat /volume/cache-visibility.txt >/dev/null; test ! -e /volume/cache-renamed.txt');
  await client.rename('/cache-visibility.txt', '/cache-renamed.txt');
  equal(
    (await client.exec('test ! -e /volume/cache-visibility.txt && test -f /volume/cache-renamed.txt')).exitCode,
    0,
    'direct rename invalidates FUSE entries and attributes',
  );

  await client.exec('stat /volume/cache-renamed.txt >/dev/null');
  await client.deleteFile('/cache-renamed.txt', true);
  equal((await client.exec('test ! -e /volume/cache-renamed.txt')).exitCode, 0, 'direct delete invalidates FUSE entry');
  await client.writeFile('/cache-renamed.txt', 'recreated');
  equal((await client.exec('cat /volume/cache-renamed.txt')).stdout, 'recreated', 'direct recreate invalidates FUSE negative lookup');

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
  const reconnectBefore = await client.perf();
  await client.destroyContainer();
  equal((await client.exec('true')).exitCode, 0, 'Hrana bridge reconnects after Container restart');
  const reconnectAfter = await client.perf();
  assert(reconnectAfter.sessionId !== reconnectBefore.sessionId, 'Hrana bridge session changes on reconnect');
  assert(reconnectAfter.sessionEpoch > reconnectBefore.sessionEpoch, 'Hrana bridge epoch advances on reconnect');
  equal(
    counterDelta(
      reconnectBefore.pipelineRequests, reconnectAfter.pipelineRequests,
      reconnectBefore.sessionId, reconnectAfter.sessionId,
      reconnectBefore.sessionEpoch, reconnectAfter.sessionEpoch,
    ),
    null,
    'Hrana bridge reconnect invalidates counter delta',
  );
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
