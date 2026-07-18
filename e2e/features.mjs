// ABOUTME: Deployed smoke test for AiryFS authentication, archives, snapshots, uploads, jobs, exec, and changes.
// ABOUTME: Uses the public TypeScript SDK against an explicitly selected integration endpoint.

import {
  AiryFSApiError,
  AiryFSClient,
  resumableUploadBlob,
  waitForJob,
} from '../sdk/dist/index.js';

const endpoint = process.env.AIRYFS_URL;
if (!endpoint) throw new Error('AIRYFS_URL is required');

const suffix = `${Date.now()}-${crypto.randomUUID().slice(0, 8)}`;
const volume = `features-${suffix}`;
const cloneVolume = `features-clone-${suffix}`;
const client = new AiryFSClient(endpoint, volume);
const clone = new AiryFSClient(endpoint, cloneVolume);
let passed = 0;

try {
  await client.createVolume(256 * 1024);
  equal((await client.authStatus()).auth, 'disabled', 'auth-disabled integration mode');

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

  const largeBytes = new Uint8Array(1024 * 1024 + 3);
  crypto.getRandomValues(largeBytes.subarray(0, 65_536));
  const checksum = await sha256(largeBytes);
  await resumableUploadBlob(client, '/large.bin', new Blob([largeBytes]), checksum);
  equal((await client.checksum('/large.bin')).checksum, checksum, 'resumable upload checksum');

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

function equal(actual, expected, name) {
  assert(Object.is(actual, expected), `${name} (${JSON.stringify(actual)})`);
}
