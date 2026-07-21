// ABOUTME: Benchmarks representative direct and Container/FUSE workloads against a deployed AiryFS.
// ABOUTME: Separates in-command operation time from client-observed exec latency and records Hrana amplification.

import { readFile, writeFile } from 'node:fs/promises';
import { execFileSync } from 'node:child_process';
import { AiryFSClient } from '../sdk/dist/index.js';
import {
  MIB,
  PROFILES,
  benchmarkHelp,
  counterDelta,
  latencyPercentiles,
  parseBenchmarkArgs,
  parseCommandResult,
  pythonCommand,
  scoreReport,
  summarizeSamples,
} from './benchmark-lib.mjs';

const options = parseBenchmarkArgs(process.argv.slice(2));
if (options.help) {
  console.log(benchmarkHelp());
  process.exit(0);
}

const endpoint = process.env.AIRYFS_URL;
if (!endpoint) throw new Error('AIRYFS_URL is required');
const token = process.env.AIRYFS_TOKEN;
const profile = PROFILES[options.profile];
const negativeLookups = 20;
const fsyncCalls = 20;
const truncateCalls = 20;
const renameCalls = 20;
const execCalls = 5;
const samples = [];
const volumes = [];

for (const chunkSize of options.chunkSizes) {
  const volume = `${options.prefix}-${chunkSize}`;
  const client = new AiryFSClient(endpoint, volume, token ? { token } : {});
  volumes.push({ volume, chunkSize });
  progress(`Preparing ${volume}`);
  const configured = await client.createVolume(chunkSize);
  if (configured.chunkSize !== chunkSize) {
    throw new Error(`${volume} uses chunk size ${configured.chunkSize}, expected ${chunkSize}`);
  }

  let ownsBenchmarkRoot = false;
  try {
    await prepareBenchmarkRoot(client, { prefix: options.prefix, chunkSize });
    ownsBenchmarkRoot = true;
    const initialUsage = await client.usage();
    const startupSamples = [];
    for (let startupRun = 0; startupRun < profile.startupRuns; startupRun++) {
      await client.destroyContainer();
      const started = performance.now();
      await checkedExec(client, ':');
      startupSamples.push(performance.now() - started);
    }

    const gitPath = '/bench/repo';
    if (selected('git')) {
      progress(`${volume}: git fixture setup`);
      await checkedExec(client, gitSetupCommand(gitPath));
      await prepareGitObjectFanout(client, gitPath);
      await checkedExec(client, `printf initial > ${fusePath(gitPath)}/file-000000.txt`);
      await runStreamingCommand(client, `git -C ${fusePath(gitPath)} add -A && git -C ${fusePath(gitPath)} -c user.name=airyfs -c user.email=airyfs@test -c maintenance.auto=false commit -q --no-verify -m fixture`);
    }

    for (let run = 1; run <= options.runs; run++) {
      const root = `/bench/run-${run}`;
      await client.makeDirectory(root);
      progress(`${volume}: sample ${run}/${options.runs}`);

      if (selected('direct-sequential')) {
        for (const bytes of profile.sequentialSizes) {
          const path = `${root}/direct-${bytes}.bin`;
          const body = repeatedBlob(bytes);
          const expectedChecksum = await sha256Blob(body);
          const writeStarted = performance.now();
          await client.writeFile(path, body);
          const writeMs = performance.now() - writeStarted;
          assert((await client.checksum(path)).checksum === expectedChecksum, `${path} direct write checksum`);
          samples.push(directSample('direct_write', chunkSize, run, bytes, writeMs));

          const readStarted = performance.now();
          const read = await client.readFileBytes(path);
          const readMs = performance.now() - readStarted;
          assert(read.byteLength === bytes, `${path} direct read length`);
          assert(await sha256Bytes(read) === expectedChecksum, `${path} direct read checksum`);
          samples.push(directSample('direct_read', chunkSize, run, bytes, readMs));
        }
      }

      if (selected('fuse-sequential')) {
        for (const bytes of profile.sequentialSizes) {
          const readPath = `${root}/fuse-read-${bytes}.bin`;
          const readBody = repeatedBlob(bytes);
          const expectedChecksum = await sha256Blob(readBody);
          await client.writeFile(readPath, readBody);
          const readSample = await runExecSample(client, {
            name: `fuse_sequential_read_${sizeLabel(bytes)}`,
            chunkSize, run,
            command: sequentialReadCommand(readPath, bytes),
          });
          assert(readSample.result.sha256 === expectedChecksum, `${readPath} FUSE read checksum`);
          samples.push(readSample);

          const writePath = `${root}/fuse-write-${bytes}.bin`;
          const written = await runExecSample(client, {
            name: `fuse_sequential_write_${sizeLabel(bytes)}`,
            chunkSize, run,
            command: sequentialWriteCommand(writePath, bytes),
          });
          const head = await client.headFile(writePath);
          assert(Number(head.headers.get('Content-Length')) === bytes, `${writePath} FUSE write length`);
          const expectedWriteChecksum = await sha256Blob(sequentialWriteBlob(bytes));
          assert((await client.checksum(writePath)).checksum === expectedWriteChecksum, `${writePath} direct write checksum`);
          samples.push(written);
        }
      }

      if (selected('fuse-random')) {
        const readPath = `${root}/random-read.bin`;
        await client.writeFile(readPath, repeatedBlob(profile.randomFileBytes));
        samples.push(await runExecSample(client, {
          name: 'fuse_random_read_4k', chunkSize, run,
          command: randomReadCommand(readPath, profile.randomFileBytes, profile.randomOperations),
        }));

        const writePath = `${root}/random-write.bin`;
        await client.writeFile(writePath, new Blob([new Uint8Array(profile.randomFileBytes)]));
        const randomWrite = await runExecSample(client, {
          name: 'fuse_random_write_4k', chunkSize, run,
          command: randomWriteCommand(writePath, profile.randomFileBytes, profile.randomOperations),
        });
        const head = await client.headFile(writePath);
        assert(Number(head.headers.get('Content-Length')) === profile.randomFileBytes, `${writePath} random write length`);
        const expectedRandomWrite = randomWriteBytes(profile.randomFileBytes, profile.randomOperations);
        assert((await client.checksum(writePath)).checksum === await sha256Bytes(expectedRandomWrite), `${writePath} random write checksum`);
        samples.push(randomWrite);
      }

      if (selected('metadata')) {
        const path = `${root}/metadata`;
        await client.makeDirectory(path);
        await checkedExec(client, smallFileSetupCommand(path, profile.metadataFiles));
        const cold = await runExecSample(client, {
          name: 'fuse_metadata_walk_after_create', chunkSize, run,
          command: metadataWalkCommand(path, profile.metadataFiles),
        });
        assert(cold.result.entries === profile.metadataFiles, `${path} first metadata count`);
        samples.push(cold);

        const warm = await runExecSample(client, {
          name: 'fuse_metadata_walk_warm', chunkSize, run,
          command: metadataWalkCommand(path, profile.metadataFiles),
        });
        assert(warm.result.entries === profile.metadataFiles, `${path} warm metadata count`);
        samples.push(warm);
      }

      if (selected('negative-lookups')) {
        const missingPath = `${root}/missing-negative-entry`;
        const misses = await runExecSample(client, {
          name: 'fuse_negative_lookup_repeated', chunkSize, run,
          command: negativeLookupCommand(missingPath, negativeLookups),
        });
        assert(misses.result.misses === negativeLookups, `${missingPath} missing lookup count`);
        samples.push(misses);
      }

      if (selected('fsync')) {
        const controlPath = `${root}/fsync-control.txt`;
        const path = `${root}/fsync-repeated.txt`;
        await checkedExec(client, `printf x > ${fusePath(controlPath)} && printf x > ${fusePath(path)} && stat ${fusePath(controlPath)} ${fusePath(path)} >/dev/null`);
        const control = await runExecSample(client, {
          name: 'fuse_fsync_open_close', chunkSize, run,
          command: fsyncCommand(controlPath, 0),
        });
        assert(control.result.operations === 0, `${controlPath} fsync control count`);
        samples.push(control);
        const synced = await runExecSample(client, {
          name: 'fuse_fsync_repeated', chunkSize, run,
          command: fsyncCommand(path, fsyncCalls),
        });
        assert(synced.result.operations === fsyncCalls, `${path} fsync count`);
        samples.push(synced);
      }

      if (selected('truncate')) {
        const controlPath = `${root}/truncate-control.bin`;
        const path = `${root}/truncate-repeated.bin`;
        const body = new Blob([new Uint8Array(profile.randomFileBytes)]);
        await client.writeFile(controlPath, body);
        await client.writeFile(path, body);
        const control = await runExecSample(client, {
          name: 'fuse_truncate_open_close', chunkSize, run,
          command: truncateCommand(controlPath, 0, profile.randomFileBytes, chunkSize),
        });
        assert(control.result.operations === 0, `${controlPath} truncate control count`);
        samples.push(control);
        const truncated = await runExecSample(client, {
          name: 'fuse_truncate_repeated', chunkSize, run,
          command: truncateCommand(path, truncateCalls, profile.randomFileBytes, chunkSize),
        });
        assert(truncated.result.operations === truncateCalls, `${path} truncate count`);
        assert(Number((await client.headFile(path)).headers.get('Content-Length')) === profile.randomFileBytes, `${path} final truncate length`);
        samples.push(truncated);
      }

      if (selected('rename')) {
        const controlPath = `${root}/rename-control-a`;
        const path = `${root}/rename-repeated-a`;
        await client.writeFile(controlPath, 'control');
        await client.writeFile(path, 'renamed');
        const control = await runExecSample(client, {
          name: 'fuse_rename_open_close', chunkSize, run,
          command: renameCommand(controlPath, 0),
        });
        assert(control.result.operations === 0, `${controlPath} rename control count`);
        samples.push(control);
        const renamed = await runExecSample(client, {
          name: 'fuse_rename_repeated', chunkSize, run,
          command: renameCommand(path, renameCalls),
        });
        assert(renamed.result.operations === renameCalls, `${path} rename count`);
        assert(await client.readFileText(path) === 'renamed', `${path} final rename contents`);
        samples.push(renamed);
      }

      if (selected('exec')) {
        samples.push(await runRepeatedExecSample(client, {
          name: 'container_exec_noop_repeated', chunkSize, run, count: execCalls,
        }));
      }

      if (selected('small-files')) {
        const path = `${root}/small-files`;
        await client.makeDirectory(path);
        const created = await runExecSample(client, {
          name: 'fuse_small_file_create', chunkSize, run,
          command: smallFileCreateCommand(path, profile.smallFiles),
        });
        assert(created.result.entries === profile.smallFiles, `${path} created file count`);
        assert((await client.listDirectory(path)).length === profile.smallFiles, `${path} direct listing count`);
        samples.push(created);
      }

      if (selected('git')) {
        const path = gitPath;
        progress(`${volume}: git status`);
        const status = await runClientSample(client, {
          name: 'git_status_clean', chunkSize, run,
          command: `git -C ${fusePath(path)} status --porcelain --untracked-files=all`,
          operations: 1,
        });
        assert(status.result.stdout.length === 0, `${path} clean git status`);
        samples.push(status);

        progress(`${volume}: git add/commit`);
        await checkedExec(client, `printf changed > ${fusePath(path)}/file-000000.txt`);
        const commit = await runClientSample(client, {
          name: 'git_add_commit', chunkSize, run,
          command: `mkdir -p ${fusePath(path)}/.git/objects/info ${fusePath(path)}/.git/objects/pack && git -C ${fusePath(path)} add -A && git -C ${fusePath(path)} -c user.name=airyfs -c user.email=airyfs@test -c maintenance.auto=false commit -q --no-verify -m change`,
          operations: 1,
          commits: 1,
          streaming: true,
        });
        samples.push(commit);

        progress(`${volume}: git checkout`);
        const checkout = await runClientSample(client, {
          name: 'git_checkout', chunkSize, run,
          command: `git -C ${fusePath(path)} checkout -q HEAD~1`,
          operations: 1,
          streaming: true,
        });
        await checkedExec(client, `test "$(cat ${fusePath(path)}/file-000000.txt)" = initial`);
        samples.push(checkout);
      }
    }

    const usage = await client.usage();
    volumes.at(-1).startupMs = latencyPercentiles(startupSamples);
    volumes.at(-1).startupSamples = startupSamples;
    volumes.at(-1).sqliteBytesBefore = initialUsage.sqliteBytes;
    volumes.at(-1).sqliteBytesAfter = usage.sqliteBytes;
    volumes.at(-1).sqliteBytesGrowth = usage.sqliteBytes - initialUsage.sqliteBytes;
    volumes.at(-1).filesystemBytesGrowth = usage.filesystem.bytesUsed - initialUsage.filesystem.bytesUsed;
    volumes.at(-1).inodeGrowth = usage.filesystem.inodes - initialUsage.filesystem.inodes;
  } finally {
    if (ownsBenchmarkRoot) await removeIfPresent(client, '/bench');
    await client.destroyContainer().catch(() => undefined);
  }
}

const report = {
  schemaVersion: 1,
  generatedAt: new Date().toISOString(),
  endpoint: new URL(endpoint).origin,
  label: options.label,
  harnessRevision: gitOutput(['rev-parse', 'HEAD']),
  harnessDirty: gitOutput(['status', '--porcelain']).length > 0,
  profile: options.profile,
  runs: options.runs,
  scenarios: options.scenarios,
  configuration: {
    ...profile,
    ...(selected('negative-lookups') ? { negativeLookups } : {}),
    ...(selected('fsync') ? { fsyncCalls } : {}),
    ...(selected('truncate') ? { truncateCalls } : {}),
    ...(selected('rename') ? { renameCalls } : {}),
    ...(selected('exec') ? { execCalls } : {}),
  },
  volumes,
  samples,
  summary: summarizeSamples(samples),
};
const baseline = options.baseline ? JSON.parse(await readFile(options.baseline, 'utf8')) : report;
report.score = {
  ...scoreReport(baseline, report),
  baseline: options.baseline ?? 'self',
};
const serialized = `${JSON.stringify(report, null, 2)}\n`;
if (options.output) await writeFile(options.output, serialized);
if (!options.json) {
  printSummary(report.summary);
  printScore(report.score);
}
process.stdout.write(serialized);

function selected(name) {
  return options.scenarios.includes(name);
}

async function runExecSample(client, input) {
  const before = await client.perf();
  const started = performance.now();
  const execution = await checkedExec(client, input.command);
  const clientMs = performance.now() - started;
  const result = parseCommandResult(execution.stdout);
  const perf = await client.perf();
  const bytes = Number.isFinite(result.bytes) ? result.bytes : null;
  const operations = Number.isFinite(result.operations) ? result.operations : null;
  const commits = Number.isFinite(result.commits) ? result.commits : null;
  return {
    name: input.name,
    chunkSize: input.chunkSize,
    run: input.run,
    operationMs: result.operationMs,
    clientMs,
    bytes,
    operations,
    throughputMiBps: bytes === null || result.operationMs === 0
      ? null
      : bytes / MIB / (result.operationMs / 1000),
    operationsPerSecond: operations === null || result.operationMs === 0
      ? null
      : operations / (result.operationMs / 1000),
    commitsPerSecond: commits === null || result.operationMs === 0
      ? null
      : commits / (result.operationMs / 1000),
    pipelineRequests: counterDelta(
      before.pipelineRequests, perf.pipelineRequests,
      before.sessionId, perf.sessionId,
      before.sessionEpoch, perf.sessionEpoch,
    ),
    sqlStatements: counterDelta(
      before.sqlStatements, perf.sqlStatements,
      before.sessionId, perf.sessionId,
      before.sessionEpoch, perf.sessionEpoch,
    ),
    result,
  };
}

async function runClientSample(client, input) {
  const before = await client.perf();
  const started = performance.now();
  let stdout = '';
  if (input.streaming) {
    stdout = await runStreamingCommand(client, input.command);
  } else {
    stdout = (await checkedExec(client, input.command)).stdout;
  }
  const clientMs = performance.now() - started;
  const perf = await client.perf();
  return {
    name: input.name,
    chunkSize: input.chunkSize,
    run: input.run,
    operationMs: clientMs,
    clientMs,
    bytes: null,
    operations: input.operations,
    throughputMiBps: null,
    operationsPerSecond: input.operations / (clientMs / 1000),
    commitsPerSecond: input.commits ? input.commits / (clientMs / 1000) : null,
    pipelineRequests: counterDelta(
      before.pipelineRequests, perf.pipelineRequests,
      before.sessionId, perf.sessionId,
      before.sessionEpoch, perf.sessionEpoch,
    ),
    sqlStatements: counterDelta(
      before.sqlStatements, perf.sqlStatements,
      before.sessionId, perf.sessionId,
      before.sessionEpoch, perf.sessionEpoch,
    ),
    result: { operationMs: clientMs, operations: input.operations, commits: input.commits ?? null, stdout },
  };
}

async function runRepeatedExecSample(client, input) {
  const before = await client.perf();
  const started = performance.now();
  for (let index = 0; index < input.count; index++) {
    const result = await client.exec('true');
    assert(result.exitCode === 0 && result.stdout === '' && result.stderr === '', `warmed exec ${index + 1}`);
  }
  const clientMs = performance.now() - started;
  const perf = await client.perf();
  return {
    name: input.name,
    chunkSize: input.chunkSize,
    run: input.run,
    operationMs: clientMs,
    clientMs,
    bytes: null,
    operations: input.count,
    throughputMiBps: null,
    operationsPerSecond: input.count / (clientMs / 1000),
    commitsPerSecond: null,
    pipelineRequests: counterDelta(
      before.pipelineRequests, perf.pipelineRequests,
      before.sessionId, perf.sessionId,
      before.sessionEpoch, perf.sessionEpoch,
    ),
    sqlStatements: counterDelta(
      before.sqlStatements, perf.sqlStatements,
      before.sessionId, perf.sessionId,
      before.sessionEpoch, perf.sessionEpoch,
    ),
    result: { operationMs: clientMs, operations: input.count },
  };
}

async function runStreamingCommand(client, command) {
  const events = await client.execStream(command);
  let stdout = '';
  let stderr = '';
  let exitCode = null;
  for await (const event of events) {
    if (event.type === 'stdout') stdout += Buffer.from(event.data, 'base64').toString('utf8');
    if (event.type === 'stderr') stderr += Buffer.from(event.data, 'base64').toString('utf8');
    if (event.type === 'exit') exitCode = event.exitCode;
  }
  if (exitCode !== 0) throw new Error(`Streaming command failed (${exitCode}): ${stderr}`);
  return stdout;
}

function directSample(name, chunkSize, run, bytes, operationMs) {
  return {
    name: `${name}_${sizeLabel(bytes)}`,
    chunkSize, run, operationMs, clientMs: operationMs, bytes, operations: 1,
    throughputMiBps: bytes / MIB / (operationMs / 1000),
    operationsPerSecond: 1000 / operationMs,
    commitsPerSecond: null,
    pipelineRequests: null,
    sqlStatements: null,
    result: { operationMs, bytes, operations: 1 },
  };
}

async function checkedExec(client, command) {
  const result = await client.exec(command);
  if (result.exitCode !== 0) throw new Error(`Command failed (${result.exitCode}): ${result.stderr}`);
  return result;
}

async function removeIfPresent(client, path) {
  try {
    await client.removeDirectory(path, true, true);
  } catch (error) {
    if (error?.code !== 'ENOENT') throw error;
  }
}

async function prepareBenchmarkRoot(client, owner) {
  const ownerPath = '/bench/.airyfs-benchmark-owner';
  let savedOwner = null;
  try {
    savedOwner = JSON.parse(await client.readFileText(ownerPath));
  } catch (error) {
    if (error?.code !== 'ENOENT') throw error;
  }
  if (savedOwner === null) {
    try {
      await client.listDirectory('/bench');
      throw new Error('Refusing to remove an unowned /bench tree');
    } catch (error) {
      if (error?.code !== 'ENOENT') throw error;
    }
  } else if (
    savedOwner.schemaVersion !== 1
    || savedOwner.prefix !== owner.prefix
    || savedOwner.chunkSize !== owner.chunkSize
  ) {
    throw new Error('Benchmark volume ownership marker does not match this run');
  }
  await removeIfPresent(client, '/bench');
  await client.makeDirectory('/bench');
  await client.writeFile(ownerPath, JSON.stringify({ schemaVersion: 1, ...owner }));
}

async function prepareGitObjectFanout(client, path) {
  const directories = Array.from({ length: 256 }, (_, index) => `${path}/.git/objects/${index.toString(16).padStart(2, '0')}`);
  for (let index = 0; index < directories.length; index += 16) {
    await Promise.all(directories.slice(index, index + 16).map(async (directory) => {
      try {
        await client.makeDirectory(directory);
      } catch (error) {
        if (error?.code !== 'EEXIST') throw error;
      }
    }));
  }
}

function sequentialReadCommand(path, bytes) {
  return pythonCommand(`
import hashlib, json, time
path = ${JSON.stringify(fusePath(path))}
expected = ${bytes}
digest = hashlib.sha256()
read = 0
started = time.perf_counter_ns()
handle = open(path, 'rb', buffering=0)
elapsed_ns = time.perf_counter_ns() - started
try:
    while True:
        started = time.perf_counter_ns()
        chunk = handle.read(1024 * 1024)
        elapsed_ns += time.perf_counter_ns() - started
        if not chunk: break
        digest.update(chunk)
        read += len(chunk)
finally:
    handle.close()
elapsed = elapsed_ns / 1e6
assert read == expected, (read, expected)
print(json.dumps({'operationMs': elapsed, 'bytes': read, 'operations': 1, 'sha256': digest.hexdigest()}))
`);
}

function sequentialWriteCommand(path, bytes) {
  return pythonCommand(`
import json, os, time
path = ${JSON.stringify(fusePath(path))}
expected = ${bytes}
block = b'airyfs-benchmark-' * 65536
written = 0
start = time.perf_counter_ns()
with open(path, 'wb', buffering=0) as handle:
    while written < expected:
        chunk = block[:min(len(block), expected - written)]
        handle.write(chunk)
        written += len(chunk)
    os.fsync(handle.fileno())
elapsed = (time.perf_counter_ns() - start) / 1e6
print(json.dumps({'operationMs': elapsed, 'bytes': written, 'operations': 1}))
`);
}

function randomReadCommand(path, fileBytes, operations) {
  return pythonCommand(`
import json, os, time
path = ${JSON.stringify(fusePath(path))}
size = ${fileBytes}
operations = ${operations}
offsets = [(index * 104729) % (size - 4096 + 1) for index in range(operations)]
expected = [bytes(((offset + index) % 65536) % 251 for index in range(4096)) for offset in offsets]
handle = os.open(path, os.O_RDONLY)
read = 0
elapsed_ns = 0
try:
    for offset, wanted in zip(offsets, expected):
        started = time.perf_counter_ns()
        data = os.pread(handle, 4096, offset)
        elapsed_ns += time.perf_counter_ns() - started
        assert len(data) == 4096
        assert data == wanted
        read += len(data)
finally:
    os.close(handle)
elapsed = elapsed_ns / 1e6
print(json.dumps({'operationMs': elapsed, 'bytes': read, 'operations': operations}))
`);
}

function randomWriteCommand(path, fileBytes, operations) {
  return pythonCommand(`
import json, os, time
path = ${JSON.stringify(fusePath(path))}
size = ${fileBytes}
operations = ${operations}
offsets = [(index * 104729) % (size - 4096 + 1) for index in range(operations)]
payload = b'R' * 4096
handle = os.open(path, os.O_RDWR)
start = time.perf_counter_ns()
try:
    for offset in offsets:
        assert os.pwrite(handle, payload, offset) == 4096
    os.fsync(handle)
finally:
    os.close(handle)
elapsed = (time.perf_counter_ns() - start) / 1e6
print(json.dumps({'operationMs': elapsed, 'bytes': operations * 4096, 'operations': operations}))
`);
}

function metadataWalkCommand(path, expected) {
  return pythonCommand(`
import json, os, time
path = ${JSON.stringify(fusePath(path))}
expected = ${expected}
entries = 0
bytes_seen = 0
start = time.perf_counter_ns()
with os.scandir(path) as iterator:
    for entry in iterator:
        stats = entry.stat(follow_symlinks=False)
        entries += 1
        bytes_seen += stats.st_size
elapsed = (time.perf_counter_ns() - start) / 1e6
assert entries == expected, (entries, expected)
print(json.dumps({'operationMs': elapsed, 'operations': entries, 'entries': entries, 'bytes': bytes_seen}))
`);
}

function smallFileCreateCommand(path, count) {
  return pythonCommand(`
import json, os, time
path = ${JSON.stringify(fusePath(path))}
count = ${count}
start = time.perf_counter_ns()
for index in range(count):
    with open(f'{path}/file-{index:06d}.txt', 'wb', buffering=0) as handle:
        handle.write(b'x')
elapsed = (time.perf_counter_ns() - start) / 1e6
print(json.dumps({'operationMs': elapsed, 'operations': count, 'entries': len(os.listdir(path)), 'bytes': count}))
`);
}

function negativeLookupCommand(path, count) {
  return pythonCommand(`
import json, os, time
path = ${JSON.stringify(fusePath(path))}
count = ${count}
misses = 0
start = time.perf_counter_ns()
for _ in range(count):
    try:
        os.stat(path)
    except FileNotFoundError:
        misses += 1
elapsed = (time.perf_counter_ns() - start) / 1e6
assert misses == count, (misses, count)
print(json.dumps({'operationMs': elapsed, 'operations': count, 'misses': misses}))
`);
}

function fsyncCommand(path, count) {
  return pythonCommand(`
import json, os, time
path = ${JSON.stringify(fusePath(path))}
count = ${count}
handle = os.open(path, os.O_RDWR)
start = time.perf_counter_ns()
try:
    for _ in range(count):
        os.fsync(handle)
finally:
    os.close(handle)
elapsed = (time.perf_counter_ns() - start) / 1e6
print(json.dumps({'operationMs': elapsed, 'operations': count}))
`);
}

function truncateCommand(path, count, fullSize, chunkSize) {
  return pythonCommand(`
import json, os, time
path = ${JSON.stringify(fusePath(path))}
count = ${count}
full_size = ${fullSize}
short_size = ${chunkSize * 8 + 123}
handle = os.open(path, os.O_RDWR)
start = time.perf_counter_ns()
try:
    for index in range(count):
        os.ftruncate(handle, short_size if index % 2 == 0 else full_size)
finally:
    os.close(handle)
elapsed = (time.perf_counter_ns() - start) / 1e6
print(json.dumps({'operationMs': elapsed, 'operations': count}))
`);
}

function renameCommand(path, count) {
  return pythonCommand(`
import json, os, time
first = ${JSON.stringify(fusePath(path))}
second = first[:-1] + 'b'
count = ${count}
current, target = first, second
start = time.perf_counter_ns()
for _ in range(count):
    os.rename(current, target)
    current, target = target, current
elapsed = (time.perf_counter_ns() - start) / 1e6
print(json.dumps({'operationMs': elapsed, 'operations': count}))
`);
}

function smallFileSetupCommand(path, count) {
  return pythonCommand(`
import os
path = ${JSON.stringify(fusePath(path))}
for index in range(${count}):
    with open(f'{path}/file-{index:06d}.txt', 'wb', buffering=0) as handle:
        handle.write(b'x')
`);
}

function gitSetupCommand(path) {
  const target = fusePath(path);
  return `mkdir ${target} && git init -q --template= ${target} && mkdir -p ${target}/.git/objects/info ${target}/.git/objects/pack ${target}/.git/refs/heads ${target}/.git/logs/refs/heads && git -C ${target} -c user.name=airyfs -c user.email=airyfs@test -c maintenance.auto=false commit -q --allow-empty --no-verify -m initial`;
}

function repeatedBlob(size) {
  const block = new Uint8Array(Math.min(64 * 1024, size));
  for (let index = 0; index < block.length; index++) block[index] = index % 251;
  const chunks = [];
  let remaining = size;
  while (remaining > 0) {
    chunks.push(block.subarray(0, Math.min(block.length, remaining)));
    remaining -= block.length;
  }
  return new Blob(chunks);
}

function randomWriteBytes(size, operations) {
  const bytes = new Uint8Array(size);
  for (let index = 0; index < operations; index++) {
    const offset = (index * 104729) % (size - 4096 + 1);
    bytes.fill(82, offset, offset + 4096);
  }
  return bytes;
}

function sequentialWriteBlob(size) {
  const block = new TextEncoder().encode('airyfs-benchmark-'.repeat(65536));
  const chunks = [];
  let remaining = size;
  while (remaining > 0) {
    chunks.push(block.subarray(0, Math.min(block.length, remaining)));
    remaining -= block.length;
  }
  return new Blob(chunks);
}

async function sha256Blob(blob) {
  return sha256Bytes(new Uint8Array(await blob.arrayBuffer()));
}

async function sha256Bytes(bytes) {
  const digest = await crypto.subtle.digest('SHA-256', bytes);
  return [...new Uint8Array(digest)].map((byte) => byte.toString(16).padStart(2, '0')).join('');
}

function sizeLabel(bytes) {
  return bytes % MIB === 0 ? `${bytes / MIB}m` : `${bytes}b`;
}

function fusePath(path) {
  return `/volume${path}`;
}

function printSummary(summary) {
  console.table(summary.map((entry) => ({
    chunk: entry.chunkSize,
    scenario: entry.name,
    samples: entry.samples,
    operationP50: round(entry.operationMsP50),
    clientP50: round(entry.clientMsP50),
    clientP95: round(entry.clientMsP95),
    clientP99: round(entry.clientMsP99),
    MiBps: round(entry.throughputMiBpsMedian),
    opsPerSec: round(entry.operationsPerSecondMedian),
    commitsPerSec: round(entry.commitsPerSecondP50),
    pipelines: round(entry.pipelineRequestsMedian),
    statements: round(entry.sqlStatementsMedian),
  })));
}

function printScore(score) {
  console.error(`Performance score: ${score.overall} (baseline: ${score.baseline})`);
  console.table(Object.entries(score.groups).map(([group, value]) => ({ group, score: value })));
}

function round(value) {
  return value === null ? null : Math.round(value * 100) / 100;
}

function progress(message) {
  console.error(`[benchmark] ${message}`);
}

function gitOutput(args) {
  try {
    return execFileSync('git', args, { encoding: 'utf8' }).trim();
  } catch {
    return '';
  }
}

function assert(condition, message) {
  if (!condition) throw new Error(`Benchmark correctness check failed: ${message}`);
}
