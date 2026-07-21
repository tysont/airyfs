// ABOUTME: Runs deterministic quick or broad regression workloads in a deployed AiryFS Container.
// ABOUTME: Exercises realistic multi-runtime, native-build, streaming, and persistence workflows.

import assert from 'node:assert/strict';
import { AiryFSClient } from '../sdk/dist/index.js';
import { pythonCommand, shellQuote } from './benchmark-lib.mjs';
import { parseRegressionArgs, REGRESSION_PROFILES, regressionHelp } from './container-regression-lib.mjs';

const options = parseRegressionArgs(process.argv.slice(2));
if (options.help) {
  console.log(regressionHelp());
  process.exit(0);
}

const endpoint = process.env.AIRYFS_URL;
if (!endpoint) throw new Error('AIRYFS_URL is required');
const token = process.env.AIRYFS_TOKEN;
const suffix = `${Date.now()}-${crypto.randomUUID().slice(0, 8)}`;
const volume = `${options.prefix}-${options.profile}-${suffix}`;
const client = new AiryFSClient(endpoint, volume, token ? { token } : {});
const timeout = options.profile === 'broad' ? 300_000 : 120_000;
let checks = 0;

const scenarios = {
  environment: async () => {
    const result = await execJson(pythonCommand(`
import json, os, subprocess, sys
def version(command):
    return subprocess.check_output(command, text=True).strip()
print(json.dumps({
    'cwd': os.getcwd(),
    'home': os.environ.get('HOME'),
    'python': sys.version_info.major,
    'node': version(['node', '--version']),
    'npm': version(['npm', '--version']),
    'git': version(['git', '--version']),
}))
`));
    equal(result.cwd, '/volume', 'default working directory');
    equal(result.home, '/root', 'home directory');
    equal(result.python, 3, 'Python runtime');
    check(/^v22\./.test(result.node), `Node.js 22 runtime (${result.node})`);
    check(/^\d+\./.test(result.npm), `npm runtime (${result.npm})`);
    check(/^git version /.test(result.git), `Git runtime (${result.git})`);
  },

  'filesystem-lifecycle': async () => {
    await exec(pythonCommand(`
import pathlib, shutil
base = pathlib.Path('/volume/regression/basic')
shutil.rmtree(base, ignore_errors=True)
(base / 'nested').mkdir(parents=True)
(base / 'nested' / 'source.txt').write_text('alpha\\nbeta\\n')
`));
    await exec("chmod 0640 /volume/regression/basic/nested/source.txt && mv /volume/regression/basic/nested/source.txt /volume/regression/basic/nested/renamed.txt");
    await exec("ln /volume/regression/basic/nested/renamed.txt /volume/regression/basic/hardlink.txt");
    await exec("ln -s nested/renamed.txt /volume/regression/basic/symlink.txt");
    await exec("printf replacement > /volume/regression/basic/replacement.tmp");
    await exec("mv /volume/regression/basic/replacement.tmp /volume/regression/basic/nested/renamed.txt");
    const result = await execJson(pythonCommand(`
import json, os, pathlib, stat
base = pathlib.Path('/volume/regression/basic')
print(json.dumps({
    'body': (base / 'nested' / 'renamed.txt').read_text(),
    'hardlink_body': (base / 'hardlink.txt').read_text(),
    'symlink_body': (base / 'symlink.txt').read_text(),
    'mode': stat.S_IMODE((base / 'nested' / 'renamed.txt').stat().st_mode),
    'old_nlink': (base / 'hardlink.txt').stat().st_nlink,
    'target': os.readlink(base / 'symlink.txt'),
}))
`));
    equal(result.body, 'replacement', 'atomic file replacement');
    equal(result.hardlink_body, 'alpha\nbeta\n', 'open inode survives replacement via hard link');
    equal(result.symlink_body, 'replacement', 'relative symlink resolution');
    equal(result.mode, 0o644, 'replacement mode');
    equal(result.old_nlink, 1, 'replaced inode link count');
    equal(result.target, 'nested/renamed.txt', 'symlink target');
  },

  'api-fuse-coherence': async () => {
    await client.makeDirectory('/regression/coherence');
    await client.writeFile('/regression/coherence/value.txt', 'from-api');
    equal((await exec('cat /volume/regression/coherence/value.txt')).stdout, 'from-api', 'FUSE sees direct API write');
    await exec('printf from-fuse > /volume/regression/coherence/value.tmp && mv /volume/regression/coherence/value.tmp /volume/regression/coherence/value.txt');
    equal(await client.readFileText('/regression/coherence/value.txt'), 'from-fuse', 'direct API sees FUSE replacement');
    await client.writeFile('/regression/coherence/value.txt', 'api-again');
    equal((await exec('cat /volume/regression/coherence/value.txt')).stdout, 'api-again', 'FUSE cache sees direct overwrite');
  },

  'cross-runtime-data-flow': async () => {
    const nodeSource = `
const fs = require('node:fs');
fs.mkdirSync('/volume/regression/pipeline', { recursive: true });
const rows = Array.from({ length: 200 }, (_, id) => JSON.stringify({ id, value: id * id }));
fs.writeFileSync('/volume/regression/pipeline/input.ndjson', rows.join('\\n') + '\\n');
`;
    await exec(`node -e ${shellQuote(nodeSource)}`);
    const aggregate = await execJson(pythonCommand(`
import json, pathlib
source = pathlib.Path('/volume/regression/pipeline/input.ndjson')
rows = [json.loads(line) for line in source.read_text().splitlines()]
summary = {'count': len(rows), 'sum': sum(row['value'] for row in rows)}
pathlib.Path('/volume/regression/pipeline/summary.json').write_text(json.dumps(summary))
print(json.dumps(summary))
`));
    equal(aggregate.count, 200, 'Python reads Node.js output');
    equal(aggregate.sum, 2_646_700, 'Python aggregate');
    const nodeRead = await exec(`node -e ${shellQuote("process.stdout.write(require('node:fs').readFileSync('/volume/regression/pipeline/summary.json', 'utf8'))")}`);
    equal(JSON.parse(nodeRead.stdout).sum, aggregate.sum, 'Node.js reads Python output');
  },

  'sequential-and-random-io': async () => {
    const size = 2 * 1024 * 1024 + 123;
    const original = new Uint8Array(size);
    for (let index = 0; index < size; index++) original[index] = index % 251;
    const readOffsets = [0, 4093, 262113, 1048571, size - 8192];
    const expectedReadHashes = await Promise.all(readOffsets.map((offset) => sha256(original.slice(offset, offset + 8192))));
    const expected = new Uint8Array(size + 4096);
    expected.set(original.subarray(0, size - 777));
    expected.fill(82, 262113, 262113 + 4096);
    const expectedChecksum = await sha256(expected);
    const result = await execJson(pythonCommand(`
import hashlib, json, os, pathlib
path = pathlib.Path('/volume/regression/io.bin')
size = ${size}
data = bytes(index % 251 for index in range(size))
with path.open('wb', buffering=0) as handle:
    for offset in range(0, size, 65537):
        handle.write(data[offset:offset + 65537])
    os.fsync(handle.fileno())
handle = os.open(path, os.O_RDWR)
try:
    reads = [os.pread(handle, 8192, offset) for offset in (0, 4093, 262113, 1048571, size - 8192)]
    os.pwrite(handle, b'R' * 4096, 262113)
    os.ftruncate(handle, size - 777)
    os.ftruncate(handle, size + 4096)
    os.fsync(handle)
finally:
    os.close(handle)
final = path.read_bytes()
print(json.dumps({
    'read_hashes': [hashlib.sha256(value).hexdigest() for value in reads],
    'size': len(final),
    'patch': final[262113:262113 + 4096] == b'R' * 4096,
    'sparse_zeroes': final[size - 777:] == bytes(4873),
    'checksum': hashlib.sha256(final).hexdigest(),
}))
`));
    equal(result.read_hashes.length, 5, 'random pread count');
    deepEqual(result.read_hashes, expectedReadHashes, 'random pread contents');
    equal(result.size, size + 4096, 'truncate extension size');
    check(result.patch, 'random pwrite contents');
    check(result.sparse_zeroes, 'truncate extension zero fill');
    equal(result.checksum, expectedChecksum, 'FUSE checksum after mixed I/O');
    equal((await client.checksum('/regression/io.bin')).checksum, expectedChecksum, 'direct API checksum after mixed FUSE I/O');
  },

  'metadata-concurrency': async () => {
    const result = await execJson(pythonCommand(`
import concurrent.futures, hashlib, json, pathlib, shutil
base = pathlib.Path('/volume/regression/metadata')
shutil.rmtree(base, ignore_errors=True)
for index in range(6): (base / f'd{index}').mkdir(parents=True)
def write(index):
    body = (f'{index:02d}:' + 'x' * 4091).encode()
    path = base / f'd{index % 6}' / f'f{index:02d}.dat'
    path.write_bytes(body)
    return path, hashlib.sha256(body).hexdigest()
with concurrent.futures.ThreadPoolExecutor(max_workers=6) as executor:
    written = list(executor.map(write, range(24)))
def read(item):
    path, expected = item
    body = path.read_bytes()
    return len(body), hashlib.sha256(body).hexdigest() == expected
with concurrent.futures.ThreadPoolExecutor(max_workers=6) as executor:
    read_back = list(executor.map(read, written))
(base / 'd0').rename(base / 'moved')
files = sorted(path.relative_to(base).as_posix() for path in base.rglob('*.dat'))
print(json.dumps({'files': len(files), 'bytes': sum(size for size, _ in read_back), 'valid': all(valid for _, valid in read_back), 'moved': sum(path.startswith('moved/') for path in files)}))
`));
    equal(result.files, 24, 'concurrent file count');
    equal(result.bytes, 24 * 4094, 'concurrent byte count');
    check(result.valid, 'concurrent file contents');
    equal(result.moved, 4, 'non-empty directory rename');
  },

  'native-build': async () => {
    await exec(pythonCommand(`
import pathlib
base = pathlib.Path('/volume/regression/native')
base.mkdir(parents=True, exist_ok=True)
(base / 'main.cpp').write_text('''#include <fstream>\n#include <iostream>\nint main(int argc, char** argv) { int value = 0; std::ifstream(argv[1]) >> value; std::cout << value * value; }\n''')
(base / 'Makefile').write_text('all: app\\n\\napp: main.cpp\\n\\tg++ -std=c++17 -O2 -o app main.cpp\\n')
(base / 'input.txt').write_text('123')
`));
    const build = await exec('make -C /volume/regression/native && /volume/regression/native/app /volume/regression/native/input.txt > /volume/regression/native/output.txt');
    check(build.stdout.includes('g++'), 'make invokes native compiler');
    equal(await client.readFileText('/regression/native/output.txt'), '15129', 'compiled binary output');
  },

  'archive-round-trip': async () => {
    const result = await execJson(pythonCommand(`
import hashlib, json, pathlib, shutil, tarfile
base = pathlib.Path('/volume/regression/archive')
source, extracted = base / 'source', base / 'extracted'
shutil.rmtree(base, ignore_errors=True)
source.mkdir(parents=True)
expected = {}
for index in range(10):
    path = source / f'file-{index}.txt'
    path.write_text((f'line-{index}\\n') * (index + 1))
    expected[path.name] = hashlib.sha256(path.read_bytes()).hexdigest()
archive = base / 'bundle.tar.gz'
with tarfile.open(archive, 'w:gz') as bundle: bundle.add(source, arcname='source')
with tarfile.open(archive, 'r:gz') as bundle: bundle.extractall(extracted)
actual = {path.name: hashlib.sha256(path.read_bytes()).hexdigest() for path in (extracted / 'source').iterdir()}
print(json.dumps({'files': len(actual), 'matches': actual == expected, 'archive_bytes': archive.stat().st_size}))
`));
    equal(result.files, 10, 'archive extracted file count');
    check(result.matches, 'archive round-trip contents');
    check(result.archive_bytes > 0, 'archive has data');
  },

  'error-semantics': async () => {
    const result = await execJson(pythonCommand(`
import errno, json, os, pathlib
base = pathlib.Path('/volume/regression/errors')
base.mkdir(parents=True, exist_ok=True)
(base / 'file').write_text('body')
os.symlink('missing', base / 'dangling')
def error(call):
    try: call()
    except OSError as exception: return exception.errno
    return None
print(json.dumps({
    'missing': error(lambda: open(base / 'missing').read()),
    'directory': error(lambda: open(base).read()),
    'not_directory': error(lambda: open(base / 'file' / 'child').read()),
    'dangling_is_link': pathlib.Path(base / 'dangling').is_symlink(),
}))
`));
    equal(result.missing, 2, 'ENOENT propagation');
    equal(result.directory, 21, 'EISDIR propagation');
    equal(result.not_directory, 20, 'ENOTDIR propagation');
    check(result.dangling_is_link, 'dangling symlink lstat');
  },

  'streaming-exec': async () => {
    const source = "import sys\nfor i in range(64): print(f'out-{i:02d}', flush=True)\nfor i in range(16): print(f'err-{i:02d}', file=sys.stderr, flush=True)";
    const events = await client.execStream(pythonCommand(source), AbortSignal.timeout(timeout));
    let starts = 0;
    let exits = 0;
    let exitCode = null;
    let stdout = '';
    let stderr = '';
    for await (const event of events) {
      if (event.type === 'start') starts++;
      else if (event.type === 'exit') { exits++; exitCode = event.exitCode; }
      else if (event.type === 'stdout') stdout += Buffer.from(event.data, 'base64').toString();
      else if (event.type === 'stderr') stderr += Buffer.from(event.data, 'base64').toString();
    }
    equal(starts, 1, 'stream start event');
    equal(exits, 1, 'stream exit event');
    equal(exitCode, 0, 'stream exit code');
    equal(stdout, Array.from({ length: 64 }, (_, index) => `out-${String(index).padStart(2, '0')}`).join('\n') + '\n', 'stream stdout contents');
    equal(stderr, Array.from({ length: 16 }, (_, index) => `err-${String(index).padStart(2, '0')}`).join('\n') + '\n', 'stream stderr contents');
  },

  'restart-persistence': async () => {
    await client.writeFile('/regression/persist-api.txt', 'api-persisted');
    await exec('printf fuse-persisted > /volume/regression/persist-fuse.txt');
    const before = await client.perf();
    await client.destroyContainer();
    const result = await execJson(pythonCommand(`
import json, pathlib
base = pathlib.Path('/volume/regression')
print(json.dumps({'api': (base / 'persist-api.txt').read_text(), 'fuse': (base / 'persist-fuse.txt').read_text()}))
`));
    const after = await client.perf();
    equal(result.api, 'api-persisted', 'direct API write persists across restart');
    equal(result.fuse, 'fuse-persisted', 'FUSE write persists across restart');
    check(after.sessionId !== before.sessionId, 'bridge session changes across restart');
    check(after.sessionEpoch > before.sessionEpoch, 'bridge epoch advances across restart');
  },
};

let completed = false;
try {
  await client.createVolume(256 * 1024);
  await client.makeDirectory('/regression');
  console.log(`Container regression ${options.profile}: ${volume}`);
  for (const name of REGRESSION_PROFILES[options.profile]) {
    const started = performance.now();
    const before = checks;
    try {
      await scenarios[name]();
      console.log(`  PASS ${name} (${checks - before} checks, ${Math.round(performance.now() - started)} ms)`);
    } catch (error) {
      error.message = `${name}: ${error.message}`;
      throw error;
    }
  }
  completed = true;
} finally {
  const cleanupErrors = [];
  try {
    await client.removeDirectory('/regression', true, true);
  } catch (error) {
    cleanupErrors.push(error);
  }
  try {
    await client.destroyContainer();
  } catch (error) {
    cleanupErrors.push(error);
  }
  if (cleanupErrors.length > 0) {
    if (completed) throw new AggregateError(cleanupErrors, 'Container regression cleanup failed');
    console.warn(`Container regression cleanup also failed: ${cleanupErrors.map((error) => error.message).join('; ')}`);
  }
}
console.log(`Container regression passed: ${checks} checks across ${REGRESSION_PROFILES[options.profile].length} scenarios`);

async function exec(command) {
  const result = await client.exec(command, AbortSignal.timeout(timeout));
  equal(result.exitCode, 0, `command succeeds; stderr=${result.stderr.trim()}`);
  return result;
}

async function execJson(command) {
  const result = await exec(command);
  const line = result.stdout.trim().split('\n').filter(Boolean).at(-1);
  if (!line) throw new Error('command returned no JSON result');
  return JSON.parse(line);
}

function check(condition, message) {
  assert.ok(condition, message);
  checks++;
}

function equal(actual, expected, message) {
  assert.equal(actual, expected, message);
  checks++;
}

function deepEqual(actual, expected, message) {
  assert.deepEqual(actual, expected, message);
  checks++;
}

async function sha256(bytes) {
  return Buffer.from(await crypto.subtle.digest('SHA-256', bytes)).toString('hex');
}
