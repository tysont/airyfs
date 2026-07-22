// ABOUTME: Tests deployed benchmark parsing and summaries without requiring a deployment.
// ABOUTME: Prevents benchmark tooling changes from silently changing result interpretation.

import assert from 'node:assert/strict';
import test from 'node:test';
import {
  commandDiagnostics,
  compareSummaries,
  counterDelta,
  parseBenchmarkArgs,
  parseCommandResult,
  pythonCommand,
  scoreReport,
  summarizeSamples,
  validateComparableReports,
} from './benchmark-lib.mjs';

test('parses benchmark selections and validates chunk sizes', () => {
  assert.deepEqual(parseBenchmarkArgs([
    '--profile', 'full',
    '--chunk-sizes', '4096,65536,262144',
    '--runs', '2',
    '--scenarios', 'metadata,git',
    '--prefix', 'experiment-one',
    '--json',
  ]), {
    profile: 'full',
    chunkSizes: [4096, 65536, 262144],
    runs: 2,
    scenarios: ['metadata', 'git'],
    prefix: 'experiment-one',
    label: null,
    json: true,
    output: null,
    baseline: null,
    help: false,
  });
  assert.throws(() => parseBenchmarkArgs(['--chunk-sizes', '5000']), /Invalid chunk size/);
  assert.throws(() => parseBenchmarkArgs(['--scenarios', 'unknown']), /Unknown scenarios/);
  assert.throws(() => parseBenchmarkArgs(['--scenarios', ',,']), /At least one scenario/);
  assert.deepEqual(parseBenchmarkArgs(['--scenarios', 'negative-lookups']).scenarios, ['negative-lookups']);
  assert.deepEqual(parseBenchmarkArgs(['--scenarios', 'fsync']).scenarios, ['fsync']);
  assert.deepEqual(parseBenchmarkArgs(['--scenarios', 'truncate']).scenarios, ['truncate']);
  assert.deepEqual(parseBenchmarkArgs(['--scenarios', 'rename']).scenarios, ['rename']);
  assert.deepEqual(parseBenchmarkArgs(['--scenarios', 'exec']).scenarios, ['exec']);
  assert.equal(parseBenchmarkArgs([]).scenarios.includes('negative-lookups'), false);
  assert.equal(parseBenchmarkArgs([]).scenarios.includes('fsync'), false);
  assert.equal(parseBenchmarkArgs([]).scenarios.includes('truncate'), false);
  assert.equal(parseBenchmarkArgs([]).scenarios.includes('rename'), false);
  assert.equal(parseBenchmarkArgs([]).scenarios.includes('exec'), false);
});

test('compares matching benchmark scenarios', () => {
  const comparison = compareSummaries([
    { name: 'read', chunkSize: 262144, operationMsMedian: 20, pipelineRequestsMedian: 4, sqlStatementsMedian: 4 },
  ], [
    { name: 'read', chunkSize: 262144, operationMsMedian: 10, pipelineRequestsMedian: 2, sqlStatementsMedian: 4 },
  ]);
  assert.deepEqual(comparison, [{
    name: 'read', chunkSize: 262144,
    baselineMs: 20, candidateMs: 10, speedup: 2,
    operationMsChangePercent: -50,
    pipelineRequestsChangePercent: -50,
    sqlStatementsChangePercent: 0,
  }]);
});

test('computes counter deltas only within one session epoch', () => {
  assert.equal(counterDelta(10, 14, 'one', 'one', 1, 1), 4);
  assert.equal(counterDelta(10, 14, 'one', 'two', 1, 1), null);
  assert.equal(counterDelta(10, 2, 'one', 'one', 1, 2), null);
  assert.equal(counterDelta(10, 14, 'one', 'one', 1, 2), null);
  assert.equal(counterDelta(10, 14, 'one', 'one', undefined, undefined), null);
  assert.equal(counterDelta(10, 14, 'one', 'one', -1, -1), null);
  assert.equal(counterDelta(undefined, 14, 'one', 'one', 1, 1), null);
});

test('preserves durable command identity and status in failure diagnostics', async () => {
  const client = { getJob: async (id) => ({ id, status: 'unknown' }) };
  const diagnostics = JSON.parse(await commandDiagnostics(client, {
    commandId: 'command-1', exitCode: 1, outputTruncated: true,
    stdout: 'partial output', stderr: 'runtime lost',
  }));
  assert.deepEqual(diagnostics, {
    commandId: 'command-1', status: 'unknown', exitCode: 1, outputTruncated: true,
    stdout: 'partial output', stderr: 'runtime lost',
  });
});

test('rejects benchmark comparisons with missing or incompatible samples', () => {
  const baseline = report([{ name: 'read', chunkSize: 262144, samples: 3 }]);
  validateComparableReports(baseline, structuredClone(baseline));
  assert.throws(() => validateComparableReports(baseline, report([])), /scenario, chunk-size, and sample sets/);
  assert.throws(() => validateComparableReports(baseline, { ...baseline, runs: 2 }), /runs must match/);
  assert.throws(() => validateComparableReports(baseline, { ...baseline, volumes: [] }), /volume chunk-size sets/);
});

test('scores balanced workload groups from p50, p95, and p99 latency', () => {
  const baseline = scoreFixture(100, 100);
  const candidate = scoreFixture(50, 200);
  baseline.summary.push(summaryEntry('fuse_rename_open_close', 1000));
  candidate.summary.push(summaryEntry('fuse_rename_open_close', 1));
  const score = scoreReport(baseline, candidate);
  assert.equal(score.groups.direct, 200);
  assert.equal(score.groups.git, 50);
  assert.equal(score.groups.startup, 100);
  assert.equal(score.groups.rename, undefined);
  assert.equal(score.overall, 100);
  const invalid = structuredClone(candidate);
  invalid.summary[0].clientMsP99 = null;
  assert.throws(() => scoreReport(baseline, invalid), /Invalid latency percentiles/);
});

test('summarizes medians without mixing chunk sizes', () => {
  const summary = summarizeSamples([
    sample(262144, 30, 10),
    sample(262144, 10, 30),
    sample(262144, 20, 20),
    sample(65536, 5, 40),
  ]);
  assert.deepEqual(summary.map((entry) => ({
    chunkSize: entry.chunkSize,
    operationMsMedian: entry.operationMsMedian,
    throughputMiBpsMedian: entry.throughputMiBpsMedian,
  })), [
    { chunkSize: 65536, operationMsMedian: 5, throughputMiBpsMedian: 40 },
    { chunkSize: 262144, operationMsMedian: 20, throughputMiBpsMedian: 20 },
  ]);
});

test('parses the final command JSON line and safely quotes Python', () => {
  assert.deepEqual(parseCommandResult('diagnostic\n{"operationMs":12.5,"operations":2}\n'), {
    operationMs: 12.5,
    operations: 2,
  });
  assert.match(pythonCommand("print('ok')"), /^python3 -c '/);
  assert.match(pythonCommand("print('ok')"), /'"'"'/);
  assert.throws(() => parseCommandResult('{"operationMs":-1}'), /invalid operationMs/);
});

function sample(chunkSize, operationMs, throughputMiBps) {
  return {
    name: 'read', chunkSize, operationMs, clientMs: operationMs + 2,
    throughputMiBps, operationsPerSecond: null, pipelineRequests: 4, sqlStatements: 4,
  };
}

function report(summary) {
  return {
    schemaVersion: 1,
    profile: 'quick',
    runs: 3,
    scenarios: ['fuse-sequential'],
    configuration: { sequentialSizes: [1024] },
    volumes: [{ chunkSize: 262144 }],
    summary,
  };
}

function scoreFixture(directMs, gitMs) {
  const summary = [
    summaryEntry('direct_read_1m', directMs),
    summaryEntry('git_add_commit', gitMs),
  ];
  return {
    ...report(summary),
    scenarios: ['direct-sequential', 'git'],
    volumes: [{ chunkSize: 262144, startupMs: { p50: 100, p95: 100, p99: 100 } }],
  };
}

function summaryEntry(name, latency) {
  return {
    name,
    chunkSize: 262144,
    samples: 3,
    clientMsP50: latency,
    clientMsP95: latency,
    clientMsP99: latency,
  };
}
