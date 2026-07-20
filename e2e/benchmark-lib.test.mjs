// ABOUTME: Tests deployed benchmark parsing and summaries without requiring a deployment.
// ABOUTME: Prevents benchmark tooling changes from silently changing result interpretation.

import assert from 'node:assert/strict';
import test from 'node:test';
import {
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

test('computes counter deltas without misreporting session resets', () => {
  assert.equal(counterDelta(10, 14, 'one', 'one'), 4);
  assert.equal(counterDelta(10, 14, 'one', 'two'), null);
  assert.equal(counterDelta(10, 2, 'one', 'one'), null);
  assert.equal(counterDelta(undefined, 2, 'one', 'one'), null);
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
  const score = scoreReport(baseline, candidate);
  assert.equal(score.groups.direct, 200);
  assert.equal(score.groups.git, 50);
  assert.equal(score.groups.startup, 100);
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
