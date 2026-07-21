// ABOUTME: Shared argument parsing, timing summaries, and command helpers for deployed benchmarks.
// ABOUTME: Keeps the benchmark runner deterministic and its local behavior unit-testable.

export const MIB = 1024 * 1024;

export const PROFILES = Object.freeze({
  quick: Object.freeze({
    startupRuns: 3,
    sequentialSizes: [MIB],
    randomFileBytes: 4 * MIB,
    randomOperations: 16,
    metadataFiles: 20,
    smallFiles: 10,
  }),
  full: Object.freeze({
    startupRuns: 5,
    sequentialSizes: [MIB, 95 * MIB],
    randomFileBytes: 64 * MIB,
    randomOperations: 2_048,
    metadataFiles: 1_000,
    smallFiles: 1_000,
  }),
});

const DEFAULT_SCENARIOS = Object.freeze([
  'direct-sequential',
  'fuse-sequential',
  'fuse-random',
  'metadata',
  'small-files',
  'git',
]);
const ALL_SCENARIOS = Object.freeze([...DEFAULT_SCENARIOS, 'negative-lookups', 'fsync', 'truncate', 'rename', 'exec']);

export function parseBenchmarkArgs(argv) {
  const options = {
    profile: 'quick',
    chunkSizes: [256 * 1024],
    runs: 3,
    scenarios: [...DEFAULT_SCENARIOS],
    prefix: 'airyfs-bench',
    label: null,
    json: false,
    output: null,
    baseline: null,
    help: false,
  };

  for (let index = 0; index < argv.length; index++) {
    const argument = argv[index];
    if (argument === '--help' || argument === '-h') options.help = true;
    else if (argument === '--json') options.json = true;
    else if (argument === '--profile') options.profile = requireValue(argv, ++index, argument);
    else if (argument === '--chunk-sizes') {
      options.chunkSizes = requireValue(argv, ++index, argument).split(',').map(parsePositiveInteger);
    } else if (argument === '--runs') options.runs = parsePositiveInteger(requireValue(argv, ++index, argument));
    else if (argument === '--scenarios') {
      options.scenarios = requireValue(argv, ++index, argument).split(',').filter(Boolean);
    } else if (argument === '--prefix') options.prefix = requireValue(argv, ++index, argument);
    else if (argument === '--label') options.label = requireValue(argv, ++index, argument);
    else if (argument === '--output') options.output = requireValue(argv, ++index, argument);
    else if (argument === '--baseline') options.baseline = requireValue(argv, ++index, argument);
    else throw new Error(`Unknown argument: ${argument}`);
  }

  if (!(options.profile in PROFILES)) throw new Error(`Unknown profile: ${options.profile}`);
  if (options.chunkSizes.length === 0) throw new Error('At least one chunk size is required');
  for (const chunkSize of options.chunkSizes) {
    if (chunkSize < 4 * 1024 || chunkSize > MIB || (chunkSize & (chunkSize - 1)) !== 0) {
      throw new Error(`Invalid chunk size: ${chunkSize}`);
    }
  }
  const unknownScenarios = options.scenarios.filter((name) => !ALL_SCENARIOS.includes(name));
  if (options.scenarios.length === 0) throw new Error('At least one scenario is required');
  if (unknownScenarios.length > 0) throw new Error(`Unknown scenarios: ${unknownScenarios.join(', ')}`);
  if (!/^[a-zA-Z0-9][a-zA-Z0-9-]{0,39}$/.test(options.prefix)) {
    throw new Error('--prefix must be 1-40 alphanumeric or hyphen characters');
  }
  return options;
}

export function summarizeSamples(samples) {
  const groups = new Map();
  for (const sample of samples) {
    const key = `${sample.chunkSize}:${sample.name}`;
    const values = groups.get(key) ?? [];
    values.push(sample);
    groups.set(key, values);
  }
  return [...groups.values()].map((values) => {
    const first = values[0];
    const operationMs = values.map((value) => value.operationMs).filter(Number.isFinite).sort(numeric);
    const clientMs = values.map((value) => value.clientMs).filter(Number.isFinite).sort(numeric);
    const throughput = values.map((value) => value.throughputMiBps).filter(Number.isFinite).sort(numeric);
    const operationsPerSecond = values.map((value) => value.operationsPerSecond).filter(Number.isFinite).sort(numeric);
    const pipelines = values.map((value) => value.pipelineRequests).filter(Number.isFinite).sort(numeric);
    const statements = values.map((value) => value.sqlStatements).filter(Number.isFinite).sort(numeric);
    return {
      name: first.name,
      chunkSize: first.chunkSize,
      samples: values.length,
      operationMsMedian: median(operationMs),
      operationMsP50: percentile(operationMs, 0.50),
      operationMsP95: percentile(operationMs, 0.95),
      operationMsP99: percentile(operationMs, 0.99),
      operationMsMin: operationMs.at(0) ?? null,
      clientMsMedian: median(clientMs),
      clientMsP50: percentile(clientMs, 0.50),
      clientMsP95: percentile(clientMs, 0.95),
      clientMsP99: percentile(clientMs, 0.99),
      throughputMiBpsMedian: median(throughput),
      operationsPerSecondMedian: median(operationsPerSecond),
      pipelineRequestsMedian: median(pipelines),
      sqlStatementsMedian: median(statements),
      commitsPerSecondP50: percentile(
        values.map((value) => value.commitsPerSecond).filter(Number.isFinite).sort(numeric),
        0.50,
      ),
    };
  }).sort((left, right) => left.chunkSize - right.chunkSize || left.name.localeCompare(right.name));
}

export function latencyPercentiles(values) {
  const sorted = values.filter(Number.isFinite).sort(numeric);
  return {
    p50: percentile(sorted, 0.50),
    p95: percentile(sorted, 0.95),
    p99: percentile(sorted, 0.99),
  };
}

export function scoreReport(baseline, candidate) {
  validateComparableReports(baseline, candidate);
  const candidateByKey = new Map(candidate.summary.map((entry) => [`${entry.chunkSize}:${entry.name}`, entry]));
  const ratios = new Map();
  for (const before of baseline.summary) {
    if (isControlSample(before.name)) continue;
    const after = candidateByKey.get(`${before.chunkSize}:${before.name}`);
    const ratio = weightedLatencyRatio(before, after);
    if (ratio === null) throw new Error(`Invalid latency percentiles for ${before.chunkSize}:${before.name}`);
    addRatio(ratios, scoreGroup(before.name), ratio);
  }
  const startupByChunk = new Map(candidate.volumes.map((volume) => [volume.chunkSize, volume]));
  for (const before of baseline.volumes) {
    const after = startupByChunk.get(before.chunkSize);
    const ratio = weightedPercentileRatio(before.startupMs, after?.startupMs);
    if (ratio === null) throw new Error(`Invalid startup percentiles for chunk size ${before.chunkSize}`);
    addRatio(ratios, 'startup', ratio);
  }
  const groups = Object.fromEntries([...ratios.entries()].map(([name, values]) => [name, roundScore(100 * geometricMean(values))]));
  const expectedGroups = new Set([
    ...baseline.summary.filter((entry) => !isControlSample(entry.name)).map((entry) => scoreGroup(entry.name)),
    'startup',
  ]);
  if ([...expectedGroups].some((group) => !Object.hasOwn(groups, group))) {
    throw new Error('Benchmark score is missing a workload group');
  }
  return {
    overall: roundScore(100 * geometricMean(Object.values(groups).map((value) => value / 100))),
    groups,
    method: 'Equal-weight geometric mean of workload groups; each scenario weights p50/p95/p99 client latency 50/30/20',
  };
}

export function compareSummaries(baseline, candidate) {
  const candidateByKey = new Map(candidate.map((entry) => [`${entry.chunkSize}:${entry.name}`, entry]));
  return baseline.flatMap((before) => {
    const after = candidateByKey.get(`${before.chunkSize}:${before.name}`);
    if (!after) return [];
    return [{
      name: before.name,
      chunkSize: before.chunkSize,
      baselineMs: before.operationMsMedian,
      candidateMs: after.operationMsMedian,
      speedup: ratio(before.operationMsMedian, after.operationMsMedian),
      operationMsChangePercent: percentChange(before.operationMsMedian, after.operationMsMedian),
      pipelineRequestsChangePercent: percentChange(before.pipelineRequestsMedian, after.pipelineRequestsMedian),
      sqlStatementsChangePercent: percentChange(before.sqlStatementsMedian, after.sqlStatementsMedian),
    }];
  });
}

export function validateComparableReports(baseline, candidate) {
  for (const field of ['schemaVersion', 'profile', 'runs']) {
    if (baseline[field] !== candidate[field]) throw new Error(`Benchmark ${field} must match`);
  }
  for (const field of ['configuration', 'scenarios']) {
    if (JSON.stringify(baseline[field]) !== JSON.stringify(candidate[field])) {
      throw new Error(`Benchmark ${field} must match`);
    }
  }
  const signature = (report) => report.summary
    .map((entry) => `${entry.chunkSize}:${entry.name}:${entry.samples}`)
    .sort()
    .join('|');
  if (signature(baseline) !== signature(candidate)) {
    throw new Error('Benchmark scenario, chunk-size, and sample sets must match');
  }
  const volumeSignature = (report) => report.volumes.map((volume) => volume.chunkSize).sort(numeric).join(',');
  if (volumeSignature(baseline) !== volumeSignature(candidate)) {
    throw new Error('Benchmark volume chunk-size sets must match');
  }
}

export function counterDelta(before, after, beforeSession, afterSession, beforeEpoch, afterEpoch) {
  return typeof beforeSession === 'string' && beforeSession === afterSession
    && Number.isSafeInteger(beforeEpoch) && beforeEpoch >= 0 && beforeEpoch === afterEpoch
    && Number.isFinite(before) && Number.isFinite(after) && after >= before
    ? after - before
    : null;
}

export function parseCommandResult(stdout) {
  const line = stdout.trim().split('\n').filter(Boolean).at(-1);
  if (!line) throw new Error('Benchmark command returned no JSON result');
  const value = JSON.parse(line);
  if (!Number.isFinite(value.operationMs) || value.operationMs < 0) {
    throw new Error('Benchmark command returned an invalid operationMs');
  }
  return value;
}

export function pythonCommand(source) {
  return `python3 -c ${shellQuote(source)}`;
}

export function shellQuote(value) {
  return `'${String(value).replaceAll("'", `'"'"'`)}'`;
}

export function benchmarkHelp() {
  return `Usage: npm run benchmark:deployed -- [options]

Options:
  --profile quick|full        Workload size (default: quick)
  --chunk-sizes N[,N...]     Volume chunk sizes (default: 262144)
  --runs N                   Samples per scenario (default: 3)
  --scenarios NAME[,NAME...] Select scenario groups
  --prefix NAME              Stable volume prefix (default: airyfs-bench)
  --label TEXT               Deployment or experiment label for the report
  --output PATH              Also write complete JSON results to PATH
  --baseline PATH            Score this run relative to a baseline report
  --json                     Suppress the human-readable table

Scenario groups: ${ALL_SCENARIOS.join(', ')}
Environment: AIRYFS_URL is required; AIRYFS_TOKEN is optional.`;
}

function requireValue(argv, index, argument) {
  const value = argv[index];
  if (!value || value.startsWith('--')) throw new Error(`${argument} requires a value`);
  return value;
}

function parsePositiveInteger(value) {
  const parsed = Number(value);
  if (!Number.isSafeInteger(parsed) || parsed <= 0) throw new Error(`Expected a positive integer, got: ${value}`);
  return parsed;
}

function median(values) {
  if (values.length === 0) return null;
  const middle = Math.floor(values.length / 2);
  return values.length % 2 === 0 ? (values[middle - 1] + values[middle]) / 2 : values[middle];
}

function percentile(values, fraction) {
  if (values.length === 0) return null;
  const position = (values.length - 1) * fraction;
  const lower = Math.floor(position);
  const upper = Math.ceil(position);
  if (lower === upper) return values[lower];
  return values[lower] + (values[upper] - values[lower]) * (position - lower);
}

function numeric(left, right) {
  return left - right;
}

function ratio(baseline, candidate) {
  return baseline === null || candidate === null || candidate === 0 ? null : baseline / candidate;
}

function percentChange(baseline, candidate) {
  return baseline === null || candidate === null || baseline === 0 ? null : ((candidate / baseline) - 1) * 100;
}

function weightedLatencyRatio(before, after) {
  if (!after) return null;
  return weightedPercentileRatio(
    { p50: before.clientMsP50, p95: before.clientMsP95, p99: before.clientMsP99 },
    { p50: after.clientMsP50, p95: after.clientMsP95, p99: after.clientMsP99 },
  );
}

function weightedPercentileRatio(before, after) {
  if (!before || !after) return null;
  const fields = [['p50', 0.5], ['p95', 0.3], ['p99', 0.2]];
  let logarithm = 0;
  for (const [field, weight] of fields) {
    if (!Number.isFinite(before[field]) || !Number.isFinite(after[field]) || before[field] <= 0 || after[field] <= 0) return null;
    logarithm += weight * Math.log(before[field] / after[field]);
  }
  return Math.exp(logarithm);
}

function scoreGroup(name) {
  if (name.startsWith('direct_')) return 'direct';
  if (name.startsWith('fuse_sequential_')) return 'fuse-sequential';
  if (name.startsWith('fuse_random_')) return 'fuse-random';
  if (name.startsWith('fuse_metadata_')) return 'metadata';
  if (name.startsWith('fuse_small_')) return 'small-files';
  if (name.startsWith('fuse_negative_')) return 'negative-lookups';
  if (name.startsWith('fuse_fsync_')) return 'fsync';
  if (name.startsWith('fuse_truncate_')) return 'truncate';
  if (name.startsWith('fuse_rename_')) return 'rename';
  if (name.startsWith('container_exec_')) return 'exec';
  if (name.startsWith('git_')) return 'git';
  return 'other';
}

function isControlSample(name) {
  return name.endsWith('_open_close');
}

function addRatio(groups, name, ratio) {
  const values = groups.get(name) ?? [];
  values.push(ratio);
  groups.set(name, values);
}

function geometricMean(values) {
  if (values.length === 0) return 1;
  return Math.exp(values.reduce((sum, value) => sum + Math.log(value), 0) / values.length);
}

function roundScore(value) {
  return Math.round(value * 100) / 100;
}
