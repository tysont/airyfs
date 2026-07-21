// ABOUTME: Defines deterministic Container regression profiles and parses runner arguments.
// ABOUTME: Keeps deployed test selection locally unit-testable without contacting a Worker.

export const REGRESSION_PROFILES = Object.freeze({
  quick: Object.freeze([
    'environment',
    'filesystem-lifecycle',
    'api-fuse-coherence',
    'cross-runtime-data-flow',
  ]),
  broad: Object.freeze([
    'environment',
    'filesystem-lifecycle',
    'api-fuse-coherence',
    'cross-runtime-data-flow',
    'sequential-and-random-io',
    'metadata-concurrency',
    'native-build',
    'archive-round-trip',
    'error-semantics',
    'streaming-exec',
    'restart-persistence',
  ]),
});

export function parseRegressionArgs(argv) {
  const options = { profile: 'quick', prefix: 'container-regression', help: false };

  for (let index = 0; index < argv.length; index++) {
    const argument = argv[index];
    if (argument === '--help' || argument === '-h') options.help = true;
    else if (argument === '--profile') options.profile = requireValue(argv, ++index, argument);
    else if (argument === '--prefix') options.prefix = requireValue(argv, ++index, argument);
    else throw new Error(`Unknown argument: ${argument}`);
  }

  if (!(options.profile in REGRESSION_PROFILES)) throw new Error(`Unknown profile: ${options.profile}`);
  if (!/^[a-zA-Z0-9][a-zA-Z0-9-]{0,39}$/.test(options.prefix)) {
    throw new Error('--prefix must be 1-40 alphanumeric or hyphen characters');
  }
  return options;
}

export function regressionHelp() {
  return `Usage: npm run test:regression:quick -- [options]

Options:
  --profile quick|broad  Regression depth (default: quick)
  --prefix NAME         Volume prefix (default: container-regression)
  --help, -h            Show this help`;
}

function requireValue(argv, index, argument) {
  const value = argv[index];
  if (value === undefined || value.startsWith('--')) throw new Error(`${argument} requires a value`);
  return value;
}
