// ABOUTME: Safely deploys the fixed Wrangler integration and production environments.
// ABOUTME: Pins account credentials, guards production, and serializes same-env mutations.

import { execFileSync } from 'node:child_process';
import {
  mkdirSync,
  mkdtempSync,
  readFileSync,
  rmSync,
  symlinkSync,
  writeFileSync,
  existsSync,
} from 'node:fs';
import { dirname, join, resolve } from 'node:path';
import { homedir, tmpdir } from 'node:os';
import { fileURLToPath, pathToFileURL } from 'node:url';

const SCRIPT_DIR = dirname(fileURLToPath(import.meta.url));
export const REPO_ROOT = resolve(SCRIPT_DIR, '..');
const WORKER_DIR = join(REPO_ROOT, 'worker');
const DEPLOY_ENVIRONMENTS = new Set(['int', 'prod']);

export function validateEnvName(env) {
  if (!DEPLOY_ENVIRONMENTS.has(env)) {
    throw new Error('environment must be int or prod');
  }
  return env;
}

export function parseDotEnv(text) {
  const values = {};
  for (const rawLine of text.split(/\r?\n/)) {
    const line = rawLine.trim();
    if (!line || line.startsWith('#')) continue;
    const match = /^(?:export\s+)?([A-Za-z_][A-Za-z0-9_]*)=(.*)$/.exec(line);
    if (!match) continue;
    let value = match[2].trim();
    if ((value.startsWith('"') && value.endsWith('"')) || (value.startsWith("'") && value.endsWith("'"))) {
      value = value.slice(1, -1);
    }
    values[match[1]] = value;
  }
  return values;
}

export function resolveAccountId({ explicit, processEnv = process.env, devVars = '' } = {}) {
  const ambient = processEnv.CLOUDFLARE_ACCOUNT_ID || parseDotEnv(devVars).CLOUDFLARE_ACCOUNT_ID;
  if (explicit && ambient && explicit !== ambient) {
    throw new Error('account id disagreement between --account-id and CLOUDFLARE_ACCOUNT_ID');
  }
  const accountId = explicit || ambient;
  if (!accountId) {
    throw new Error('CLOUDFLARE_ACCOUNT_ID is required in the environment, --account-id, or .dev.vars');
  }
  return accountId;
}

export function sanitizeDockerConfig(config) {
  const sanitized = { ...(config ?? {}) };
  delete sanitized.credsStore;
  delete sanitized.credHelpers;
  delete sanitized.currentContext;
  return sanitized;
}

function readDevVars(root = REPO_ROOT) {
  try {
    return readFileSync(join(root, '.dev.vars'), 'utf8');
  } catch (error) {
    if (error?.code === 'ENOENT') return '';
    throw error;
  }
}

function gitIsClean(root = REPO_ROOT) {
  return execFileSync('git', ['status', '--porcelain'], { cwd: root, encoding: 'utf8' }).trim() === '';
}

export function assertDeployAllowed(env, opts, clean) {
  if (env === 'prod' && !opts.allowProd && !opts.dryRun) {
    throw new Error('refusing to deploy prod without --allow-prod');
  }
  if (env === 'prod' && opts.allowDirty && !opts.dryRun) {
    throw new Error('prod deployments never accept --allow-dirty');
  }
  if (!opts.allowDirty && !clean) {
    throw new Error('refusing to deploy from a dirty tree; use --allow-dirty for non-prod');
  }
}

function withEnvLock(env, operation) {
  const lock = join(REPO_ROOT, '.airyfs', 'cloud', env, '.lock');
  mkdirSync(dirname(lock), { recursive: true });
  try {
    mkdirSync(lock);
  } catch (error) {
    if (error?.code === 'EEXIST') throw new Error(`another ${env} deployment holds ${lock}`);
    throw error;
  }
  try {
    return operation();
  } finally {
    rmSync(lock, { recursive: true });
  }
}

function withIsolatedDockerConfig(operation) {
  const directory = mkdtempSync(join(tmpdir(), 'airyfs-docker-'));
  try {
    const dockerHost = execFileSync(
      'docker',
      ['context', 'inspect', '--format', '{{.Endpoints.docker.Host}}'],
      { encoding: 'utf8' }
    ).trim();
    let source = {};
    try {
      source = JSON.parse(readFileSync(join(homedir(), '.docker', 'config.json'), 'utf8'));
    } catch {
      source = {};
    }
    writeFileSync(
      join(directory, 'config.json'),
      `${JSON.stringify(sanitizeDockerConfig(source), null, 2)}\n`,
      { mode: 0o600 }
    );
    const plugins = join(homedir(), '.docker', 'cli-plugins');
    if (existsSync(plugins)) symlinkSync(plugins, join(directory, 'cli-plugins'), 'dir');
    return operation(directory, dockerHost);
  } finally {
    rmSync(directory, { recursive: true, force: true });
  }
}

export function deploy(env, opts = {}) {
  validateEnvName(env);
  assertDeployAllowed(env, opts, gitIsClean(opts.root));
  const accountId = resolveAccountId({
    explicit: opts.accountId,
    devVars: readDevVars(opts.root),
  });
  const run = () => withIsolatedDockerConfig((dockerConfig, dockerHost) => {
    const args = wranglerArgs(env, opts);
    execFileSync('npx', args, {
      cwd: opts.workerDir ?? WORKER_DIR,
      env: {
        ...process.env,
        CLOUDFLARE_ACCOUNT_ID: accountId,
        DOCKER_CONFIG: dockerConfig,
        DOCKER_HOST: dockerHost,
      },
      stdio: 'inherit',
    });
    return { accountId, env };
  });
  return opts.dryRun ? run() : withEnvLock(env, run);
}

export function wranglerArgs(env, opts = {}) {
  validateEnvName(env);
  const args = ['wrangler', 'deploy', '--env', env, '--containers-rollout', 'immediate'];
  if (opts.dryRun) args.push('--dry-run');
  return args;
}

function parseArgs(argv) {
  const [command, env, ...flags] = argv;
  const valueAfter = (flag) => {
    const index = flags.indexOf(flag);
    return index >= 0 ? flags[index + 1] : undefined;
  };
  return {
    command,
    env,
    opts: {
      allowDirty: flags.includes('--allow-dirty'),
      allowProd: flags.includes('--allow-prod'),
      dryRun: flags.includes('--dry-run'),
      accountId: valueAfter('--account-id'),
    },
  };
}

export function main(argv = process.argv.slice(2)) {
  const { command, env, opts } = parseArgs(argv);
  if (!command || !env || !['check', 'deploy'].includes(command)) {
    throw new Error('usage: deploy.mjs <check|deploy> <int|prod> [--dry-run] [--allow-dirty] [--allow-prod] [--account-id ID]');
  }
  deploy(env, { ...opts, dryRun: command === 'check' ? true : opts.dryRun });
}

if (process.argv[1] && import.meta.url === pathToFileURL(resolve(process.argv[1])).href) {
  try {
    main();
  } catch (error) {
    console.error(error instanceof Error ? error.message : String(error));
    process.exitCode = 1;
  }
}
