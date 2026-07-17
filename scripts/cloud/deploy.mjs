// ABOUTME: Generates isolated Wrangler configs and safely deploys named AiryFS environments.
// ABOUTME: Pins account credentials, guards production, and serializes same-env mutations.

import { execFileSync } from 'node:child_process';
import {
  mkdirSync,
  mkdtempSync,
  readFileSync,
  rmSync,
  symlinkSync,
  writeFileSync,
} from 'node:fs';
import { dirname, join, resolve } from 'node:path';
import { homedir, tmpdir } from 'node:os';
import { fileURLToPath, pathToFileURL } from 'node:url';

const SCRIPT_DIR = dirname(fileURLToPath(import.meta.url));
export const REPO_ROOT = resolve(SCRIPT_DIR, '../..');
const WORKER_DIR = join(REPO_ROOT, 'worker');
const BASE_CONFIG = join(WORKER_DIR, 'wrangler.jsonc');

export function validateEnvName(env) {
  if (typeof env !== 'string' || !/^[a-z](?:[a-z0-9-]*[a-z0-9])?$/.test(env)
    || env.length > 20 || env.includes('--')) {
    throw new Error('environment must be a lowercase [a-z0-9-] token, start with a letter, and be at most 20 characters');
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

export function generateConfig(base, env, accountId) {
  validateEnvName(env);
  return {
    ...structuredClone(base),
    account_id: accountId,
    name: `airyfs-${env}`,
    vars: { ...(base.vars ?? {}), AIRYFS_ENVIRONMENT: env },
    workers_dev: true,
    preview_urls: false,
  };
}

export function sanitizeDockerConfig(config) {
  const sanitized = { ...(config ?? {}) };
  delete sanitized.credsStore;
  delete sanitized.credHelpers;
  delete sanitized.currentContext;
  return sanitized;
}

export function generatedConfigPath(env, workerDir = WORKER_DIR) {
  return join(workerDir, `wrangler.generated.${validateEnvName(env)}.jsonc`);
}

function readDevVars(root = REPO_ROOT) {
  try {
    return readFileSync(join(root, '.dev.vars'), 'utf8');
  } catch (error) {
    if (error?.code === 'ENOENT') return '';
    throw error;
  }
}

export function render(env, { accountId, root = REPO_ROOT, workerDir = WORKER_DIR } = {}) {
  const resolvedAccount = resolveAccountId({
    explicit: accountId,
    devVars: readDevVars(root),
  });
  const base = JSON.parse(readFileSync(join(workerDir, 'wrangler.jsonc'), 'utf8'));
  const output = generatedConfigPath(env, workerDir);
  writeFileSync(output, `${JSON.stringify(generateConfig(base, env, resolvedAccount), null, 2)}\n`);
  return { accountId: resolvedAccount, output };
}

function gitIsClean(root = REPO_ROOT) {
  return execFileSync('git', ['status', '--porcelain'], { cwd: root, encoding: 'utf8' }).trim() === '';
}

export function assertDeployAllowed(env, opts, clean) {
  if (env === 'prod' && !opts.allowProd && !opts.dryRun) {
    throw new Error('refusing to deploy prod without --allow-prod');
  }
  if (env === 'prod' && opts.allowDirty) {
    throw new Error('prod never accepts --allow-dirty');
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
    symlinkSync(join(homedir(), '.docker', 'cli-plugins'), join(directory, 'cli-plugins'), 'dir');
    return operation(directory, dockerHost);
  } finally {
    rmSync(directory, { recursive: true, force: true });
  }
}

export function deploy(env, opts = {}) {
  validateEnvName(env);
  assertDeployAllowed(env, opts, gitIsClean(opts.root));

  const rendered = render(env, opts);
  const run = () => withIsolatedDockerConfig((dockerConfig, dockerHost) => {
    const args = ['wrangler', 'deploy', '-c', rendered.output, '--containers-rollout', 'immediate'];
    if (opts.dryRun) args.push('--dry-run');
    execFileSync('npx', args, {
      cwd: opts.workerDir ?? WORKER_DIR,
      env: {
        ...process.env,
        CLOUDFLARE_ACCOUNT_ID: rendered.accountId,
        DOCKER_CONFIG: dockerConfig,
        DOCKER_HOST: dockerHost,
      },
      stdio: 'inherit',
    });
    return rendered;
  });
  return opts.dryRun ? run() : withEnvLock(env, run);
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
  if (!command || !env || !['render', 'check', 'deploy'].includes(command)) {
    throw new Error('usage: deploy.mjs <render|check|deploy> <env> [--dry-run] [--allow-dirty] [--allow-prod] [--account-id ID]');
  }
  if (command === 'render') {
    const result = render(env, opts);
    console.log(`rendered ${result.output}`);
    return;
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
