// ABOUTME: Repo-local one-shot provisioner: deploys the Worker, sets the auth secret, prints the URL.
// ABOUTME: Backs `airy deploy`/`airy init`; emits a machine-readable JSON result line with --json.

import { execFileSync } from 'node:child_process';
import { randomBytes } from 'node:crypto';
import { join } from 'node:path';
import { pathToFileURL } from 'node:url';
import { REPO_ROOT, deploy, resolveAccountId, validateEnvName } from './deploy.mjs';

const WORKER_DIR = join(REPO_ROOT, 'worker');

/** Generate a high-entropy deployment secret (base64url, 32 bytes). */
export function generateSecret(bytes = 32) {
  return randomBytes(bytes).toString('base64url');
}

/** Build the wrangler arguments that store the deployment auth secret. */
export function secretPutArgs(env, name = 'AIRYFS_AUTH_SECRET') {
  return ['wrangler', 'secret', 'put', name, '--env', validateEnvName(env)];
}

function putSecret(env, secret, accountId, opts = {}) {
  if (opts.dryRun) return;
  execFileSync('npx', secretPutArgs(env), {
    cwd: opts.workerDir ?? WORKER_DIR,
    input: `${secret}\n`,
    env: { ...process.env, CLOUDFLARE_ACCOUNT_ID: accountId },
    stdio: ['pipe', 'inherit', 'inherit'],
  });
}

/**
 * Deploy the Worker for `env`, ensure a deployment secret is set, and return the
 * result. When `opts.secret` is omitted a fresh one is generated and returned so
 * the caller can store it as the session's root credential.
 */
export function provision(env, opts = {}) {
  validateEnvName(env);
  const secret = opts.secret ?? generateSecret();
  const result = deploy(env, { ...opts, capture: true });
  putSecret(env, secret, result.accountId, opts);
  return { env, url: result.url ?? null, secret, accountId: result.accountId };
}

function parseArgs(argv) {
  const [env, ...flags] = argv.filter((arg) => arg !== '--json');
  return {
    env,
    json: argv.includes('--json'),
    opts: {
      allowDirty: flags.includes('--allow-dirty'),
      allowProd: flags.includes('--allow-prod'),
      dryRun: flags.includes('--dry-run'),
    },
  };
}

export function main(argv = process.argv.slice(2)) {
  const { env, json, opts } = parseArgs(argv);
  if (!env) throw new Error('usage: provision.mjs <int|prod> [--json] [--dry-run] [--allow-dirty] [--allow-prod]');
  const result = provision(env, opts);
  if (json) {
    // A single JSON line on stdout for the CLI to parse; secret included for local session setup.
    process.stdout.write(`\n${JSON.stringify({ airyfsProvision: result })}\n`);
  } else {
    process.stdout.write(`\nDeployed ${env}${result.url ? ` at ${result.url}` : ''}.\n`);
  }
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  try {
    main();
  } catch (error) {
    console.error(error instanceof Error ? error.message : String(error));
    process.exitCode = 1;
  }
}
