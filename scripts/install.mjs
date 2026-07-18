// ABOUTME: One-command local installer for the AiryFS CLI (airyfs + airy alias) and SDK.
// ABOUTME: Verifies the Node runtime, builds the SDK then the CLI, and links them globally.

import { execFileSync } from 'node:child_process';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';

const SCRIPT_DIR = dirname(fileURLToPath(import.meta.url));
export const REPO_ROOT = resolve(SCRIPT_DIR, '..');

/** Minimum Node major version the CLI and SDK support. */
export const MIN_NODE_MAJOR = 22;

/** Parse a `process.version`-style string into its major integer. */
export function parseNodeMajor(version) {
  const match = /^v?(\d+)\./.exec(String(version).trim());
  if (!match) throw new Error(`Unrecognized Node version: ${version}`);
  return Number(match[1]);
}

/** Throw when the running Node is older than the supported minimum. */
export function assertNodeSupported(version = process.version, min = MIN_NODE_MAJOR) {
  const major = parseNodeMajor(version);
  if (major < min) {
    throw new Error(`Node ${min}+ is required to install AiryFS; found ${version}`);
  }
  return major;
}

/**
 * Ordered build/link steps. Each step is a package directory plus the npm
 * commands to run there. The SDK builds before the CLI because the CLI depends
 * on the SDK's built output, and linking happens last so a failed build never
 * publishes a broken global binary.
 */
export function installSteps({ link = true } = {}) {
  const steps = [
    { dir: 'sdk', commands: [['ci'], ['run', 'build']] },
    { dir: 'cli', commands: [['ci'], ['run', 'build']] },
  ];
  if (link) steps.push({ dir: 'cli', commands: [['link']] });
  return steps;
}

function runNpm(dir, args, root) {
  execFileSync('npm', args, { cwd: join(root, dir), stdio: 'inherit' });
}

export function install(opts = {}) {
  const root = opts.root ?? REPO_ROOT;
  assertNodeSupported();
  for (const step of installSteps({ link: opts.link !== false })) {
    for (const command of step.commands) {
      if (opts.dryRun) {
        process.stdout.write(`[dry-run] (${step.dir}) npm ${command.join(' ')}\n`);
      } else {
        runNpm(step.dir, command, root);
      }
    }
  }
  if (!opts.dryRun) {
    process.stdout.write(
      '\nAiryFS CLI installed. Try:\n' +
        '  airy --help\n' +
        '  airy init            # deploy a Worker and create your first session\n',
    );
  }
  return { root };
}

export function main(argv = process.argv.slice(2)) {
  install({ dryRun: argv.includes('--dry-run'), link: !argv.includes('--no-link') });
}

if (process.argv[1] && import.meta.url === pathToFileURL(resolve(process.argv[1])).href) {
  try {
    main();
  } catch (error) {
    console.error(error instanceof Error ? error.message : String(error));
    process.exitCode = 1;
  }
}
