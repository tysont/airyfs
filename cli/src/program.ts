// ABOUTME: Constructs and runs the AiryFS command-line program.
// ABOUTME: Shares command registration between one-shot invocation and the interactive shell.

import { Command, CommanderError } from 'commander';
import { registerCommands } from './commands/index.js';
import { createRuntime, type Runtime } from './context.js';
import { ConfigError } from './config/store.js';
import { Output } from './ui/output.js';

export function createProgram(runtime = createRuntime()): Command {
  const program = new Command()
    .name('airyfs')
    .description('A local command-line client for AiryFS volumes')
    .version('0.1.0')
    .option('-s, --session <name>', 'use a named session for this invocation')
    .option('--json', 'emit machine-readable JSON')
    .option('--no-color', 'disable color output')
    .option('-q, --quiet', 'suppress non-error output')
    .enablePositionalOptions()
    .configureHelp({ sortSubcommands: true, sortOptions: true })
    .showHelpAfterError(!runtime.jsonMode);

  program.configureOutput({
    writeOut: (text) => (runtime.stdout || process.stdout).write(text),
    writeErr: (text) => (runtime.stderr || process.stderr).write(text),
    outputError: (text, write) => {
      if (!runtime.jsonMode) write(text);
    },
  });
  program.exitOverride();
  registerCommands(program, runtime);
  return program;
}

export async function execute(argv: string[], overrides: Partial<Runtime> = {}): Promise<number> {
  const runtime = createRuntime({ ...overrides, jsonMode: hasGlobalJson(argv.slice(2)) });
  try {
    const program = createProgram(runtime);
    if (argv.length <= 2) program.help();
    await program.parseAsync(argv);
  } catch (error) {
    if (!(error instanceof CommanderError)) throw error;
    if (!['commander.helpDisplayed', 'commander.version'].includes(error.code)) {
      runtime.exitCode = error.exitCode || 2;
      if (runtime.jsonMode) {
        new Output({ json: true, stdout: runtime.stdout, stderr: runtime.stderr })
          .error(new ConfigError(error.message.replace(/^error:\s*/i, '')));
      }
    }
  }
  return runtime.exitCode;
}

function hasGlobalJson(args: string[]): boolean {
  for (let index = 0; index < args.length; index++) {
    const argument = args[index];
    if (argument === '--json') return true;
    if (['-s', '--session'].includes(argument)) {
      index++;
      continue;
    }
    if (argument.startsWith('-')) continue;
    return false;
  }
  return false;
}

export async function run(argv: string[]): Promise<void> {
  process.exitCode = await execute(argv);
}
