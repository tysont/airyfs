// ABOUTME: Registers the complete AiryFS command surface and maps commands to API calls.
// ABOUTME: One-shot and interactive-shell execution share these same handlers.

import { createReadStream, createWriteStream } from 'node:fs';
import { access, link, rename as renameLocal, rm as removeLocal, stat as statLocal } from 'node:fs/promises';
import { basename as localBasename } from 'node:path';
import { Readable } from 'node:stream';
import { pipeline } from 'node:stream/promises';
import { createInterface } from 'node:readline/promises';
import { Command } from 'commander';
import ora from 'ora';
import { AiryFSApiError, AiryFSTransportError } from '../api/errors.js';
import { remoteBasename, remoteDirname } from '../api/paths.js';
import type { DirectoryEntry, ExecResult } from '../api/types.js';
import {
  createContext,
  resolveRuntimeSession,
  type CommandContext,
  type GlobalOptions,
  type Runtime,
} from '../context.js';
import { ConfigError } from '../config/store.js';
import { Output } from '../ui/output.js';

type Handler = (context: CommandContext) => Promise<void>;

export function registerCommands(program: Command, runtime: Runtime): void {
  registerSessionCommands(program, runtime);
  registerContextCommands(program, runtime);
  registerNavigationCommands(program, runtime);
  registerFileCommands(program, runtime);
  registerExecCommand(program, runtime);
  registerVolumeCommands(program, runtime);
  registerDiagnosticCommands(program, runtime);
  registerKvCommands(program, runtime);

  program.command('shell')
    .description('Start an interactive AiryFS shell')
    .action(async (_options, command) => performConfig(runtime, command, async (output) => {
      if (output.json) throw new ConfigError('`airyfs shell` cannot be combined with --json');
      const globals = (command as Command).optsWithGlobals() as GlobalOptions;
      const listed = await runtime.sessions.list();
      const initialSession = globals.session || process.env.AIRYFS_SESSION || listed.currentSession;
      if (initialSession) await runtime.sessions.resolve(initialSession);
      const { runShell } = await import('../shell.js');
      await runShell(runtime, initialSession);
    }));
}

function registerSessionCommands(program: Command, runtime: Runtime): void {
  const session = program.command('session').description('Manage named local sessions');

  session.command('create')
    .alias('new')
    .argument('[name]')
    .option('-e, --endpoint <url>', 'endpoint for the new session')
    .option('-v, --volume <name>', 'volume for the new session')
    .description('Create and select a named session')
    .action(async (name, options, command) => performConfig(runtime, command, async (output) => {
      const sessionName = await requiredInput(runtime, name, 'Session name: ', 'name');
      const endpoint = await requiredInput(runtime, options.endpoint, 'Endpoint: ', '--endpoint');
      const volume = await requiredInput(runtime, options.volume, 'Volume: ', '--volume');
      const created = await runtime.sessions.create(sessionName, {
        endpoint,
        volume,
      });
      runtime.onSessionEvent?.({ type: 'select', name: created.name });
      output.success(`Created and selected session ${created.name}`, { name: created.name, ...created.session });
    }));

  session.command('list')
    .alias('ls')
    .description('List local sessions')
    .action(async (_options, command) => performConfig(runtime, command, async (output) => {
      const listed = await runtime.sessions.list();
      const globals = (command as Command).optsWithGlobals() as GlobalOptions;
      const selected = globals.session
        || (runtime.sessionOverride !== undefined
          ? runtime.sessionOverride ?? undefined
          : process.env.AIRYFS_SESSION || listed.currentSession);
      if (output.json) {
        output.value(listed.sessions.map(({ name, session: value }) => ({
          name, active: name === selected, ...value,
        })));
        return;
      }
      output.table(
        ['', 'Session', 'Endpoint', 'Volume', 'Path'],
        listed.sessions.map(({ name, session: value }) => [
          name === selected ? '*' : '', name, value.endpoint || '-', value.volume || '-', value.cwd,
        ]),
      );
    }));

  session.command('use')
    .argument('<name>')
    .description('Select the active session')
    .action(async (name, _options, command) => performConfig(runtime, command, async (output) => {
      const selected = await runtime.sessions.use(name);
      runtime.onSessionEvent?.({ type: 'select', name: selected.name });
      output.success(`Using session ${selected.name}`, { name: selected.name, ...selected.session });
    }));

  session.command('show')
    .argument('[name]')
    .description('Show a session')
    .action(async (name, _options, command) => performConfig(runtime, command, async (output) => {
      const globals = (command as Command).optsWithGlobals() as GlobalOptions;
      const selected = await resolveRuntimeSession(runtime, globals, name);
      output.value({ name: selected.name, ...selected.session });
    }));

  session.command('delete')
    .alias('rm')
    .argument('<name>')
    .description('Delete a local session without deleting its volume')
    .action(async (name, _options, command) => performConfig(runtime, command, async (output) => {
      await runtime.sessions.remove(name);
      runtime.onSessionEvent?.({ type: 'delete', name });
      output.success(`Deleted session ${name}`);
    }));

  session.command('edit')
    .argument('[name]')
    .option('-e, --endpoint <url>', 'replace the session endpoint')
    .option('-v, --volume <name>', 'replace the session volume and reset cwd')
    .description('Edit endpoint or volume for a session')
    .action(async (name, options, command) => performConfig(runtime, command, async (output) => {
      if (!options.endpoint && !options.volume) {
        throw new ConfigError('Provide --endpoint, --volume, or both');
      }
      const globals = (command as Command).optsWithGlobals() as GlobalOptions;
      const selected = await resolveRuntimeSession(runtime, globals, name);
      const updated = await runtime.sessions.edit(selected.name, {
        endpoint: options.endpoint,
        volume: options.volume,
      });
      output.success(`Updated session ${updated.name}`, { name: updated.name, ...updated.session });
    }));

  session.command('rename')
    .argument('<from>')
    .argument('<to>')
    .description('Rename a local session')
    .action(async (from, to, _options, command) => performConfig(runtime, command, async (output) => {
      const renamed = await runtime.sessions.rename(from, to);
      runtime.onSessionEvent?.({ type: 'rename', from, to });
      output.success(`Renamed session ${from} to ${to}`, { name: renamed.name, ...renamed.session });
    }));
}

function registerContextCommands(program: Command, runtime: Runtime): void {
  program.command('context')
    .alias('config')
    .description('Show the active endpoint, volume, and remote path')
    .action(async (_options, command) => perform(runtime, command, async (context) => {
      context.output.value({
        session: context.named.name,
        endpoint: context.endpoint || null,
        volume: context.volume || null,
        cwd: context.cwd,
      });
    }));
}

function registerNavigationCommands(program: Command, runtime: Runtime): void {
  program.command('pwd')
    .description('Print the current remote directory')
    .action(async (_options, command) => perform(runtime, command, async (context) => {
      context.output.value(context.cwd);
    }));

  program.command('cd')
    .argument('[path]', 'remote directory', '/')
    .description('Change the current remote directory')
    .action(async (path, _options, command) => perform(runtime, command, async (context) => {
      const globals = (command as Command).optsWithGlobals() as GlobalOptions;
      const target = context.path(path);
      await context.client().listDirectory(target);
      const updated = await context.sessions.setCwd(context.named.name, target);
      context.output.value(updated.session.cwd);
    }));

  program.command('ls')
    .argument('[path]', 'remote directory', '.')
    .option('-l, --long', 'show detailed metadata')
    .option('-a, --all', 'include dotfiles')
    .description('List a remote directory')
    .action(async (path, options, command) => perform(runtime, command, async (context) => {
      const target = context.path(path);
      const entries = (await context.client().listDirectory(target))
        .filter((entry) => options.all || !entry.name.startsWith('.'))
        .sort(compareEntries);
      printDirectory(context, entries, options.long);
    }));
}

function registerFileCommands(program: Command, runtime: Runtime): void {
  program.command('cat')
    .argument('<path>')
    .description('Print a remote file')
    .action(async (path, _options, command) => perform(runtime, command, async (context) => {
      if (context.output.json || context.output.quiet) {
        throw new ConfigError('cat emits raw bytes and cannot be combined with --json or --quiet');
      }
      const response = await context.client().readFile(context.path(path));
      await pipeResponse(response, context.output.stdout, false);
    }));

  program.command('get')
    .argument('<remote>')
    .argument('[local]')
    .option('-f, --force', 'overwrite an existing local file')
    .description('Download a remote file')
    .action(async (remote, local, options, command) => perform(runtime, command, async (context) => {
      const remotePath = context.path(remote);
      const localPath = local || remoteBasename(remotePath);
      if (!options.force) {
        await access(localPath).then(() => {
          throw new ConfigError(`Local path already exists: ${localPath} (use --force to overwrite)`);
        }).catch((error: unknown) => {
          if (!(error instanceof Error && 'code' in error && error.code === 'ENOENT')) throw error;
        });
      }
      const response = await context.client().readFile(remotePath);
      const temporary = `${localPath}.airyfs-${process.pid}.tmp`;
      try {
        await pipeResponse(response, createWriteStream(temporary, { flags: 'wx' }));
        if (options.force) await renameLocal(temporary, localPath);
        else {
          await link(temporary, localPath);
          await removeLocal(temporary, { force: true });
        }
      } catch (error) {
        await removeLocal(temporary, { force: true });
        throw error;
      }
      context.output.success(`Downloaded ${remotePath} to ${localPath}`, { remote: remotePath, local: localPath });
    }));

  program.command('put')
    .argument('<local>')
    .argument('[remote]')
    .description('Upload a local file')
    .action(async (local, remote, _options, command) => perform(runtime, command, async (context) => {
      const localStats = await statLocal(local);
      if (!localStats.isFile()) throw new ConfigError(`Local path is not a file: ${local}`);
      const remotePath = context.path(remote || localBasename(local));
      await context.client().writeFile(remotePath, createReadStream(local) as NonNullable<RequestInit['body']>);
      context.output.success(`Uploaded ${local} to ${remotePath}`, { local, remote: remotePath });
    }));

  program.command('write')
    .argument('<remote>')
    .description('Write stdin to a remote file')
    .action(async (remote, _options, command) => perform(runtime, command, async (context) => {
      if (context.shellMode) throw new ConfigError('`write` cannot consume stdin inside `airyfs shell`; use `put` instead');
      const remotePath = context.path(remote);
      await context.client().writeFile(remotePath, context.stdin as NonNullable<RequestInit['body']>);
      context.output.success(`Wrote ${remotePath}`, { remote: remotePath });
    }));

  program.command('mkdir')
    .argument('<path>')
    .option('-p, --parents', 'create missing parent directories')
    .description('Create a remote directory')
    .action(async (path, options, command) => perform(runtime, command, async (context) => {
      const target = context.path(path);
      if (options.parents) {
        const parts = target.split('/').filter(Boolean);
        let current = '';
        for (const part of parts) {
          current += `/${part}`;
          await context.client().makeDirectory(current).catch((error: unknown) => {
            if (!(error instanceof AiryFSApiError && error.code === 'EEXIST')) throw error;
          });
        }
      } else {
        await context.client().makeDirectory(target);
      }
      context.output.success(`Created ${target}`, { path: target });
    }));

  program.command('rm')
    .argument('<path>')
    .option('-r, --recursive', 'remove directories recursively')
    .description('Remove a remote file or directory')
    .action(async (path, options, command) => perform(runtime, command, async (context) => {
      const target = context.path(path);
      if (target === '/') throw new ConfigError('Refusing to remove the volume root');
      const entry = await statRemote(context, target);
      if (entry.type === 'directory') await context.client().removeDirectory(target, options.recursive);
      else await context.client().deleteFile(target);
      context.output.success(`Removed ${target}`, { path: target });
    }));

  program.command('mv')
    .argument('<from>')
    .argument('<to>')
    .description('Move or rename a remote path')
    .action(async (from, to, _options, command) => perform(runtime, command, async (context) => {
      const source = context.path(from);
      const target = context.path(to);
      await context.client().rename(source, target);
      context.output.success(`Moved ${source} to ${target}`, { from: source, to: target });
    }));

  program.command('cp')
    .argument('<from>')
    .argument('<to>')
    .description('Copy a remote file')
    .action(async (from, to, _options, command) => perform(runtime, command, async (context) => {
      const source = context.path(from);
      const target = context.path(to);
      await context.client().copy(source, target);
      context.output.success(`Copied ${source} to ${target}`, { from: source, to: target });
    }));

  program.command('ln')
    .requiredOption('-s, --symbolic', 'create a symbolic link')
    .argument('<target>')
    .argument('<path>')
    .description('Create a remote symbolic link')
    .action(async (target, path, _options, command) => perform(runtime, command, async (context) => {
      const linkPath = context.path(path);
      await context.client().symlink(target, linkPath);
      context.output.success(`Linked ${linkPath} to ${target}`, { path: linkPath, target });
    }));

  program.command('readlink')
    .argument('<path>')
    .description('Print a symbolic-link target')
    .action(async (path, _options, command) => perform(runtime, command, async (context) => {
      context.output.value(await context.client().readlink(context.path(path)));
    }));

  program.command('truncate')
    .argument('<path>')
    .argument('<size>')
    .description('Resize a remote file')
    .action(async (path, size, _options, command) => perform(runtime, command, async (context) => {
      const bytes = parseSize(size);
      const target = context.path(path);
      await context.client().truncate(target, bytes);
      context.output.success(`Truncated ${target} to ${bytes} bytes`, { path: target, size: bytes });
    }));

  program.command('stat')
    .argument('<path>')
    .description('Show remote path metadata')
    .action(async (path, _options, command) => perform(runtime, command, async (context) => {
      context.output.value(await statRemote(context, context.path(path)));
    }));
}

function registerExecCommand(program: Command, runtime: Runtime): void {
  program.command('warm')
    .alias('wake')
    .option('--timeout <duration>', 'maximum startup time', '90s')
    .description('Start and mount the volume Container without changing its contents')
    .action(async (options, command) => perform(runtime, command, async (context) => {
      const spinner = createSpinner(context, `Warming ${context.volume}`);
      spinner?.start();
      try {
        await execWithRetry(context, ':', Date.now() + parseDuration(options.timeout), true);
        spinner?.stop();
      } catch (error) {
        spinner?.stop();
        throw error;
      }
      context.output.success(`Container is warm for ${context.volume}`, { volume: context.volume });
    }));

  program.command('exec')
    .argument('<command...>')
    .allowUnknownOption(true)
    .passThroughOptions()
    .option('--no-wait', 'fail immediately if another command is running')
    .option('--timeout <duration>', 'maximum startup and busy-wait time', '90s')
    .description('Execute a command in the volume Container')
    .action(async (parts: string[], options, command) => perform(runtime, command, async (context) => {
      const commandText = commandForExec(parts);
      const remoteDirectory = context.cwd === '/' ? '/volume' : `/volume${context.cwd}`;
      const fullCommand = `cd -- ${shellQuote(remoteDirectory)} && ${commandText}`;
      const timeout = options.wait ? parseDuration(options.timeout) : 0;
      const deadline = Date.now() + timeout;
      const spinner = createSpinner(context, `Running in ${context.volume}:${context.cwd}`);
      spinner?.start();
      let result: ExecResult;
      try {
        // Resolve startup failures with a retry-safe no-op before submitting the
        // user command, whose outcome can be ambiguous after a transport error.
        await execWithRetry(context, ':', deadline, true);
        result = await execWithRetry(context, fullCommand, deadline, false);
        spinner?.stop();
      } catch (error) {
        spinner?.stop();
        throw error;
      }

      if (context.output.json) {
        context.output.value(result);
      } else {
        context.output.text(result.stdout);
        if (result.stderr) context.output.stderr.write(result.stderr);
      }
      runtime.exitCode = result.exitCode;
    }));
}

function registerVolumeCommands(program: Command, runtime: Runtime): void {
  const volume = program.command('volume').description('Manage the selected volume');
  volume.command('create')
    .option('--chunk-size <size>', 'immutable chunk size (4k to 1m)', '256k')
    .description('Create or configure the selected volume')
    .action(async (options, command) => perform(runtime, command, async (context) => {
      const result = await context.client().createVolume(parseSize(options.chunkSize));
      context.output.success(`Volume ${context.volume} uses ${formatSize(result.chunkSize)} chunks`, result);
    }));

  volume.command('info')
    .description('Show selected-volume configuration')
    .action(async (_options, command) => perform(runtime, command, async (context) => {
      context.output.value(await context.client().getVolume());
    }));
}

function registerDiagnosticCommands(program: Command, runtime: Runtime): void {
  program.command('status')
    .alias('doctor')
    .description('Check endpoint, volume, Container, and Hrana health')
    .action(async (_options, command) => perform(runtime, command, async (context) => {
      const usage = await context.client().usage();
      if (context.output.json) {
        context.output.value({ session: context.named.name, endpoint: context.endpoint, volume: context.volume, ...usage });
      } else {
        context.output.table(['Component', 'Status'], [
          ['Endpoint', context.endpoint || '-'],
          ['Volume', context.volume || '-'],
          ['Container', usage.container.state],
          ['FUSE', usage.container.fuseMounted === undefined ? '-' : usage.container.fuseMounted ? 'mounted' : 'not mounted'],
          ['SQLite', formatSize(usage.sqliteBytes)],
          ['Hrana requests', usage.hrana.pipelineRequests],
          ['SQL statements', usage.hrana.sqlStatements],
        ]);
      }
    }));

  program.command('usage')
    .description('Show filesystem and runtime usage')
    .action(async (_options, command) => perform(runtime, command, async (context) => {
      context.output.value(await context.client().usage());
    }));

  program.command('perf')
    .description('Show Hrana request and SQL statement counters')
    .action(async (_options, command) => perform(runtime, command, async (context) => {
      context.output.value(await context.client().perf());
    }));

  program.command('db-info')
    .description('Show row counts for Durable Object SQLite tables')
    .action(async (_options, command) => perform(runtime, command, async (context) => {
      const info = await context.client().databaseInfo();
      if (context.output.json) context.output.value(info);
      else context.output.table(['Table', 'Rows'], Object.entries(info).map(([name, count]) => [name, count]));
    }));

  program.command('destroy')
    .option('-f, --force', 'skip confirmation')
    .description('Destroy the selected Container; volume data persists')
    .action(async (options, command) => perform(runtime, command, async (context) => {
      if (context.shellMode && !options.force) {
        throw new ConfigError('Interactive confirmation is unavailable inside `airyfs shell`; use `destroy --force`');
      }
      if (!options.force && !await confirmDestroy(context)) {
        context.output.value('Cancelled');
        return;
      }
      await context.client().destroyContainer();
      context.output.success(`Destroyed Container for ${context.volume}; volume data persists`);
    }));
}

function registerKvCommands(program: Command, runtime: Runtime): void {
  const kv = program.command('kv').description('Access the volume key-value store');
  kv.command('set')
    .argument('<key>')
    .argument('[value]')
    .description('Set a value; reads stdin when value is omitted')
    .action(async (key, value, _options, command) => perform(runtime, command, async (context) => {
      if (context.shellMode && value === undefined) {
        throw new ConfigError('`kv set` cannot consume stdin inside `airyfs shell`; provide the value as an argument');
      }
      const resolved = value === undefined ? await readStdin(context.stdin) : value;
      await context.client().setKv(key, resolved);
      context.output.success(`Set ${key}`, { key });
    }));

  kv.command('get')
    .argument('<key>')
    .description('Get a value')
    .action(async (key, _options, command) => perform(runtime, command, async (context) => {
      context.output.value(await context.client().getKv(key));
    }));
}

async function perform(runtime: Runtime, command: Command, handler: Handler): Promise<void> {
  const globals = command.optsWithGlobals<GlobalOptions>();
  try {
    await handler(await createContext(runtime, globals));
  } catch (error) {
    runtime.exitCode = 1;
    new Output({
      json: globals.json,
      color: globals.color,
      quiet: globals.quiet,
      stdout: runtime.stdout,
      stderr: runtime.stderr,
    }).error(error);
  }
}

async function performConfig(
  runtime: Runtime,
  command: Command,
  handler: (output: Output) => Promise<void>,
): Promise<void> {
  const globals = command.optsWithGlobals() as GlobalOptions;
  const output = new Output({
    json: globals.json,
    color: globals.color,
    quiet: globals.quiet,
    stdout: runtime.stdout,
    stderr: runtime.stderr,
  });
  try {
    await handler(output);
  } catch (error) {
    runtime.exitCode = 1;
    output.error(error);
  }
}

async function statRemote(context: CommandContext, path: string): Promise<DirectoryEntry> {
  if (path === '/') {
    return { name: '/', type: 'directory', ino: 1, mode: 0o40755, nlink: 1, uid: 0, gid: 0, size: 0, atime: 0, mtime: 0, ctime: 0 };
  }
  const parent = remoteDirname(path);
  const name = remoteBasename(path);
  const entry = (await context.client().listDirectory(parent)).find((candidate) => candidate.name === name);
  if (!entry) throw new AiryFSApiError(404, 'ENOENT', `No such file or directory: ${path}`, path);
  return entry;
}

function printDirectory(context: CommandContext, entries: DirectoryEntry[], long: boolean): void {
  if (context.output.json) {
    context.output.value(entries);
    return;
  }
  if (long) {
    context.output.table(
      ['Mode', 'Links', 'Size', 'Modified', 'Name'],
      entries.map((entry) => [
        formatMode(entry), entry.nlink, formatSize(entry.size), formatTime(entry.mtime), displayName(entry),
      ]),
    );
  } else {
    context.output.table(
      ['Name', 'Type', 'Size', 'Modified'],
      entries.map((entry) => [displayName(entry), entry.type, formatSize(entry.size), formatTime(entry.mtime)]),
    );
  }
}

function displayName(entry: DirectoryEntry): string {
  const name = escapeControls(entry.name);
  if (entry.type === 'directory') return `${name}/`;
  if (entry.type === 'symlink') return `${name}@`;
  return name;
}

function compareEntries(left: DirectoryEntry, right: DirectoryEntry): number {
  if (left.type === 'directory' && right.type !== 'directory') return -1;
  if (left.type !== 'directory' && right.type === 'directory') return 1;
  return left.name.localeCompare(right.name);
}

function formatMode(entry: DirectoryEntry): string {
  const type = entry.type === 'directory' ? 'd' : entry.type === 'symlink' ? 'l' : '-';
  const bits = entry.mode & 0o777;
  const permissions = [0o400, 0o200, 0o100, 0o040, 0o020, 0o010, 0o004, 0o002, 0o001]
    .map((bit, index) => bits & bit ? 'rwx'[index % 3] : '-')
    .join('');
  return `${type}${permissions}`;
}

function formatSize(bytes: number): string {
  if (bytes < 1024) return `${bytes} B`;
  const units = ['KiB', 'MiB', 'GiB', 'TiB'];
  let value = bytes;
  let unit = -1;
  do {
    value /= 1024;
    unit++;
  } while (value >= 1024 && unit < units.length - 1);
  return `${value >= 10 ? value.toFixed(0) : value.toFixed(1)} ${units[unit]}`;
}

function formatTime(seconds: number): string {
  return seconds ? new Date(seconds * 1000).toLocaleString() : '-';
}

function parseSize(value: string): number {
  const match = /^(\d+(?:\.\d+)?)\s*(b|k|kb|kib|m|mb|mib|g|gb|gib)?$/i.exec(value.trim());
  if (!match) throw new ConfigError(`Invalid size: ${value}`);
  const suffix = (match[2] || 'b').toLowerCase();
  const exponent = suffix.startsWith('g') ? 3 : suffix.startsWith('m') ? 2 : suffix.startsWith('k') ? 1 : 0;
  const result = Number(match[1]) * 1024 ** exponent;
  if (!Number.isSafeInteger(result) || result < 0) throw new ConfigError(`Invalid size: ${value}`);
  return result;
}

function parseDuration(value: string): number {
  const match = /^(\d+(?:\.\d+)?)\s*(ms|s|m)?$/i.exec(value.trim());
  if (!match) throw new ConfigError(`Invalid duration: ${value}`);
  const unit = (match[2] || 'ms').toLowerCase();
  const multiplier = unit === 'm' ? 60_000 : unit === 's' ? 1_000 : 1;
  return Number(match[1]) * multiplier;
}

function commandForExec(parts: string[]): string {
  return parts.length === 1 ? parts[0] : parts.map(shellQuote).join(' ');
}

function shellQuote(value: string): string {
  if (/^[A-Za-z0-9_@%+=:,./-]+$/.test(value)) return value;
  return `'${value.replace(/'/g, `'"'"'`)}'`;
}

async function execWithRetry(
  context: CommandContext,
  command: string,
  deadline: number,
  retryTransient: boolean,
): Promise<ExecResult> {
  let delay = 150;
  let previousError: unknown;
  let attempted = false;
  while (true) {
    if (attempted && Date.now() >= deadline) throw previousError;
    attempted = true;
    try {
      const remaining = deadline - Date.now();
      const signal = retryTransient && remaining > 0
        ? AbortSignal.timeout(Math.max(1, remaining))
        : undefined;
      return await context.client().exec(command, signal);
    } catch (error) {
      const retryable = (error instanceof AiryFSApiError && error.code === 'EXEC_BUSY')
        || (retryTransient && (error instanceof AiryFSTransportError
          || error instanceof AiryFSApiError && [502, 503, 504].includes(error.status)));
      if (!retryable || Date.now() >= deadline) throw error;
      previousError = error;
      await new Promise((resolve) => setTimeout(resolve, Math.min(delay, Math.max(0, deadline - Date.now()))));
      delay = Math.min(delay * 2, 1_000);
    }
  }
}

function createSpinner(context: CommandContext, text: string): ReturnType<typeof ora> | null {
  const stream = context.output.stderr as NodeJS.WriteStream;
  if (context.shellMode || context.output.json || context.output.quiet || !stream.isTTY) return null;
  return ora({ text, stream });
}

async function pipeResponse(
  response: Response,
  destination: NodeJS.WritableStream,
  end = true,
): Promise<void> {
  if (!response.body) return;
  await pipeline(Readable.fromWeb(response.body as never), destination, { end });
}

async function readStdin(stdin: NodeJS.ReadableStream): Promise<string> {
  let result = '';
  for await (const chunk of stdin) result += chunk.toString();
  return result;
}

async function confirmDestroy(context: CommandContext): Promise<boolean> {
  const input = context.stdin as NodeJS.ReadStream;
  if (!input.isTTY) throw new ConfigError('Refusing to prompt without a TTY; use --force');
  const readline = createInterface({ input, output: context.output.stderr as NodeJS.WriteStream });
  try {
    const answer = await readline.question(`Destroy the Container for ${context.volume}? Volume data will persist. [y/N] `);
    return /^y(es)?$/i.test(answer.trim());
  } finally {
    readline.close();
  }
}

async function requiredInput(
  runtime: Runtime,
  value: string | undefined,
  prompt: string,
  field: string,
): Promise<string> {
  if (value?.trim()) return value.trim();
  let answer: string;
  if (runtime.prompt) {
    answer = await runtime.prompt(prompt);
  } else {
    const input = runtime.stdin as NodeJS.ReadStream;
    if (!input?.isTTY) {
      throw new ConfigError(`Missing ${field}; provide it explicitly when not running interactively`);
    }
    const readline = createInterface({
      input,
      output: (runtime.stderr || process.stderr) as NodeJS.WriteStream,
    });
    try {
      answer = await readline.question(prompt);
    } finally {
      readline.close();
    }
  }
  if (!answer.trim()) throw new ConfigError(`${field} cannot be empty`);
  return answer.trim();
}

function escapeControls(value: string): string {
  return value.replace(/[\x00-\x1f\x7f-\x9f]/g, (character) => {
    return `\\x${character.charCodeAt(0).toString(16).padStart(2, '0')}`;
  });
}
