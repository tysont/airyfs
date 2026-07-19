// ABOUTME: Registers the complete AiryFS command surface and maps commands to API calls.
// ABOUTME: One-shot and interactive-shell execution share these same handlers.

import { spawn } from 'node:child_process';
import { createHash } from 'node:crypto';
import { createReadStream, createWriteStream, existsSync } from 'node:fs';
import {
  access,
  link,
  mkdir as mkdirLocal,
  rename as renameLocal,
  rm as removeLocal,
  stat as statLocal,
} from 'node:fs/promises';
import { basename as localBasename, dirname as localDirname, join as joinLocal, resolve as resolveLocal } from 'node:path';
import { Readable } from 'node:stream';
import { pipeline } from 'node:stream/promises';
import { createInterface } from 'node:readline/promises';
import { Command } from 'commander';
import ora from 'ora';
import { AiryFSClient } from '../api/client.js';
import { createLocalTreeStream, extractLocalTree } from '../api/archive.js';
import { resumableDownload, resumableUpload, type TransferProgress } from '../api/resume.js';
import { AiryFSApiError, AiryFSTransportError } from '../api/errors.js';
import { encodeRemotePath, remoteBasename, remoteDirname } from '../api/paths.js';
import type {
  ChangeEvent,
  ChangePage,
  DirectoryEntry,
  ExecResult,
  Job,
  JobLogEntry,
  JobStatus,
  Operation,
  WebhookEvent,
} from '../api/types.js';
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
  registerAssetCommands(program, runtime);
  registerSearchCommands(program, runtime);
  registerExecCommand(program, runtime);
  registerJobCommands(program, runtime);
  registerScheduleCommands(program, runtime);
  registerServiceCommands(program, runtime);
  registerWatchCommand(program, runtime);
  registerWebhookCommands(program, runtime);
  registerVolumeCommands(program, runtime);
  registerSnapshotCommands(program, runtime);
  registerAuthCommands(program, runtime);
  registerCapabilityCommands(program, runtime);
  registerDiagnosticCommands(program, runtime);
  registerKvCommands(program, runtime);
  registerSqlCommand(program, runtime);
  registerDeployCommands(program, runtime);
  registerSiteCommands(program, runtime);

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

function registerSqlCommand(program: Command, runtime: Runtime): void {
  program.command('sql')
    .argument('<statement>')
    .option('--arg <json>', 'positional JSON argument; repeat in placeholder order', collectOption, [])
    .description('Execute scoped SQLite against app_* tables')
    .action(async (statement, options, command) => perform(runtime, command, async (context) => {
      const args = (options.arg as string[]).map((value) => {
        try { return JSON.parse(value) as string | number | null | { base64: string }; }
        catch { throw new ConfigError(`Invalid JSON argument: ${value}`); }
      });
      const result = await context.client().sql(statement, args);
      if (context.output.json) {
        context.output.value(result);
      } else if (result.columns.length > 0) {
        context.output.table(result.columns, result.rows.map((row) => row.map((value) => {
          if (value === null) return 'NULL';
          if (typeof value === 'object') return `<blob ${value.base64.length} base64 chars>`;
          return value;
        })));
        if (result.truncated) context.output.value('Results truncated at 1,000 rows');
      } else {
        context.output.success(`SQL executed; ${result.rowsWritten} row${result.rowsWritten === 1 ? '' : 's'} written`, result);
      }
    }));
}

function registerServiceCommands(program: Command, runtime: Runtime): void {
  const service = program.command('service').description('Manage persistent Container preview services');
  service.command('create')
    .argument('<name>')
    .argument('<command...>')
    .option('--cwd <path>', 'remote working directory', '.')
    .option('--env <KEY=VALUE>', 'environment variable', collectOption, [])
    .option('--public', 'publish without authentication')
    .description('Create and start a preview service; command must bind $PORT')
    .action(async (name, parts: string[], options, command) => perform(runtime, command, async (context) => {
      const env: Record<string, string> = {};
      for (const assignment of options.env as string[]) {
        const split = assignment.indexOf('=');
        if (split < 1) throw new ConfigError(`Invalid --env value: ${assignment}`);
        env[assignment.slice(0, split)] = assignment.slice(split + 1);
      }
      const record = await context.client().createService({
        name, command: commandForExec(parts), cwd: context.path(options.cwd), env, public: Boolean(options.public),
      });
      const url = record.public ? `${context.endpoint}/p/${encodeURIComponent(context.volume)}/${encodeURIComponent(record.name)}/` : null;
      const suffix = url ? ` at ${url}` : '';
      context.output.success(`Created preview service ${record.name} on $PORT=${record.port}${suffix}`, { ...record, url });
    }));

  service.command('list', { isDefault: true })
    .description('List preview services')
    .action(async (_options, command) => perform(runtime, command, async (context) => {
      const records = await context.client().listServices();
      if (context.output.json) context.output.value(records);
      else context.output.table(['Name', 'Port', 'Enabled', 'Public', 'Directory', 'Command'], records.map((record) => [
        record.name, record.port, record.enabled ? 'yes' : 'no', record.public ? 'yes' : 'no', record.cwd, truncateCommand(record.command),
      ]));
    }));

  for (const action of ['start', 'stop'] as const) {
    service.command(action).argument('<name>').description(`${action} a preview service`)
      .action(async (name, _options, command) => perform(runtime, command, async (context) => {
        const record = action === 'start' ? await context.client().startService(name) : await context.client().stopService(name);
        context.output.success(`${action === 'start' ? 'Started' : 'Stopped'} preview service ${name}`, record);
      }));
  }
  service.command('delete').alias('rm').argument('<name>').description('Stop and delete a preview service')
    .action(async (name, _options, command) => perform(runtime, command, async (context) => {
      const record = await context.client().deleteService(name);
      context.output.success(`Deleted preview service ${name}`, record);
    }));

  service.command('logs')
    .argument('<name>')
    .option('--follow', 'poll for new output until interrupted')
    .option('--after <seq>', 'only show log entries after this seq cursor')
    .description('Print ephemeral preview service stdout/stderr')
    .action(async (name, options, command) => perform(runtime, command, async (context) => {
      const after = options.after === undefined ? undefined : parseCursor(options.after);
      if (context.output.json && options.follow) throw new ConfigError('--json cannot be combined with --follow');
      if (context.output.json) {
        context.output.value(await context.client().getServiceLogs(name, { after }));
        return;
      }
      await printServiceLogs(context, name, after, Boolean(options.follow), runtime);
    }));
}

async function printServiceLogs(
  context: CommandContext,
  name: string,
  after: number | undefined,
  follow: boolean,
  runtime: Runtime,
): Promise<void> {
  const controller = new AbortController();
  let interrupted = false;
  const onInterrupt = (): void => { interrupted = true; controller.abort(); };
  process.once('SIGINT', onInterrupt);
  let cursor = after;
  let generation: string | undefined;
  try {
    while (true) {
      const page = await context.client().getServiceLogs(name, {
        after: cursor,
        generation,
        signal: controller.signal,
      });
      if (page.reset) {
        context.output.stderr.write('Preview service restarted; log cursor reset\n');
        cursor = undefined;
      }
      if (page.truncated) {
        context.output.stderr.write(`Preview service logs truncated before sequence ${page.earliestSeq}\n`);
      }
      generation = page.generation ?? undefined;
      for (const entry of page.entries) {
        writeJobLogEntry(context, entry);
        cursor = entry.seq;
      }
      if (!follow || controller.signal.aborted) break;
      await sleep(jobPollIntervalMs(), controller.signal);
      if (controller.signal.aborted) break;
    }
  } catch (error) {
    if (!interrupted) throw error;
  } finally {
    process.removeListener('SIGINT', onInterrupt);
  }
  if (interrupted) runtime.exitCode = 130;
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

  session.command('export')
    .argument('[name]')
    .description('Print a portable session blob (endpoint, volume, token) for another computer')
    .action(async (name, _options, command) => performConfig(runtime, command, async (output) => {
      const globals = (command as Command).optsWithGlobals() as GlobalOptions;
      const selected = await resolveRuntimeSession(runtime, globals, name);
      const blob = encodeSessionBlob(selected.name, selected.session);
      if (output.json) {
        output.value({ name: selected.name, blob });
        return;
      }
      // The blob embeds a bearer token; surface it once and warn like other credentials.
      output.stderr.write(output.dim('This blob contains a credential. Share it only over a trusted channel.\n'));
      output.value(blob);
    }));

  session.command('import')
    .argument('<blob>')
    .argument('[name]', 'override the session name from the blob')
    .description('Recreate a session from a blob produced by `session export`')
    .action(async (blob, name, _options, command) => performConfig(runtime, command, async (output) => {
      const decoded = decodeSessionBlob(blob);
      const sessionName = name || decoded.name;
      const created = await runtime.sessions.create(sessionName, {
        endpoint: decoded.endpoint,
        volume: decoded.volume,
      });
      if (decoded.token) await runtime.sessions.setToken(created.name, decoded.token);
      runtime.onSessionEvent?.({ type: 'select', name: created.name });
      output.success(`Imported and selected session ${created.name}`, {
        name: created.name,
        endpoint: decoded.endpoint,
        volume: decoded.volume,
        token: decoded.token ? 'configured' : 'none',
      });
    }));
}

interface SessionBlob {
  name: string;
  endpoint: string;
  volume: string;
  token?: string;
}

/** Encode a session as a base64url JSON blob for transfer between computers. */
function encodeSessionBlob(name: string, session: { endpoint: string; volume: string; token?: string }): string {
  const payload: SessionBlob = {
    name,
    endpoint: session.endpoint,
    volume: session.volume,
    ...(session.token ? { token: session.token } : {}),
  };
  return `airyfs1:${Buffer.from(JSON.stringify(payload), 'utf8').toString('base64url')}`;
}

function decodeSessionBlob(blob: string): SessionBlob {
  const trimmed = blob.trim();
  const body = trimmed.startsWith('airyfs1:') ? trimmed.slice('airyfs1:'.length) : trimmed;
  let parsed: unknown;
  try {
    parsed = JSON.parse(Buffer.from(body, 'base64url').toString('utf8'));
  } catch {
    throw new ConfigError('Invalid session blob');
  }
  if (
    typeof parsed !== 'object' || parsed === null ||
    typeof (parsed as SessionBlob).name !== 'string' ||
    typeof (parsed as SessionBlob).endpoint !== 'string' ||
    typeof (parsed as SessionBlob).volume !== 'string'
  ) {
    throw new ConfigError('Invalid session blob');
  }
  return parsed as SessionBlob;
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

  program.command('tree')
    .argument('[path]', 'remote directory', '.')
    .option('-d, --depth <count>', 'maximum depth', Number, 20)
    .option('--limit <count>', 'maximum entries', Number, 1000)
    .description('Print a remote directory tree without starting the Container')
    .action(async (path, options, command) => perform(runtime, command, async (context) => {
      const response = await context.client().tree(context.path(path), {
        depth: validateTreeInteger(options.depth, 'depth', 0, 100),
        limit: validateTreeInteger(options.limit, 'limit', 1, 100_000),
      });
      if (context.output.json) {
        context.output.value(response);
        return;
      }
      context.output.text(`${response.root}\n`);
      for (const entry of response.entries) {
        const marker = entry.type === 'directory' ? '/' : entry.type === 'symlink' ? '@' : '';
        context.output.text(`${'  '.repeat(entry.depth - 1)}${entry.name}${marker}\n`);
      }
      if (response.truncated && !context.output.quiet) {
        context.output.stderr.write('Tree truncated; increase --depth or --limit.\n');
      }
    }));
}

function validateTreeInteger(value: number, name: string, minimum: number, maximum: number): number {
  if (!Number.isSafeInteger(value) || value < minimum || value > maximum) {
    throw new ConfigError(`${name} must be between ${minimum} and ${maximum}`);
  }
  return value;
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

  program.command('tail')
    .argument('<path>')
    .option('-n, --lines <count>', 'number of trailing lines', Number, 10)
    .option('-c, --bytes <count>', 'number of trailing bytes', Number)
    .option('-f, --follow', 'follow appended bytes')
    .option('-F, --retry', 'follow and wait for a removed path to reappear')
    .description('Print and optionally follow the end of a remote file')
    .action(async (path, options, command) => perform(runtime, command, async (context) => {
      if (context.output.json || context.output.quiet) {
        throw new ConfigError('tail emits raw bytes and cannot be combined with --json or --quiet');
      }
      const lines = validateTailCount(options.lines, '--lines');
      const bytes = options.bytes === undefined ? undefined : validateTailCount(options.bytes, '--bytes');
      if (bytes !== undefined && command.getOptionValueSource('lines') === 'cli') {
        throw new ConfigError('--lines and --bytes are mutually exclusive');
      }
      await runTail(context, context.path(path), {
        lines, bytes, follow: Boolean(options.follow || options.retry), retry: Boolean(options.retry), runtime,
      });
    }));

  program.command('get')
    .argument('<remote>')
    .argument('[local]')
    .option('-f, --force', 'overwrite an existing local file')
    .option('--resume', 'resumable, checksummed download with a partial sidecar')
    .description('Download a remote file')
    .action(async (remote, local, options, command) => perform(runtime, command, async (context) => {
      const remotePath = context.path(remote);
      const localPath = local || remoteBasename(remotePath);
      await downloadFile(context, remotePath, localPath, { force: options.force, resume: options.resume });
    }));

  program.command('put')
    .argument('<local>')
    .argument('[remote]')
    .option('--resume', 'resumable, checksummed upload in 1 MiB chunks')
    .description('Upload a local file')
    .action(async (local, remote, options, command) => perform(runtime, command, async (context) => {
      const localStats = await statLocal(local);
      if (!localStats.isFile()) throw new ConfigError(`Local path is not a file: ${local}`);
      const remotePath = context.path(remote || localBasename(local));
      await uploadFile(context, local, remotePath, { resume: options.resume, size: localStats.size });
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
    .option('--permanent', 'delete immediately instead of moving to trash')
    .description('Remove a remote file or directory')
    .action(async (path, options, command) => perform(runtime, command, async (context) => {
      const target = context.path(path);
      if (target === '/') throw new ConfigError('Refusing to remove the volume root');
      const entry = await statRemote(context, target);
      const trashed = entry.type === 'directory'
        ? await context.client().removeDirectory(target, options.recursive, options.permanent)
        : await context.client().deleteFile(target, options.permanent);
      context.output.success(options.permanent ? `Permanently removed ${target}` : `Moved ${target} to trash`, trashed ?? { path: target });
    }));

  const trash = program.command('trash').description('List, restore, and permanently purge deleted paths');
  trash.command('list', { isDefault: true })
    .description('List recoverable deleted paths')
    .action(async (_options, command) => perform(runtime, command, async (context) => {
      const entries = await context.client().listTrash();
      if (context.output.json) context.output.value(entries);
      else context.output.table(['ID', 'Deleted', 'Type', 'Size', 'Original path'], entries.map((entry) => [
        entry.id, formatTime(entry.deletedAt), entry.type, formatSize(entry.size), entry.originalPath,
      ]));
    }));
  trash.command('restore')
    .argument('<id>')
    .argument('[destination]')
    .description('Restore a trashed path')
    .action(async (id, destination, _options, command) => perform(runtime, command, async (context) => {
      const result = await context.client().restoreTrash(id, destination ? context.path(destination) : undefined);
      context.output.success(`Restored ${result.restoredPath}`, result);
    }));
  trash.command('purge')
    .argument('<id>')
    .description('Permanently delete a trash entry')
    .action(async (id, _options, command) => perform(runtime, command, async (context) => {
      const result = await context.client().purgeTrash(id);
      context.output.success(`Permanently deleted ${result.originalPath}`, result);
    }));

  program.command('undo')
    .description('Restore the most recently trashed path')
    .action(async (_options, command) => perform(runtime, command, async (context) => {
      const result = await context.client().undoTrash();
      context.output.success(`Restored ${result.restoredPath}`, result);
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

  program.command('push')
    .argument('<local-directory>')
    .argument('[remote-directory]')
    .option('--replace', 'replace an existing remote directory')
    .description('Upload a local directory tree as a transactional archive')
    .action(async (localDir, remote, options, command) => perform(runtime, command, async (context) => {
      const localStats = await statLocal(localDir);
      if (!localStats.isDirectory()) throw new ConfigError(`Local path is not a directory: ${localDir}`);
      const defaultName = localBasename(resolveLocal(localDir));
      const remotePath = context.path(remote || defaultName);
      await uploadTree(context, localDir, remotePath, { replace: options.replace });
    }));

  program.command('pull')
    .argument('<remote-directory>')
    .argument('[local-directory]')
    .option('-f, --force', 'replace an existing local directory')
    .description('Download a remote directory tree as a transactional archive')
    .action(async (remote, local, options, command) => perform(runtime, command, async (context) => {
      const remotePath = context.path(remote);
      const localPath = local || remoteBasename(remotePath);
      await downloadTree(context, remotePath, localPath, { force: options.force });
    }));

  program.command('upload')
    .argument('<local>')
    .argument('[remote]')
    .option('-r, --recursive', 'upload a directory tree')
    .option('--replace', 'replace an existing remote directory (directories only)')
    .option('--resume', 'resumable, checksummed upload in 1 MiB chunks (files only)')
    .description('Upload a local file or directory, choosing the transfer automatically')
    .action(async (local, remote, options, command) => perform(runtime, command, async (context) => {
      const localStats = await statLocal(local);
      if (localStats.isDirectory()) {
        if (!options.recursive) {
          throw new ConfigError(`${local} is a directory (use -r/--recursive to upload it)`);
        }
        if (options.resume) throw new ConfigError('--resume applies to files, not directories');
        const remotePath = context.path(remote || localBasename(resolveLocal(local)));
        await uploadTree(context, local, remotePath, { replace: options.replace });
        return;
      }
      if (!localStats.isFile()) throw new ConfigError(`Local path is not a file or directory: ${local}`);
      if (options.replace) throw new ConfigError('--replace applies to directories, not files');
      const remotePath = context.path(remote || localBasename(local));
      await uploadFile(context, local, remotePath, { resume: options.resume, size: localStats.size });
    }));

  program.command('download')
    .argument('<remote>')
    .argument('[local]')
    .option('-r, --recursive', 'download a directory tree')
    .option('-f, --force', 'overwrite an existing local path')
    .option('--resume', 'resumable, checksummed download with a partial sidecar (files only)')
    .description('Download a remote file or directory, choosing the transfer automatically')
    .action(async (remote, local, options, command) => perform(runtime, command, async (context) => {
      const remotePath = context.path(remote);
      const localPath = local || remoteBasename(remotePath);
      const entry = await statRemote(context, remotePath);
      if (entry.type === 'directory') {
        if (!options.recursive) {
          throw new ConfigError(`${remotePath} is a directory (use -r/--recursive to download it)`);
        }
        if (options.resume) throw new ConfigError('--resume applies to files, not directories');
        await downloadTree(context, remotePath, localPath, { force: options.force });
        return;
      }
      await downloadFile(context, remotePath, localPath, { force: options.force, resume: options.resume });
    }));
}

function validateTailCount(value: number, option: string): number {
  if (!Number.isSafeInteger(value) || value < 0) throw new ConfigError(`${option} must be a non-negative integer`);
  return value;
}

async function runTail(
  context: CommandContext,
  path: string,
  options: { lines: number; bytes?: number; follow: boolean; retry: boolean; runtime: Runtime },
): Promise<void> {
  const controller = new AbortController();
  let interrupted = false;
  const onInterrupt = (): void => { interrupted = true; controller.abort(); };
  process.once('SIGINT', onInterrupt);
  try {
    let cursor = (await context.client().getChanges({ path, since: 'latest', signal: controller.signal })).cursor;
    let offset = await writeTailInitial(context, path, options.lines, options.bytes, controller.signal);
    if (!options.follow) return;
    while (!controller.signal.aborted) {
      const page = await context.client().getChanges({ path, since: cursor, wait: 25_000, signal: controller.signal });
      reportChangeGap(context, page);
      cursor = page.cursor;
      if (!page.events.some((event) => event.path === path || event.oldPath === path)) continue;
      try {
        const head = await context.client().headFile(path);
        const size = tailResponseSize(head);
        if (size < offset) offset = 0;
        if (size > offset) {
          const response = await context.client().readFile(path, `bytes=${offset}-`);
          const total = tailResponseTotal(response, size);
          await pipeResponse(response, context.output.stdout, false);
          offset = total;
        }
      } catch (error) {
        if (error instanceof AiryFSApiError && error.code === 'ENOENT') {
          if (!options.retry) return;
          offset = 0;
          continue;
        }
        throw error;
      }
    }
  } catch (error) {
    if (!interrupted && !controller.signal.aborted) throw error;
  } finally {
    process.removeListener('SIGINT', onInterrupt);
  }
  if (interrupted) options.runtime.exitCode = 130;
}

async function writeTailInitial(
  context: CommandContext,
  path: string,
  lines: number,
  bytes: number | undefined,
  signal: AbortSignal,
): Promise<number> {
  const head = await context.client().headFile(path);
  const size = tailResponseSize(head);
  if (size === 0 || bytes === 0 || (bytes === undefined && lines === 0)) return size;
  if (bytes !== undefined) {
    const response = await context.client().readFile(path, `bytes=-${Math.min(bytes, size)}`);
    await pipeResponse(response, context.output.stdout, false);
    return tailResponseTotal(response, size);
  }
  let length = Math.min(size, 64 * 1024);
  while (true) {
    signal.throwIfAborted();
    const response = await context.client().readFile(path, `bytes=-${length}`);
    const data = new Uint8Array(await response.arrayBuffer());
    if (length >= size || countByte(data, 10) > lines) {
      context.output.stdout.write(lastTailLines(data, lines));
      return tailResponseTotal(response, size);
    }
    length = Math.min(size, length * 2);
  }
}

function tailResponseSize(response: Response): number {
  const value = Number(response.headers.get('Content-Length'));
  if (!Number.isSafeInteger(value) || value < 0) throw new ConfigError('File response is missing a valid Content-Length');
  return value;
}

function tailResponseTotal(response: Response, fallback: number): number {
  const match = response.headers.get('Content-Range')?.match(/\/(\d+)$/);
  return match ? Number(match[1]) : fallback;
}

function countByte(data: Uint8Array, target: number): number {
  let count = 0;
  for (const byte of data) if (byte === target) count++;
  return count;
}

function lastTailLines(data: Uint8Array, lines: number): Uint8Array {
  let remaining = lines;
  let index = data.length;
  if (index > 0 && data[index - 1] === 10) index--;
  while (index > 0 && remaining > 0) if (data[--index] === 10) remaining--;
  return data.subarray(remaining === 0 ? index + 1 : 0);
}

function registerAssetCommands(program: Command, runtime: Runtime): void {
  const asset = program.command('asset').description('Store immutable SHA-256-addressed files');

  asset.command('put')
    .argument('<local>', 'local file')
    .description('Hash and publish a local file under its content address')
    .action(async (local, _options, command) => perform(runtime, command, async (context) => {
      const stats = await statLocal(local);
      if (!stats.isFile()) throw new ConfigError(`Local path is not a file: ${local}`);
      const checksum = await sha256LocalFile(local);
      const result = await context.client().putAsset(
        checksum,
        createReadStream(local) as NonNullable<RequestInit['body']>,
      );
      context.output.success(
        result.created ? `Published asset ${checksum}` : `Asset ${checksum} already exists`,
        { ...result, local },
      );
    }));

  asset.command('get')
    .argument('<sha256>', 'asset SHA-256')
    .argument('[local]', 'local destination')
    .option('-f, --force', 'overwrite an existing local file')
    .description('Download an immutable asset')
    .action(async (checksum, local, options, command) => perform(runtime, command, async (context) => {
      const localPath = local || checksum;
      await saveResponse(await context.client().getAsset(checksum), localPath, Boolean(options.force));
      context.output.success(`Downloaded asset ${checksum} to ${localPath}`, { checksum, local: localPath });
    }));
}

function registerSearchCommands(program: Command, runtime: Runtime): void {
  program.command('find')
    .argument('[path]', 'remote root', '.')
    .requiredOption('-n, --name <text>', 'literal filename text')
    .option('--limit <count>', 'maximum results', Number, 100)
    .description('Find paths by filename without starting the Container')
    .action(async (path, options, command) => perform(runtime, command, async (context) => {
      await printSearch(context, await context.client().search({
        mode: 'find', path: context.path(path), pattern: options.name, limit: validateSearchLimit(options.limit),
      }));
    }));

  program.command('glob')
    .argument('<pattern>', 'glob relative to the remote root; ** crosses directories')
    .argument('[path]', 'remote root', '.')
    .option('--limit <count>', 'maximum results', Number, 100)
    .description('Find paths by glob without starting the Container')
    .action(async (pattern, path, options, command) => perform(runtime, command, async (context) => {
      await printSearch(context, await context.client().search({
        mode: 'glob', path: context.path(path), pattern, limit: validateSearchLimit(options.limit),
      }));
    }));

  program.command('grep')
    .argument('<pattern>')
    .argument('[path]', 'remote root', '.')
    .option('-E, --regex', 'interpret pattern as a regular expression')
    .option('-i, --ignore-case', 'case-insensitive matching')
    .option('--limit <count>', 'maximum matches', Number, 100)
    .description('Search bounded remote file contents without starting the Container')
    .action(async (pattern, path, options, command) => perform(runtime, command, async (context) => {
      await printSearch(context, await context.client().search({
        mode: 'grep', path: context.path(path), pattern,
        regex: Boolean(options.regex), ignoreCase: Boolean(options.ignoreCase), limit: validateSearchLimit(options.limit),
      }));
    }));
}

async function printSearch(context: CommandContext, response: import('../api/types.js').SearchResponse): Promise<void> {
  if (context.output.json) {
    context.output.value(response);
    return;
  }
  for (const result of response.results) {
    context.output.text(result.line === undefined
      ? `${result.path}\n`
      : `${result.path}:${result.line}:${result.column}:${result.text}\n`);
  }
  if (response.truncated && !context.output.quiet) {
    context.output.stderr.write('Results truncated; increase --limit or narrow the search.\n');
  }
}

function validateSearchLimit(value: number): number {
  if (!Number.isSafeInteger(value) || value < 1 || value > 1000) {
    throw new ConfigError('Search limit must be between 1 and 1000');
  }
  return value;
}

async function sha256LocalFile(path: string): Promise<string> {
  const hash = createHash('sha256');
  for await (const chunk of createReadStream(path)) hash.update(chunk);
  return hash.digest('hex');
}

function summarize(summary: { files: number; directories: number; symlinks: number; bytes: number }): string {
  return `${summary.files} files, ${summary.directories} dirs, ${summary.symlinks} symlinks, ${summary.bytes} bytes`;
}

async function localExists(path: string): Promise<boolean> {
  try {
    await access(path);
    return true;
  } catch (error) {
    if (error instanceof Error && 'code' in error && error.code === 'ENOENT') return false;
    throw error;
  }
}

// ---------------------------------------------------------------------------
// Shared transfer helpers
//
// `put`/`get` (files) and `push`/`pull` (directory trees) delegate here, and the
// smart `upload`/`download` verbs reuse the same paths after detecting whether
// the source is a file or a directory. One implementation keeps progress,
// atomic-replace, and resumable behavior identical across every entry point.
// ---------------------------------------------------------------------------

async function uploadFile(
  context: CommandContext,
  local: string,
  remotePath: string,
  options: { resume?: boolean; size: number },
): Promise<void> {
  if (options.resume) {
    const spinner = createSpinner(context, `Uploading ${local}`);
    spinner?.start();
    try {
      const result = await resumableUpload(
        context.client(), local, options.size, remotePath,
        (progress) => updateTransfer(spinner, `Uploading ${local}`, progress),
      );
      spinner?.stop();
      context.output.success(`Uploaded ${local} to ${remotePath}`, result);
    } catch (error) {
      spinner?.stop();
      throw error;
    }
    return;
  }
  await context.client().writeFile(remotePath, createReadStream(local) as NonNullable<RequestInit['body']>);
  context.output.success(`Uploaded ${local} to ${remotePath}`, { local, remote: remotePath });
}

async function downloadFile(
  context: CommandContext,
  remotePath: string,
  localPath: string,
  options: { force?: boolean; resume?: boolean },
): Promise<void> {
  if (options.resume) {
    const spinner = createSpinner(context, `Downloading ${remotePath}`);
    spinner?.start();
    try {
      const result = await resumableDownload(
        context.client(), remotePath, localPath, { force: options.force },
        (progress) => updateTransfer(spinner, `Downloading ${remotePath}`, progress),
      );
      spinner?.stop();
      context.output.success(`Downloaded ${remotePath} to ${localPath}`, { remote: remotePath, local: localPath, ...result });
    } catch (error) {
      spinner?.stop();
      throw error;
    }
    return;
  }
  await saveResponse(await context.client().readFile(remotePath), localPath, Boolean(options.force));
  context.output.success(`Downloaded ${remotePath} to ${localPath}`, { remote: remotePath, local: localPath });
}

async function saveResponse(response: Response, localPath: string, force: boolean): Promise<void> {
  if (!force) {
    await access(localPath).then(() => {
      throw new ConfigError(`Local path already exists: ${localPath} (use --force to overwrite)`);
    }).catch((error: unknown) => {
      if (!(error instanceof Error && 'code' in error && error.code === 'ENOENT')) throw error;
    });
  }
  const temporary = `${localPath}.airyfs-${process.pid}.tmp`;
  try {
    await pipeResponse(response, createWriteStream(temporary, { flags: 'wx' }));
    if (force) await renameLocal(temporary, localPath);
    else {
      await link(temporary, localPath);
      await removeLocal(temporary, { force: true });
    }
  } catch (error) {
    await removeLocal(temporary, { force: true });
    throw error;
  }
}

async function uploadTree(
  context: CommandContext,
  localDir: string,
  remotePath: string,
  options: { replace?: boolean },
): Promise<void> {
  const body = createLocalTreeStream(localDir) as unknown as NonNullable<RequestInit['body']>;
  const summary = await context.client().importTree(remotePath, body, Boolean(options.replace));
  context.output.success(
    `Pushed ${localDir} to ${remotePath} (${summarize(summary)})`,
    { local: localDir, remote: remotePath, ...summary },
  );
}

async function downloadTree(
  context: CommandContext,
  remotePath: string,
  localPath: string,
  options: { force?: boolean },
): Promise<void> {
  const exists = await localExists(localPath);
  if (exists && !options.force) {
    throw new ConfigError(`Local path already exists: ${localPath} (use --force to overwrite)`);
  }
  const response = await context.client().exportTree(remotePath);
  if (!response.body) throw new ConfigError('Server returned an empty archive');

  const temporary = `${localPath}.airyfs-${process.pid}-${crypto.randomUUID()}.tmp`;
  let summary;
  try {
    await mkdirLocal(temporary, { recursive: true });
    summary = await extractLocalTree(response.body as ReadableStream<Uint8Array>, temporary);
  } catch (error) {
    await removeLocal(temporary, { recursive: true, force: true });
    throw error;
  }

  const backup = `${localPath}.airyfs-backup-${process.pid}-${crypto.randomUUID()}`;
  try {
    if (exists) await renameLocal(localPath, backup);
    try {
      await renameLocal(temporary, localPath);
    } catch (error) {
      if (exists) await renameLocal(backup, localPath).catch(() => undefined);
      throw error;
    }
  } catch (error) {
    await removeLocal(temporary, { recursive: true, force: true });
    throw error;
  }
  if (exists) await removeLocal(backup, { recursive: true, force: true });

  context.output.success(
    `Pulled ${remotePath} to ${localPath} (${summarize(summary)})`,
    { remote: remotePath, local: localPath, ...summary },
  );
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
    .option('--no-stream', 'disable live streaming and buffer the full result (text mode)')
    .option('--pty', 'run interactively in a remote pseudo-terminal')
    .option('--timeout <duration>', 'maximum startup and busy-wait time', '90s')
    .description('Execute a command in the volume Container')
    .action(async (parts: string[], options, command) => perform(runtime, command, async (context) => {
      const commandText = commandForExec(parts);
      const remoteDirectory = context.cwd === '/' ? '/volume' : `/volume${context.cwd}`;
      const fullCommand = `cd -- ${shellQuote(remoteDirectory)} && ${commandText}`;
      const timeout = options.wait ? parseDuration(options.timeout) : 0;
      const deadline = Date.now() + timeout;
      // Live streaming is the text-mode default; --json stays buffered so machine
      // output remains a single ExecResult object, and --no-stream opts out.
      const streaming = options.stream !== false && !context.output.json;
      const spinner = createSpinner(context, `Running in ${context.volume}:${context.cwd}`);
      spinner?.start();
      try {
        // Resolve startup failures with a retry-safe no-op before submitting the
        // user command, whose outcome can be ambiguous after a transport error.
        await execWithRetry(context, ':', deadline, true);
        if (options.pty) {
          spinner?.stop();
          await runPtyExec(context, runtime, fullCommand);
          return;
        }
        if (streaming) {
          await runStreamingExec(context, runtime, fullCommand, spinner);
          return;
        }
        const result = await execWithRetry(context, fullCommand, deadline, false);
        spinner?.stop();
        if (context.output.json) {
          context.output.value(result);
        } else {
          context.output.text(result.stdout);
          if (result.stderr) context.output.stderr.write(result.stderr);
        }
        runtime.exitCode = result.exitCode;
      } catch (error) {
        spinner?.stop();
        throw error;
      }
    }));
}

async function runPtyExec(context: CommandContext, runtime: Runtime, fullCommand: string): Promise<void> {
  if (context.shellMode) throw new ConfigError('`exec --pty` is unavailable inside `airyfs shell`');
  if (context.output.json || context.output.quiet) throw new ConfigError('`exec --pty` cannot be combined with --json or --quiet');
  const input = context.stdin as NodeJS.ReadStream;
  const output = context.output.stdout as NodeJS.WriteStream;
  if (!input.isTTY || !output.isTTY || typeof input.setRawMode !== 'function') {
    throw new ConfigError('`exec --pty` requires an interactive terminal');
  }

  const session = await context.client().openPty();
  const resize = (): void => session.resize(output.columns || 80, output.rows || 24);
  const onInput = (chunk: Buffer | string): void => session.write(
    typeof chunk === 'string' ? Buffer.from(chunk) : new Uint8Array(chunk.buffer, chunk.byteOffset, chunk.byteLength),
  );
  const removeData = session.onData((data) => output.write(data));
  input.setRawMode(true);
  input.resume();
  input.on('data', onInput);
  process.on('SIGWINCH', resize);
  try {
    resize();
    session.write(new TextEncoder().encode(`${fullCommand}\r`));
    runtime.exitCode = (await session.closed).exitCode;
  } finally {
    input.removeListener('data', onInput);
    process.removeListener('SIGWINCH', resize);
    removeData();
    input.setRawMode(false);
    session.close();
  }
}

/**
 * Stream a command's output live, decoding base64 stdout/stderr to their raw
 * streams and adopting the remote exit code. On SIGINT, once the start id is
 * known, cancel the command once and abort the local stream; the SIGINT listener
 * is always removed afterward so neither one-shot nor shell mode leaks it.
 */
async function runStreamingExec(
  context: CommandContext,
  runtime: Runtime,
  fullCommand: string,
  spinner: ReturnType<typeof ora> | null,
): Promise<void> {
  const controller = new AbortController();
  let startId: string | undefined;
  let interrupted = false;
  let exitCode = 0;
  let sawExit = false;

  const onInterrupt = (): void => {
    interrupted = true;
    // Only an admitted command has an id to cancel; always abort the local read.
    if (startId) void context.client().cancelExec(startId).catch(() => undefined);
    controller.abort();
  };
  process.once('SIGINT', onInterrupt);

  try {
    const events = await context.client().execStream(fullCommand, controller.signal);
    for await (const event of events) {
      if (event.type === 'start') {
        startId = event.id;
        spinner?.stop();
      } else if (event.type === 'stdout') {
        context.output.stdout.write(Buffer.from(event.data, 'base64'));
      } else if (event.type === 'stderr') {
        context.output.stderr.write(Buffer.from(event.data, 'base64'));
      } else if (event.type === 'exit') {
        exitCode = event.exitCode;
        sawExit = true;
      }
    }
  } catch (error) {
    if (!interrupted && !controller.signal.aborted) throw error;
    // An interrupted stream is expected to reject; the exit code reflects it below.
  } finally {
    process.removeListener('SIGINT', onInterrupt);
    spinner?.stop();
  }

  // 130 = 128 + SIGINT, matching how a shell reports an interrupted foreground job.
  runtime.exitCode = interrupted && !sawExit ? 130 : exitCode;
}

const TERMINAL_JOB_STATUSES: readonly JobStatus[] = ['succeeded', 'failed', 'canceled'];

function isTerminalJob(status: JobStatus): boolean {
  return TERMINAL_JOB_STATUSES.includes(status);
}

/** Poll interval for --wait/--follow; overridable via env so tests stay fast. */
function jobPollIntervalMs(): number {
  const raw = process.env.AIRYFS_JOB_POLL_MS;
  if (raw === undefined) return 500;
  const parsed = Number(raw);
  return Number.isFinite(parsed) && parsed >= 0 ? parsed : 500;
}

function sleep(ms: number, signal?: AbortSignal): Promise<void> {
  return new Promise((resolve) => {
    if (ms <= 0 || signal?.aborted) {
      resolve();
      return;
    }
    const cleanup = (): void => {
      clearTimeout(timer);
      signal?.removeEventListener('abort', onAbort);
    };
    const onAbort = (): void => {
      cleanup();
      resolve();
    };
    const timer = setTimeout(() => {
      cleanup();
      resolve();
    }, ms);
    signal?.addEventListener('abort', onAbort, { once: true });
  });
}

/** Write one persisted log entry's exact bytes to the matching output stream. */
function writeJobLogEntry(context: CommandContext, entry: JobLogEntry): void {
  const bytes = Buffer.from(entry.data, 'base64');
  if (entry.stream === 'stderr') context.output.stderr.write(bytes);
  else context.output.stdout.write(bytes);
}

/**
 * Drain every available log page from `after`, invoking `onEntry` in seq order.
 * Returns the new cursor (the last seq seen, or the incoming cursor if none).
 */
async function drainJobLogs(
  context: CommandContext,
  id: string,
  after: number | undefined,
  onEntry: (entry: JobLogEntry) => void,
): Promise<number | undefined> {
  let cursor = after;
  while (true) {
    const page = await context.client().getJobLogs(id, cursor);
    for (const entry of page.entries) {
      onEntry(entry);
      cursor = entry.seq;
    }
    if (page.next === null) return cursor;
  }
}

/**
 * Poll a job until it reaches a terminal state, draining logs each pass. A local
 * SIGINT stops polling without canceling the remote job; its listener is always
 * removed. Returns the final job snapshot and whether it was interrupted.
 */
async function pollJobUntilTerminal(
  context: CommandContext,
  id: string,
  onEntry: (entry: JobLogEntry) => void,
  after: number | undefined = undefined,
): Promise<{ job: Job; interrupted: boolean }> {
  const controller = new AbortController();
  let interrupted = false;
  const onInterrupt = (): void => {
    interrupted = true;
    controller.abort();
  };
  process.once('SIGINT', onInterrupt);

  let cursor = after;
  let job: Job;
  try {
    while (true) {
      cursor = await drainJobLogs(context, id, cursor, onEntry);
      job = await context.client().getJob(id);
      if (isTerminalJob(job.status)) {
        cursor = await drainJobLogs(context, id, cursor, onEntry);
        break;
      }
      if (controller.signal.aborted) break;
      await sleep(jobPollIntervalMs(), controller.signal);
      if (controller.signal.aborted) break;
    }
  } finally {
    process.removeListener('SIGINT', onInterrupt);
  }
  return { job, interrupted };
}

function registerJobCommands(program: Command, runtime: Runtime): void {
  const job = program.command('job').alias('jobs').description('Submit and manage durable background jobs');

  job.command('submit')
    .argument('<command...>')
    .allowUnknownOption(true)
    .passThroughOptions()
    .option('--cwd <remote>', 'remote working directory (defaults to the session cwd)')
    .option('--idempotency-key <key>', 'idempotency key for safe retries (a UUID is generated by default)')
    .option('--wait', 'wait for the job to finish, streaming persisted output, and adopt its exit code')
    .description('Submit a command to run as a durable background job')
    .action(async (parts: string[], options, command) => perform(runtime, command, async (context) => {
      const commandText = commandForExec(parts);
      const cwd = options.cwd ? context.path(options.cwd) : context.cwd;
      const submitted = await context.client().submitJob(commandText, cwd, options.idempotencyKey);

      if (!options.wait) {
        if (context.output.json) context.output.value(submitted);
        else context.output.value(submitted.id);
        return;
      }

      const onEntry = context.output.json
        ? (): void => undefined
        : (entry: JobLogEntry): void => writeJobLogEntry(context, entry);
      const { job: finished, interrupted } = await pollJobUntilTerminal(context, submitted.id, onEntry);

      if (interrupted && !isTerminalJob(finished.status)) {
        // 130 = 128 + SIGINT: the local wait was interrupted; the job keeps running.
        runtime.exitCode = 130;
        if (context.output.json) context.output.value(finished);
        return;
      }
      if (context.output.json) context.output.value(finished);
      runtime.exitCode = finished.exitCode ?? (finished.status === 'succeeded' ? 0 : 1);
    }));

  job.command('list')
    .alias('ls')
    .option('--status <status>', 'filter by queued, running, succeeded, failed, or canceled')
    .description('List durable jobs')
    .action(async (options, command) => perform(runtime, command, async (context) => {
      const jobs = await context.client().listJobs(options.status as JobStatus | undefined);
      if (context.output.json) {
        context.output.value(jobs);
        return;
      }
      context.output.table(
        ['Id', 'Status', 'Exit', 'Created', 'Command'],
        jobs.map((entry) => [
          entry.id,
          entry.status,
          entry.exitCode === null ? '-' : entry.exitCode,
          formatTime(entry.createdAt),
          truncateCommand(entry.command),
        ]),
      );
    }));

  job.command('status')
    .argument('<id>')
    .description('Show a job\'s current state')
    .action(async (id, _options, command) => perform(runtime, command, async (context) => {
      context.output.value(await context.client().getJob(id));
    }));

  job.command('logs')
    .argument('<id>')
    .option('--follow', 'poll for new output until the job reaches a terminal state')
    .option('--after <seq>', 'only show log entries after this seq cursor')
    .description('Print a job\'s persisted stdout/stderr')
    .action(async (id, options, command) => perform(runtime, command, async (context) => {
      const after = options.after === undefined ? undefined : parseCursor(options.after);

      if (context.output.json) {
        const entries: JobLogEntry[] = [];
        if (options.follow) {
          await pollJobUntilTerminal(context, id, (entry) => entries.push(entry), after);
        } else {
          await drainJobLogs(context, id, after, (entry) => entries.push(entry));
        }
        const next = entries.length > 0 ? entries[entries.length - 1].seq : null;
        context.output.value({ entries, next });
        return;
      }

      const onEntry = (entry: JobLogEntry): void => writeJobLogEntry(context, entry);
      if (options.follow) await pollJobUntilTerminal(context, id, onEntry, after);
      else await drainJobLogs(context, id, after, onEntry);
    }));

  job.command('cancel')
    .argument('<id>')
    .description('Request cancellation of a job')
    .action(async (id, _options, command) => perform(runtime, command, async (context) => {
      const canceled = await context.client().cancelJob(id);
      context.output.success(`Requested cancellation of job ${id}`, canceled);
    }));
}

function truncateCommand(command: string): string {
  const singleLine = command.replace(/\s+/g, ' ').trim();
  return singleLine.length > 60 ? `${singleLine.slice(0, 57)}...` : singleLine;
}

function parseCursor(value: string, option = '--after'): number {
  const parsed = Number(value);
  if (!Number.isSafeInteger(parsed) || parsed < 0) {
    throw new ConfigError(`Invalid ${option} cursor: ${value}`);
  }
  return parsed;
}

function registerScheduleCommands(program: Command, runtime: Runtime): void {
  const schedule = program.command('schedule').description('Run durable jobs on UTC cron schedules');

  schedule.command('create')
    .argument('<name>')
    .argument('<cron>', 'five-field UTC cron or @hourly/@daily/@weekly/@monthly/@yearly')
    .argument('<command...>')
    .allowUnknownOption(true)
    .option('--cwd <remote>', 'remote working directory (defaults to the session cwd)')
    .description('Create an enabled job schedule')
    .action(async (name, cron, parts: string[], options, command) => perform(runtime, command, async (context) => {
      const created = await context.client().createSchedule({
        name,
        cron,
        command: commandForExec(parts),
        cwd: options.cwd ? context.path(options.cwd) : context.cwd,
      });
      context.output.success(`Created schedule ${created.name}`, created);
    }));

  schedule.command('list')
    .alias('ls')
    .description('List job schedules')
    .action(async (_options, command) => perform(runtime, command, async (context) => {
      const schedules = await context.client().listSchedules();
      if (context.output.json) {
        context.output.value(schedules);
        return;
      }
      context.output.table(
        ['ID', 'Name', 'Cron (UTC)', 'Enabled', 'Next', 'Command'],
        schedules.map((entry) => [
          entry.id, entry.name, entry.cron, entry.enabled ? 'yes' : 'no',
          entry.nextRun === null ? '-' : formatTime(entry.nextRun), truncateCommand(entry.command),
        ]),
      );
    }));

  for (const [action, enabled] of [['enable', true], ['disable', false]] as const) {
    schedule.command(action)
      .argument('<id>')
      .description(`${action[0].toUpperCase()}${action.slice(1)} a job schedule`)
      .action(async (id, _options, command) => perform(runtime, command, async (context) => {
        const updated = await context.client().setScheduleEnabled(id, enabled);
        context.output.success(`${action === 'enable' ? 'Enabled' : 'Disabled'} schedule ${updated.name}`, updated);
      }));
  }

  schedule.command('delete')
    .alias('rm')
    .argument('<id>')
    .description('Delete a job schedule')
    .action(async (id, _options, command) => perform(runtime, command, async (context) => {
      const result = await context.client().deleteSchedule(id);
      context.output.success(result.removed ? `Deleted schedule ${id}` : `Schedule ${id} was not found`, result);
    }));
}

function registerWatchCommand(program: Command, runtime: Runtime): void {
  program.command('watch')
    .argument('[path]', 'remote path prefix', '.')
    .option('--since <seq>', 'replay changes after this sequence cursor')
    .option('--limit <count>', 'maximum events returned per poll', '100')
    .option('--once', 'drain currently retained changes and exit')
    .description('Watch filesystem changes from API and Container/FUSE writers')
    .action(async (path, options, command) => perform(runtime, command, async (context) => {
      const target = context.path(path);
      const limit = parseChangeLimit(options.limit);
      const requestedSince = options.since === undefined
        ? undefined
        : parseCursor(options.since, '--since');

      if (options.once) {
        const page = await drainChanges(context, target, requestedSince ?? 0, limit);
        if (context.output.json) context.output.value(page);
        else for (const event of page.events) writeChangeEvent(context, event);
        reportChangeGap(context, page);
        return;
      }

      const controller = new AbortController();
      let interrupted = false;
      const onInterrupt = (): void => {
        interrupted = true;
        controller.abort();
      };
      process.once('SIGINT', onInterrupt);

      try {
        let cursor: number;
        if (requestedSince === undefined) {
          cursor = (await context.client().getChanges({ path: target, signal: controller.signal })).cursor;
        } else {
          cursor = requestedSince;
        }

        while (!controller.signal.aborted) {
          const page = await context.client().getChanges({
            since: cursor,
            limit,
            path: target,
            wait: 25_000,
            signal: controller.signal,
          });
          reportChangeGap(context, page);
          for (const event of page.events) writeChangeEvent(context, event, true);
          cursor = page.cursor;
        }
      } catch (error) {
        if (!interrupted && !controller.signal.aborted) throw error;
      } finally {
        process.removeListener('SIGINT', onInterrupt);
      }
      if (interrupted) runtime.exitCode = 130;
    }));
}

async function drainChanges(
  context: CommandContext,
  path: string,
  since: number,
  limit: number,
): Promise<ChangePage> {
  const events: ChangeEvent[] = [];
  let cursor = since;
  let latest = since;
  let oldest = since + 1;
  let gap = false;
  let targetLatest: number | undefined;

  do {
    const previousCursor = cursor;
    const page = await context.client().getChanges({ since: cursor, limit, path });
    events.push(...page.events);
    cursor = page.cursor;
    latest = page.latest;
    oldest = page.oldest;
    gap ||= page.gap;
    targetLatest ??= page.latest;
    if (cursor <= previousCursor && cursor < targetLatest) {
      throw new ConfigError(`Change feed did not advance beyond sequence ${previousCursor}`);
    }
  } while (cursor < targetLatest);

  return { events, cursor, latest, oldest, gap };
}

function writeChangeEvent(context: CommandContext, event: ChangeEvent, streamingJson = false): void {
  if (context.output.quiet) return;
  if (context.output.json) {
    if (streamingJson) context.output.stdout.write(`${JSON.stringify(event)}\n`);
    else context.output.value(event);
    return;
  }
  const marker: Record<ChangeEvent['type'], string> = {
    create: 'A',
    modify: 'M',
    remove: 'D',
    rename: 'R',
  };
  const detail = event.type === 'rename'
    ? `${event.oldPath ?? '?'} -> ${event.path}`
    : event.path;
  context.output.text(`${marker[event.type]} ${detail}\n`);
}

function reportChangeGap(context: CommandContext, page: ChangePage): void {
  if (!page.gap || context.output.quiet || context.output.json) return;
  context.output.stderr.write(
    `Warning: change history before sequence ${page.oldest} is no longer retained; resync may be required.\n`,
  );
}

function registerWebhookCommands(program: Command, runtime: Runtime): void {
  const webhook = program.command('webhook').description('Deliver signed filesystem change events');

  webhook.command('create')
    .argument('<url>', 'HTTPS delivery endpoint')
    .option('-p, --path <prefix>', 'remote path prefix', '/')
    .option('-e, --event <type>', 'create, modify, remove, or rename (repeatable)', collectOption, [])
    .description('Create a durable webhook subscription')
    .action(async (url, options, command) => perform(runtime, command, async (context) => {
      const events = normalizeWebhookEvents(options.event);
      const created = await context.client().createWebhook({
        url,
        pathPrefix: context.path(options.path),
        events: events.length > 0 ? events : undefined,
      });
      if (context.output.json) {
        context.output.value(created);
        return;
      }
      context.output.success(`Created webhook ${created.id}`, {
        id: created.id, url: created.url, pathPrefix: created.pathPrefix, events: created.events,
      });
      context.output.value(created.secret);
      context.output.stderr.write(context.output.dim('The signing secret is shown once. Store it securely.\n'));
    }));

  webhook.command('list')
    .alias('ls')
    .description('List webhook subscriptions without signing secrets')
    .action(async (_options, command) => perform(runtime, command, async (context) => {
      const webhooks = await context.client().listWebhooks();
      if (context.output.json) {
        context.output.value(webhooks);
        return;
      }
      context.output.table(
        ['ID', 'URL', 'Path', 'Events'],
        webhooks.map((entry) => [entry.id, entry.url, entry.pathPrefix, entry.events.join(',')]),
      );
    }));

  webhook.command('delete')
    .alias('rm')
    .argument('<id>')
    .description('Delete a webhook and its pending deliveries')
    .action(async (id, _options, command) => perform(runtime, command, async (context) => {
      const result = await context.client().deleteWebhook(id);
      context.output.success(result.removed ? `Deleted webhook ${id}` : `Webhook ${id} was not found`, result);
    }));
}

function parseChangeLimit(value: string): number {
  const parsed = Number(value);
  if (!Number.isSafeInteger(parsed) || parsed < 1 || parsed > 1000) {
    throw new ConfigError(`Invalid --limit count: ${value} (expected 1 to 1000)`);
  }
  return parsed;
}

function registerVolumeCommands(program: Command, runtime: Runtime): void {
  const volume = program.command('volume').description('Manage the selected volume');
  volume.command('list')
    .description('List registered volumes; requires root access when authentication is enabled')
    .action(async (_options, command) => perform(runtime, command, async (context) => {
      const records = await context.client().listVolumes();
      if (context.output.json) context.output.value(records);
      else context.output.table(['Name', 'Chunk size', 'Created'], records.map((record) => [
        record.name,
        formatSize(record.chunkSize),
        formatTime(record.createdAt),
      ]));
    }));

  volume.command('create')
    .option('--chunk-size <size>', 'immutable chunk size (4k to 1m)', '256k')
    .option('-p, --password [password]', 'set a volume password and log the session in with a scoped token')
    .description('Create or configure the selected volume')
    .action(async (options, command) => perform(runtime, command, async (context) => {
      const result = await context.client().createVolume(parseSize(options.chunkSize));
      if (options.password === undefined) {
        context.output.success(`Volume ${context.volume} uses ${formatSize(result.chunkSize)} chunks`, result);
        return;
      }
      if (context.shellMode && options.password === true) {
        throw new ConfigError('Provide the password as an argument inside `airyfs shell`: volume create --password <pw>');
      }
      const password = await requiredInput(
        runtime,
        typeof options.password === 'string' ? options.password : undefined,
        'Volume password: ',
        'password',
      );
      if (password.length < 8) throw new ConfigError('Password must be at least 8 characters');
      // Set the password (needs the current root/admin token), then downgrade the
      // session to a scoped password token so day-to-day use is least-privilege.
      await context.client().setVolumePassword(password);
      const minted = await context.client().loginWithPassword(password);
      await context.sessions.setToken(context.named.name, minted.token);
      context.output.success(
        `Volume ${context.volume} uses ${formatSize(result.chunkSize)} chunks; password set and session logged in`,
        { ...result, passwordSet: true, capability: minted.id, expires: minted.expires },
      );
    }));

  volume.command('info')
    .description('Show selected-volume configuration')
    .action(async (_options, command) => perform(runtime, command, async (context) => {
      context.output.value(await context.client().getVolume());
    }));

  volume.command('fork')
    .argument('<target-volume>')
    .description('Create an independent point-in-time copy in an empty target volume')
    .action(async (targetVolume, _options, command) => perform(runtime, command, async (context) => {
      const summary = await context.client().forkVolume(targetVolume);
      context.output.success(
        `Forked volume ${context.volume} to ${targetVolume}`,
        { sourceVolume: context.volume, targetVolume, ...summary },
      );
    }));

  volume.command('quota')
    .option('--bytes <size>', 'logical byte limit, or unlimited')
    .option('--inodes <count>', 'inode limit, or unlimited')
    .description('Show or configure persistent volume quotas')
    .action(async (options, command) => perform(runtime, command, async (context) => {
      const hasBytes = options.bytes !== undefined;
      const hasInodes = options.inodes !== undefined;
      const quota = hasBytes || hasInodes
        ? await context.client().setQuota({
          ...(hasBytes ? { bytes: parseQuotaBytes(options.bytes) } : {}),
          ...(hasInodes ? { inodes: parseQuotaInodes(options.inodes) } : {}),
        })
        : await context.client().quota();
      if (context.output.json) {
        context.output.value(quota);
      } else {
        context.output.table(['Resource', 'Limit'], [
          ['Logical bytes', quota.bytes === null ? 'unlimited' : formatSize(quota.bytes)],
          ['Inodes', quota.inodes === null ? 'unlimited' : quota.inodes],
        ]);
      }
    }));
}

function parseQuotaBytes(value: string): number | null {
  return value.toLowerCase() === 'unlimited' ? null : parseSize(value);
}

function parseQuotaInodes(value: string): number | null {
  if (value.toLowerCase() === 'unlimited') return null;
  const parsed = Number(value);
  if (!Number.isSafeInteger(parsed) || parsed < 1) throw new ConfigError('Inode quota must be a positive integer or unlimited');
  return parsed;
}

function registerSnapshotCommands(program: Command, runtime: Runtime): void {
  const snapshot = program.command('snapshot').alias('snap').description('Capture and manage full-volume snapshots');

  snapshot.command('create')
    .argument('[name]', 'snapshot name (a timestamped default is generated when omitted)')
    .option('-n, --note <note>', 'attach a free-form note')
    .description('Capture a full-volume snapshot')
    .action(async (name, options, command) => perform(runtime, command, async (context) => {
      const info = await context.client().createSnapshot(name, options.note);
      context.output.success(`Created snapshot ${info.name}`, info);
    }));

  snapshot.command('list')
    .alias('ls')
    .description('List snapshots')
    .action(async (_options, command) => perform(runtime, command, async (context) => {
      const snapshots = await context.client().listSnapshots();
      if (context.output.json) {
        context.output.value(snapshots);
        return;
      }
      context.output.table(
        ['Name', 'Created', 'Files', 'Size', 'Note'],
        snapshots.map((info) => [
          info.name,
          formatTime(info.createdAt),
          info.fileCount,
          formatSize(info.byteCount),
          info.note ?? '-',
        ]),
      );
    }));

  snapshot.command('diff')
    .argument('<id>', 'snapshot id or name')
    .argument('[against]', 'compare against "live" or another snapshot id/name', 'live')
    .description('Diff a snapshot against the live volume or another snapshot')
    .action(async (id, against, _options, command) => perform(runtime, command, async (context) => {
      const entries = await context.client().diffSnapshot(id, against);
      if (context.output.json) {
        context.output.value(entries);
        return;
      }
      if (entries.length === 0) {
        context.output.value('No differences');
        return;
      }
      const marker: Record<string, string> = { added: 'A', removed: 'D', modified: 'M' };
      context.output.text(`${entries.map((entry) => `${marker[entry.change]} ${entry.path}`).join('\n')}\n`);
    }));

  snapshot.command('restore')
    .argument('<id>', 'snapshot id or name')
    .option('-f, --force', 'skip confirmation')
    .description('Restore a snapshot over the live volume; recycles the Container')
    .action(async (id, options, command) => perform(runtime, command, async (context) => {
      if (context.shellMode && !options.force) {
        throw new ConfigError('Interactive confirmation is unavailable inside `airyfs shell`; use `restore --force`');
      }
      if (!options.force && !await confirmAction(
        context,
        `Restore snapshot ${id} over ${context.volume}? Current volume contents will be replaced.`,
      )) {
        context.output.value('Cancelled');
        return;
      }
      const info = await context.client().restoreSnapshot(id);
      context.output.success(`Restored ${context.volume} from snapshot ${info.name}`, info);
    }));

  snapshot.command('clone')
    .argument('<id>', 'snapshot id or name')
    .requiredOption('--to <volume>', 'target volume to clone into (must differ from the source)')
    .description('Clone a snapshot into another volume; requires root or auth-disabled access')
    .action(async (id, options, command) => perform(runtime, command, async (context) => {
      const summary = await context.client().cloneSnapshot(id, options.to);
      context.output.success(
        `Cloned snapshot ${id} to volume ${options.to} (${summarize(summary)})`,
        { snapshot: id, targetVolume: options.to, ...summary },
      );
    }));

  snapshot.command('delete')
    .alias('rm')
    .argument('<id>', 'snapshot id or name')
    .option('-f, --force', 'skip confirmation')
    .description('Delete a snapshot')
    .action(async (id, options, command) => perform(runtime, command, async (context) => {
      if (context.shellMode && !options.force) {
        throw new ConfigError('Interactive confirmation is unavailable inside `airyfs shell`; use `delete --force`');
      }
      if (!options.force && !await confirmAction(context, `Delete snapshot ${id}?`)) {
        context.output.value('Cancelled');
        return;
      }
      const info = await context.client().deleteSnapshot(id);
      context.output.success(`Deleted snapshot ${info.name}`, info);
    }));
}

function registerAuthCommands(program: Command, runtime: Runtime): void {
  const auth = program.command('auth').description('Manage session authentication');

  auth.command('login')
    .argument('[token]', 'bearer token (root secret or capability)')
    .option('-p, --password [password]', 'log in with the volume password instead of a token')
    .option('--expires <duration>', 'token lifetime for password login, such as 24h')
    .description('Authenticate the active session and store its token')
    .action(async (token, options, command) => perform(runtime, command, async (context) => {
      if (options.password !== undefined) {
        if (context.shellMode && options.password === true) {
          throw new ConfigError('Provide the password as an argument inside `airyfs shell`: auth login --password <pw>');
        }
        const password = await requiredInput(
          runtime,
          typeof options.password === 'string' ? options.password : undefined,
          'Volume password: ',
          'password',
        );
        const expiresInSeconds = options.expires ? durationSeconds(options.expires) : undefined;
        const minted = await context.client().loginWithPassword(password, expiresInSeconds);
        await context.sessions.setToken(context.named.name, minted.token);
        context.output.success(`Logged in to ${context.volume} with a password-scoped token`, {
          session: context.named.name,
          volume: context.volume,
          capability: minted.id,
          expires: minted.expires,
        });
        return;
      }
      const candidate = await requiredInput(runtime, token, 'Token: ', 'token');
      const probe = new AiryFSClient(context.endpoint, context.volume, undefined, candidate);
      const status = await probe.authStatus();
      await context.sessions.setToken(context.named.name, candidate);
      context.output.success(`Logged in to ${context.volume} as ${status.auth}`, {
        session: context.named.name,
        volume: context.volume,
        auth: status.auth,
        capability: status.capability,
      });
    }));

  auth.command('passwd')
    .argument('[password]', 'new volume password (min 8 characters)')
    .option('--current <password>', 'current password, when rotating without a root/admin token')
    .description('Set or rotate the volume password (root, admin, or current password required)')
    .action(async (password, options, command) => perform(runtime, command, async (context) => {
      if (context.shellMode && password === undefined) {
        throw new ConfigError('Provide the new password as an argument inside `airyfs shell`: auth passwd <pw>');
      }
      const next = await requiredInput(runtime, password, 'New volume password: ', 'password');
      if (next.length < 8) throw new ConfigError('Password must be at least 8 characters');
      const status = await context.client().setVolumePassword(next, options.current);
      context.output.success(`Set the volume password for ${status.volume}`, status);
    }));

  auth.command('logout')
    .description('Remove the stored token from the active session')
    .action(async (_options, command) => perform(runtime, command, async (context) => {
      await context.sessions.clearToken(context.named.name);
      context.output.success(`Logged out of ${context.named.name}`, { session: context.named.name });
    }));

  auth.command('status')
    .description('Show the authentication state of the active session')
    .action(async (_options, command) => perform(runtime, command, async (context) => {
      const status = await context.client().authStatus();
      const localToken = Boolean(context.named.session.token);
      if (context.output.json) {
        context.output.value({ session: context.named.name, localToken, ...status });
        return;
      }
      const rows: Array<[string, string]> = [
        ['Session', context.named.name],
        ['Volume', status.volume],
        ['Auth', status.auth],
        ['Local token', localToken ? 'configured' : 'none'],
      ];
      if (status.capability) {
        rows.push(['Capability', status.capability.id]);
        rows.push(['Operations', status.capability.operations.join(', ')]);
        rows.push(['Path prefixes', status.capability.pathPrefixes.join(', ') || '(all)']);
        rows.push(['Expires', formatTime(status.capability.expires)]);
      }
      context.output.table(['Field', 'Value'], rows);
    }));
}

function registerCapabilityCommands(program: Command, runtime: Runtime): void {
  const capability = program.command('capability').description('Mint and revoke scoped capability tokens');

  capability.command('create')
    .option('-o, --operation <operation>', 'grant read, write, exec, sql, or admin (repeatable)', collectOption, [])
    .option('-p, --path <prefix>', 'restrict to a remote path prefix (repeatable)', collectOption, [])
    .option('--expires <duration>', 'validity duration such as 1h or 30m', '1h')
    .description('Mint a capability token; requires root or admin access')
    .action(async (options, command) => perform(runtime, command, async (context) => {
      const minted = await context.client().createCapability({
        operations: normalizeOperations(options.operation),
        pathPrefixes: options.path,
        expiresInSeconds: durationSeconds(options.expires),
      });
      if (context.output.json) {
        context.output.value(minted);
        return;
      }
      context.output.success(`Created capability ${minted.id}`, minted);
      // The only place the CLI prints a token: the freshly minted capability, once.
      context.output.value(minted.token);
    }));

  capability.command('revoke')
    .argument('<id>')
    .description('Revoke a capability by id; requires root or admin access')
    .action(async (id, _options, command) => perform(runtime, command, async (context) => {
      await context.client().revokeCapability(id);
      context.output.success(`Revoked capability ${id}`, { id });
    }));

  program.command('browser-upload')
    .argument('<path>', 'exact destination file path')
    .option('--expires <duration>', 'upload credential lifetime such as 1h or 30m', '1h')
    .description('Mint a path-scoped browser upload credential and print its endpoint')
    .action(async (path, options, command) => perform(runtime, command, async (context) => {
      const target = context.path(path);
      const minted = await context.client().createCapability({
        operations: ['write'],
        pathPrefixes: [target],
        expiresInSeconds: durationSeconds(options.expires),
      });
      const url = `${context.endpoint}/v1/volumes/${encodeURIComponent(context.volume)}/browser-uploads/${encodeRemotePath(target)}`;
      const result = { url, token: minted.token, capability: minted.id, expires: minted.expires, path: target };
      if (context.output.json) {
        context.output.value(result);
        return;
      }
      context.output.success(`Created browser upload ${minted.id} for ${target}`);
      context.output.value(url);
      context.output.value(minted.token);
      context.output.stderr.write(context.output.dim(
        'POST the raw File body with Authorization: Bearer <token>; the token is not part of the URL.\n',
      ));
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

  program.command('usage-history')
    .description('Show recent per-volume usage samples')
    .option('--before <timestamp>', 'show samples before this Unix timestamp')
    .option('--limit <count>', 'maximum samples to return', '288')
    .action(async (options, command) => perform(runtime, command, async (context) => {
      const before = options.before === undefined ? undefined : parseCursor(options.before, '--before');
      const limit = parseChangeLimit(options.limit);
      const page = await context.client().usageHistory({ before, limit });
      if (context.output.json) context.output.value(page);
      else context.output.table(['Sampled', 'Files', 'Filesystem', 'SQLite', 'Byte quota', 'Inode quota'],
        page.samples.map((sample) => [
          new Date(sample.sampledAt * 1000).toLocaleString(),
          sample.inodes,
          formatSize(sample.bytesUsed),
          formatSize(sample.sqliteBytes),
          sample.quotaBytes === null ? '-' : formatSize(sample.quotaBytes),
          sample.quotaInodes ?? '-',
        ]));
      if (!context.output.json && page.next !== null) context.output.text(`Next cursor: ${page.next}\n`);
    }));

  program.command('perf')
    .description('Show Hrana request and SQL statement counters')
    .action(async (_options, command) => perform(runtime, command, async (context) => {
      context.output.value(await context.client().perf());
    }));

  program.command('metrics')
    .description('Print per-volume Prometheus metrics')
    .action(async (_options, command) => perform(runtime, command, async (context) => {
      context.output.text(await context.client().metrics());
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

// ---------------------------------------------------------------------------
// Web hosting (sites and shares)
// ---------------------------------------------------------------------------

/** Build the public URL for a volume's site root. */
function siteUrl(endpoint: string, volume: string): string {
  return `${endpoint}/s/${encodeURIComponent(volume)}/`;
}

/** Build the public URL for a share link. */
function shareUrl(endpoint: string, volume: string, id: string): string {
  return `${endpoint}/d/${encodeURIComponent(volume)}/${encodeURIComponent(id)}`;
}

function registerSiteCommands(program: Command, runtime: Runtime): void {
  const site = program.command('site').description('Publish the volume as a static website');

  site.command('publish')
    .argument('[path]', 'volume subtree to serve as the web root', '/')
    .option('--index <file>', 'directory index document', 'index.html')
    .option('--spa', 'serve the index document for unmatched routes (single-page apps)')
    .option('--listing', 'generate a directory index when no index document exists')
    .option('--cache <value>', 'Cache-Control header for served files')
    .description('Publish (or update) the volume web root for public serving')
    .action(async (path, options, command) => perform(runtime, command, async (context) => {
      const info = await context.client().publishSite({
        path: context.path(path),
        indexDocument: options.index,
        spa: Boolean(options.spa),
        directoryListing: Boolean(options.listing),
        cacheControl: options.cache,
      });
      context.output.success(
        `Published ${context.volume} at ${siteUrl(context.endpoint, context.volume)}`,
        { ...info, url: siteUrl(context.endpoint, context.volume) },
      );
    }));

  site.command('status')
    .description('Show the published-site status')
    .action(async (_options, command) => perform(runtime, command, async (context) => {
      const status = await context.client().getSite();
      if (context.output.json) {
        context.output.value({ ...status, url: status.published ? siteUrl(context.endpoint, context.volume) : null });
        return;
      }
      if (!status.published || !status.site) {
        context.output.value('No site published');
        return;
      }
      context.output.table(['Field', 'Value'], [
        ['URL', siteUrl(context.endpoint, context.volume)],
        ['Root', status.site.pathPrefix],
        ['Index', status.site.indexDocument],
        ['SPA', status.site.spa ? 'yes' : 'no'],
        ['Directory listing', status.site.directoryListing ? 'yes' : 'no'],
        ['Cache-Control', status.site.cacheControl ?? '-'],
      ]);
    }));

  site.command('deploy')
    .argument('<local-directory>', 'built static-site directory')
    .option('-n, --note <note>', 'snapshot note')
    .description('Snapshot the volume and atomically replace the published web root')
    .action(async (localDir, options, command) => perform(runtime, command, async (context) => {
      const localStats = await statLocal(localDir);
      if (!localStats.isDirectory()) throw new ConfigError(`Local path is not a directory: ${localDir}`);
      const status = await context.client().getSite();
      if (!status.published || !status.site) {
        throw new ConfigError('Publish a site before deploying: airy site publish <remote-path>');
      }
      const stamp = new Date().toISOString().replace(/[:.]/g, '-');
      const snapshot = await context.client().createSnapshot(
        `site-deploy-${stamp}`,
        options.note || `Before site deploy to ${status.site.pathPrefix}`,
      );
      const body = createLocalTreeStream(localDir) as unknown as NonNullable<RequestInit['body']>;
      const summary = await context.client().importTree(status.site.pathPrefix, body, true);
      context.output.success(
        `Deployed ${localDir} to ${status.site.pathPrefix}; rollback snapshot ${snapshot.name}`,
        { root: status.site.pathPrefix, snapshot, ...summary, url: siteUrl(context.endpoint, context.volume) },
      );
    }));

  site.command('rollback')
    .argument('<snapshot>', 'snapshot id or name created before deployment')
    .description('Restore a pre-deploy full-volume snapshot')
    .action(async (snapshot, _options, command) => perform(runtime, command, async (context) => {
      const restored = await context.client().restoreSnapshot(snapshot);
      context.output.success(`Restored site from full-volume snapshot ${restored.name}`, restored);
    }));

  site.command('unpublish')
    .description('Remove the published site')
    .action(async (_options, command) => perform(runtime, command, async (context) => {
      const result = await context.client().unpublishSite();
      context.output.success(result.removed ? `Unpublished ${context.volume}` : 'No site was published', result);
    }));

  const share = program.command('share').description('Create public share links for individual files');

  share.command('create', { isDefault: true })
    .argument('<path>', 'file to share')
    .option('--expires <duration>', 'link lifetime, such as 24h or 30m')
    .option('--cache <value>', 'Cache-Control header for the shared file')
    .description('Create a public share link for a file')
    .action(async (path, options, command) => perform(runtime, command, async (context) => {
      const info = await context.client().createShare({
        path: context.path(path),
        expiresInSeconds: options.expires ? durationSeconds(options.expires) : undefined,
        cacheControl: options.cache,
      });
      const url = shareUrl(context.endpoint, context.volume, info.id);
      if (context.output.json) {
        context.output.value({ ...info, url });
        return;
      }
      context.output.success(`Shared ${info.path}`, { ...info, url });
      context.output.value(url);
    }));

  share.command('list')
    .alias('ls')
    .description('List share links')
    .action(async (_options, command) => perform(runtime, command, async (context) => {
      const shares = await context.client().listShares();
      if (context.output.json) {
        context.output.value(shares.map((info) => ({ ...info, url: shareUrl(context.endpoint, context.volume, info.id) })));
        return;
      }
      context.output.table(
        ['Id', 'Path', 'Expires', 'Created'],
        shares.map((info) => [
          info.id,
          info.path,
          info.expiresAt ? formatTime(info.expiresAt) : 'never',
          formatTime(info.createdAt),
        ]),
      );
    }));

  share.command('delete')
    .alias('rm')
    .argument('<id>')
    .description('Delete a share link')
    .action(async (id, _options, command) => perform(runtime, command, async (context) => {
      const result = await context.client().deleteShare(id);
      context.output.success(result.removed ? `Deleted share ${id}` : `No share ${id}`, result);
    }));
}

// ---------------------------------------------------------------------------
// Deployment / bootstrap
// ---------------------------------------------------------------------------

export interface ProvisionResult {
  env: string;
  url: string | null;
  secret: string;
  accountId: string;
}

/** Walk up from `start` to find the AiryFS repository root (worker + provision script). */
export function findRepoRoot(start: string): string | null {
  let dir = resolveLocal(start);
  while (true) {
    if (
      existsSync(joinLocal(dir, 'worker', 'wrangler.jsonc')) &&
      existsSync(joinLocal(dir, 'scripts', 'provision.mjs'))
    ) {
      return dir;
    }
    const parent = localDirname(dir);
    if (parent === dir) return null;
    dir = parent;
  }
}

/** Parse the JSON result line emitted by `scripts/provision.mjs --json`. */
export function parseProvisionOutput(text: string): ProvisionResult {
  for (const line of text.split('\n').reverse()) {
    const trimmed = line.trim();
    if (!trimmed.startsWith('{')) continue;
    try {
      const parsed = JSON.parse(trimmed) as { airyfsProvision?: ProvisionResult };
      if (parsed.airyfsProvision) return parsed.airyfsProvision;
    } catch {
      // Not the JSON result line; keep scanning older lines.
    }
  }
  throw new ConfigError('Could not parse the provisioning result from the deploy output');
}

/** Run the repo-local provisioner, streaming its progress and returning the parsed result. */
function runProvision(
  runtime: Runtime,
  repoRoot: string,
  env: string,
  flags: string[],
): Promise<ProvisionResult> {
  return new Promise((resolve, reject) => {
    const child = spawn(
      process.execPath,
      [joinLocal(repoRoot, 'scripts', 'provision.mjs'), env, '--json', ...flags],
      { cwd: repoRoot, stdio: ['inherit', 'pipe', 'inherit'] },
    );
    let stdout = '';
    child.stdout.on('data', (chunk: Buffer) => {
      stdout += chunk.toString();
      (runtime.stderr || process.stderr).write(chunk);
    });
    child.on('error', reject);
    child.on('close', (code) => {
      if (code !== 0) {
        reject(new ConfigError(`Deploy failed (provision.mjs exited with code ${code})`));
        return;
      }
      try {
        resolve(parseProvisionOutput(stdout));
      } catch (error) {
        reject(error);
      }
    });
  });
}

function registerDeployCommands(program: Command, runtime: Runtime): void {
  program.command('deploy')
    .argument('[env]', 'deployment environment (int or prod)', 'int')
    .option('--as <name>', 'session to create or update for the deployed endpoint', 'work')
    .option('--volume <name>', 'volume for the created session', 'default')
    .option('--allow-dirty', 'allow deploying from a dirty git tree (non-prod only)')
    .option('--allow-prod', 'confirm a production deployment')
    .description('Deploy the AiryFS Worker to Cloudflare and create a session for it')
    .action(async (env, options, command) => performConfig(runtime, command, async (output) => {
      const result = await deployAndCreateSession(runtime, output, env, options);
      output.success(
        `Deployed ${result.env} and configured session ${options.as} at ${result.url}`,
        { session: options.as, endpoint: result.url, volume: options.volume, env: result.env },
      );
    }));

  program.command('init')
    .argument('[env]', 'deployment environment (int or prod)', 'int')
    .option('--as <name>', 'session to create for the deployment', 'work')
    .option('--volume <name>', 'volume to create', 'default')
    .option('--password [password]', 'set a volume password (prompted when omitted)')
    .option('--chunk-size <size>', 'immutable chunk size (4k to 1m)', '256k')
    .option('--allow-dirty', 'allow deploying from a dirty git tree (non-prod only)')
    .option('--allow-prod', 'confirm a production deployment')
    .description('Deploy the Worker, create a session, and provision a secured volume in one step')
    .action(async (env, options, command) => performConfig(runtime, command, async (output) => {
      const result = await deployAndCreateSession(runtime, output, env, options);
      const rootClient = new AiryFSClient(result.url!, options.volume, undefined, result.secret);
      const created = await rootClient.createVolume(parseSize(options.chunkSize));

      const password = await requiredInput(
        runtime,
        typeof options.password === 'string' ? options.password : undefined,
        'Volume password: ',
        'password',
      );
      if (password.length < 8) throw new ConfigError('Password must be at least 8 characters');
      await rootClient.setVolumePassword(password);
      const minted = await rootClient.loginWithPassword(password);
      await runtime.sessions.setToken(options.as, minted.token);

      output.success(
        `Ready: session ${options.as} points at volume ${options.volume} (${formatSize(created.chunkSize)} chunks) with a password-scoped token`,
        { session: options.as, endpoint: result.url, volume: options.volume, env: result.env, capability: minted.id },
      );
    }));
}

/** Shared deploy step: provision the Worker and create/select a session holding the root secret. */
async function deployAndCreateSession(
  runtime: Runtime,
  output: Output,
  env: string,
  options: { as: string; volume: string; allowDirty?: boolean; allowProd?: boolean },
): Promise<ProvisionResult> {
  const repoRoot = findRepoRoot(process.cwd());
  if (!repoRoot) {
    throw new ConfigError('`airy deploy` must run from within the AiryFS repository (worker/ and scripts/ not found)');
  }
  const flags: string[] = [];
  if (options.allowDirty) flags.push('--allow-dirty');
  if (options.allowProd) flags.push('--allow-prod');

  const result = await runProvision(runtime, repoRoot, env, flags);
  if (!result.url) {
    throw new ConfigError('Deploy succeeded but no workers.dev URL was found in the output');
  }

  const existing = await runtime.sessions.list();
  if (existing.sessions.some((entry) => entry.name === options.as)) {
    await runtime.sessions.edit(options.as, { endpoint: result.url, volume: options.volume });
  } else {
    await runtime.sessions.create(options.as, { endpoint: result.url, volume: options.volume });
  }
  await runtime.sessions.use(options.as);
  // The deployment secret is the root credential; store it so volume/password setup works immediately.
  await runtime.sessions.setToken(options.as, result.secret);
  runtime.onSessionEvent?.({ type: 'select', name: options.as });
  output.value(output.dim(`Session ${options.as} now targets ${result.url}`));
  return result;
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

function collectOption(value: string, previous: string[]): string[] {
  return [...previous, value];
}

function normalizeOperations(values: string[]): Operation[] {
  if (values.length === 0) throw new ConfigError('Provide at least one --operation (read, write, exec, sql, admin)');
  const allowed: Operation[] = ['read', 'write', 'exec', 'sql', 'admin'];
  for (const value of values) {
    if (!(allowed as string[]).includes(value)) throw new ConfigError(`Unknown operation: ${value}`);
  }
  return [...new Set(values)] as Operation[];
}

function normalizeWebhookEvents(values: string[]): WebhookEvent[] {
  const allowed: WebhookEvent[] = ['create', 'modify', 'remove', 'rename'];
  const events: WebhookEvent[] = [];
  for (const value of values) {
    if (!allowed.includes(value as WebhookEvent)) {
      throw new ConfigError(`Unknown webhook event: ${value}`);
    }
    if (!events.includes(value as WebhookEvent)) events.push(value as WebhookEvent);
  }
  return events;
}

function durationSeconds(value: string): number {
  const seconds = Math.ceil(parseDuration(value) / 1000);
  if (seconds <= 0) throw new ConfigError(`Invalid duration: ${value}`);
  return seconds;
}

function parseDuration(value: string): number {
  const match = /^(\d+(?:\.\d+)?)\s*(ms|s|m|h|d)?$/i.exec(value.trim());
  if (!match) throw new ConfigError(`Invalid duration: ${value}`);
  const unit = (match[2] || 'ms').toLowerCase();
  const multipliers: Record<string, number> = {
    ms: 1,
    s: 1_000,
    m: 60_000,
    h: 3_600_000,
    d: 86_400_000,
  };
  return Number(match[1]) * multipliers[unit];
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

/** Update a transfer spinner concisely; a no-op when no spinner is active (JSON/quiet/non-TTY). */
function updateTransfer(
  spinner: ReturnType<typeof ora> | null,
  label: string,
  progress: TransferProgress,
): void {
  if (!spinner) return;
  const percent = progress.total === 0 ? 100 : Math.floor((progress.transferred / progress.total) * 100);
  spinner.text = `${label} ${percent}%`;
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

/**
 * Prompt for a yes/no confirmation, sharing the same TTY safety across every
 * destructive command: without a TTY it refuses rather than hang, directing the
 * caller to --force. Shell mode disallows prompting entirely (checked by callers).
 */
async function confirmAction(context: CommandContext, prompt: string): Promise<boolean> {
  const input = context.stdin as NodeJS.ReadStream;
  if (!input.isTTY) throw new ConfigError('Refusing to prompt without a TTY; use --force');
  const readline = createInterface({ input, output: context.output.stderr as NodeJS.WriteStream });
  try {
    const answer = await readline.question(`${prompt} [y/N] `);
    return /^y(es)?$/i.test(answer.trim());
  } finally {
    readline.close();
  }
}

function confirmDestroy(context: CommandContext): Promise<boolean> {
  return confirmAction(context, `Destroy the Container for ${context.volume}? Volume data will persist.`);
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
