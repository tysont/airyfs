// ABOUTME: Verifies interactive-shell tokenization and shared command dispatch.
// ABOUTME: Runs with in-memory streams and an isolated AiryFS home directory.

import { mkdtemp, readFile, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { PassThrough, Writable } from 'node:stream';
import { setTimeout as sleep } from 'node:timers/promises';
import { afterEach, describe, expect, it } from 'vitest';
import { SessionManager } from '../src/config/sessions.js';
import { ConfigStore } from '../src/config/store.js';
import { createRuntime } from '../src/context.js';
import { completeShellLine, runShell, tokenize } from '../src/shell.js';

const homes: string[] = [];

afterEach(async () => {
  await Promise.all(homes.splice(0).map((home) => rm(home, { recursive: true, force: true })));
});

describe('tokenize', () => {
  it('parses whitespace, quotes, escapes, and empty arguments', () => {
    expect(tokenize(`exec sh -c "echo hello world" '' a\\ b`)).toEqual([
      'exec', 'sh', '-c', 'echo hello world', '', 'a b',
    ]);
  });

  it('rejects unterminated input', () => {
    expect(() => tokenize(`cat 'missing`)).toThrow('Unterminated quote');
    expect(() => tokenize('cat trailing\\')).toThrow('Trailing escape character');
  });
});

describe('runShell', () => {
  it('dispatches shared commands and persists history', async () => {
    const home = await mkdtemp(join(tmpdir(), 'airyfs-shell-'));
    homes.push(home);
    const sessions = new SessionManager(new ConfigStore(home));
    await sessions.create('test', { endpoint: 'https://example.com', volume: 'volume' });
    const chunks: Buffer[] = [];
    const stdout = new Writable({
      write(chunk, _encoding, callback) {
        chunks.push(Buffer.from(chunk));
        callback();
      },
    });
    const stdin = new PassThrough();
    const runtime = createRuntime({
      sessions,
      stdin,
      stdout,
      stderr: stdout,
      shellMode: true,
    });

    const shell = runShell(runtime, 'test');
    await sleep(10);
    stdin.end('pwd\ncontext\nhelp pwd\nexec tool --session remote\nexit\n');
    await shell;

    expect(Buffer.concat(chunks).toString()).toContain('/');
    const history = await readFile(join(home, 'history'), 'utf8');
    expect(history).toContain('pwd');
    expect(history).toContain('context');
    expect(Buffer.concat(chunks).toString()).toContain('Usage: airyfs pwd');
    expect(Buffer.concat(chunks).toString()).not.toContain('Use `session use <name>`');
  });

  it('creates and deletes the active session through shared shell commands', async () => {
    const home = await mkdtemp(join(tmpdir(), 'airyfs-shell-'));
    homes.push(home);
    const sessions = new SessionManager(new ConfigStore(home));
    const chunks: Buffer[] = [];
    const stdout = new Writable({
      write(chunk, _encoding, callback) {
        chunks.push(Buffer.from(chunk));
        callback();
      },
    });
    const stdin = new PassThrough();
    const runtime = createRuntime({ sessions, stdin, stdout, stderr: stdout, shellMode: true });

    const shell = runShell(runtime);
    await sleep(10);
    stdin.end([
      'pwd',
      'session create',
      'work',
      'https://example.com',
      'volume',
      'pwd',
      'session delete work',
      'pwd',
      'exit',
      '',
    ].join('\n'));
    await shell;

    const rendered = Buffer.concat(chunks).toString();
    expect(rendered).toContain('Created and selected session work');
    expect(rendered).toContain('Deleted session work');
    expect(rendered.match(/No active session/g)).toHaveLength(2);
    expect((await sessions.list()).currentSession).toBeUndefined();
  });

  it('keeps shell selection independent when deleting a non-current persisted session', async () => {
    const home = await mkdtemp(join(tmpdir(), 'airyfs-shell-'));
    homes.push(home);
    const sessions = new SessionManager(new ConfigStore(home));
    await sessions.create('shell', { endpoint: 'https://example.com', volume: 'shell-volume' });
    await sessions.create('global', { endpoint: 'https://example.com', volume: 'global-volume' });
    const chunks: Buffer[] = [];
    const stdout = new Writable({
      write(chunk, _encoding, callback) {
        chunks.push(Buffer.from(chunk));
        callback();
      },
    });
    const stdin = new PassThrough();
    const runtime = createRuntime({ sessions, stdin, stdout, stderr: stdout, shellMode: true });

    const shell = runShell(runtime, 'shell');
    await sleep(10);
    stdin.end('session delete shell\npwd\nexit\n');
    await shell;

    expect((await sessions.list()).currentSession).toBe('global');
    expect(Buffer.concat(chunks).toString()).toContain('No active session');
  });

  it('does not adopt a persisted selection while the shell is explicitly unselected', async () => {
    const home = await mkdtemp(join(tmpdir(), 'airyfs-shell-'));
    homes.push(home);
    const sessions = new SessionManager(new ConfigStore(home));
    const chunks: Buffer[] = [];
    const stdout = new Writable({
      write(chunk, _encoding, callback) {
        chunks.push(Buffer.from(chunk));
        callback();
      },
    });
    const stdin = new PassThrough();
    const runtime = createRuntime({ sessions, stdin, stdout, stderr: stdout, shellMode: true });

    const shell = runShell(runtime);
    await sleep(10);
    await sessions.create('external', { endpoint: 'https://example.com', volume: 'volume' });
    stdin.end('pwd\nexit\n');
    await shell;

    expect(Buffer.concat(chunks).toString()).toContain('No active session');
  });

  it('rejects attached session overrides and clears an earlier failure exit code', async () => {
    const home = await mkdtemp(join(tmpdir(), 'airyfs-shell-'));
    homes.push(home);
    const sessions = new SessionManager(new ConfigStore(home));
    await sessions.create('test', { endpoint: 'https://example.com', volume: 'volume' });
    const chunks: Buffer[] = [];
    const stdout = new Writable({
      write(chunk, _encoding, callback) {
        chunks.push(Buffer.from(chunk));
        callback();
      },
    });
    const stdin = new PassThrough();
    const runtime = createRuntime({ sessions, stdin, stdout, stderr: stdout, shellMode: true });

    const shell = runShell(runtime, 'test');
    await sleep(10);
    stdin.end('-qsother pwd\nmissing-command\npwd\nexit\n');
    await shell;

    expect(Buffer.concat(chunks).toString()).toContain('Use `session use <name>`');
    expect(runtime.exitCode).toBe(0);
  });

  it('returns a failure status when a session override is rejected', async () => {
    const home = await mkdtemp(join(tmpdir(), 'airyfs-shell-'));
    homes.push(home);
    const sessions = new SessionManager(new ConfigStore(home));
    await sessions.create('test', { endpoint: 'https://example.com', volume: 'volume' });
    const stdout = new Writable({ write(_chunk, _encoding, callback) { callback(); } });
    const stdin = new PassThrough();
    const runtime = createRuntime({ sessions, stdin, stdout, stderr: stdout, shellMode: true });

    const shell = runShell(runtime, 'test');
    await sleep(10);
    stdin.end('-qs other pwd\nexit\n');
    await shell;

    expect(runtime.exitCode).toBe(2);
  });
});

describe('completeShellLine', () => {
  it('completes commands and session names', async () => {
    const home = await mkdtemp(join(tmpdir(), 'airyfs-shell-'));
    homes.push(home);
    const sessions = new SessionManager(new ConfigStore(home));
    await sessions.create('sample', { endpoint: 'https://example.com', volume: 'volume' });

    expect(await completeShellLine('wa', sessions, 'sample', ['pwd', 'warm'], ['use']))
      .toEqual([['warm'], 'wa']);
    expect(await completeShellLine('session use sa', sessions, 'sample', ['session'], ['use']))
      .toEqual([['sample'], 'sa']);
  });

  it('completes remote paths relative to the session cwd', async () => {
    const home = await mkdtemp(join(tmpdir(), 'airyfs-shell-'));
    homes.push(home);
    const sessions = new SessionManager(new ConfigStore(home));
    await sessions.create('sample', { endpoint: 'https://example.com', volume: 'volume' });
    const entries = [
      { name: 'demo', type: 'directory' as const, size: 0, ino: 2, mode: 0, nlink: 1, uid: 0, gid: 0, atime: 0, mtime: 0, ctime: 0 },
      { name: 'document.txt', type: 'file' as const, size: 1, ino: 3, mode: 0, nlink: 1, uid: 0, gid: 0, atime: 0, mtime: 0, ctime: 0 },
    ];

    const result = await completeShellLine(
      'cd de', sessions, 'sample', ['cd'], [], async (path) => path === '/' ? entries : [],
    );

    expect(result).toEqual([['demo/'], 'de']);
  });

  it('only completes remote-path argument positions', async () => {
    const home = await mkdtemp(join(tmpdir(), 'airyfs-shell-'));
    homes.push(home);
    const sessions = new SessionManager(new ConfigStore(home));
    await sessions.create('sample', { endpoint: 'https://example.com', volume: 'volume' });
    const entries = [
      { name: 'document.txt', type: 'file' as const, size: 1, ino: 3, mode: 0, nlink: 1, uid: 0, gid: 0, atime: 0, mtime: 0, ctime: 0 },
    ];
    const list = async () => entries;

    expect(await completeShellLine('get document.txt lo', sessions, 'sample', ['get'], [], list))
      .toEqual([[], 'lo']);
    expect(await completeShellLine('truncate document.txt 1', sessions, 'sample', ['truncate'], [], list))
      .toEqual([[], '1']);
    expect(await completeShellLine('put local.txt do', sessions, 'sample', ['put'], [], list))
      .toEqual([['document.txt'], 'do']);
    // push completes only the remote (2nd) positional; pull completes the remote (1st).
    expect(await completeShellLine('push ./local do', sessions, 'sample', ['push'], [], list))
      .toEqual([['document.txt'], 'do']);
    expect(await completeShellLine('pull do', sessions, 'sample', ['pull'], [], list))
      .toEqual([['document.txt'], 'do']);
    expect(await completeShellLine('pull remotedir lo', sessions, 'sample', ['pull'], [], list))
      .toEqual([[], 'lo']);
    // Snapshot names/ids must never be path-completed.
    expect(await completeShellLine('snapshot diff doc', sessions, 'sample', ['snapshot'], [], list))
      .toEqual([[], 'doc']);
    expect(await completeShellLine('snapshot restore doc', sessions, 'sample', ['snapshot'], [], list))
      .toEqual([[], 'doc']);
    expect(await completeShellLine('snap rm doc', sessions, 'sample', ['snap'], [], list))
      .toEqual([[], 'doc']);
  });

  it('completes paths inside an open quote', async () => {
    const home = await mkdtemp(join(tmpdir(), 'airyfs-shell-'));
    homes.push(home);
    const sessions = new SessionManager(new ConfigStore(home));
    await sessions.create('sample', { endpoint: 'https://example.com', volume: 'volume' });
    const entries = [
      { name: 'demo files', type: 'directory' as const, size: 0, ino: 2, mode: 0, nlink: 1, uid: 0, gid: 0, atime: 0, mtime: 0, ctime: 0 },
    ];

    const result = await completeShellLine(
      'cd "demo f', sessions, 'sample', ['cd'], [], async () => entries,
    );

    expect(result).toEqual([['"demo files/"'], '"demo f']);
  });

  it('completes paths with embedded quoted segments', async () => {
    const home = await mkdtemp(join(tmpdir(), 'airyfs-shell-'));
    homes.push(home);
    const sessions = new SessionManager(new ConfigStore(home));
    await sessions.create('sample', { endpoint: 'https://example.com', volume: 'volume' });
    const entries = [
      { name: 'foo bar', type: 'directory' as const, size: 0, ino: 2, mode: 0, nlink: 1, uid: 0, gid: 0, atime: 0, mtime: 0, ctime: 0 },
    ];

    const result = await completeShellLine(
      'cd foo" b', sessions, 'sample', ['cd'], [], async () => entries,
    );

    expect(result).toEqual([['foo\\ bar/'], 'foo" b']);
  });
});
