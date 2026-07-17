// ABOUTME: Resolves and enforces the active session for remote commands.
// ABOUTME: Creates API and output services shared by all command handlers.

import type { Writable } from 'node:stream';
import { AiryFSClient } from './api/client.js';
import { resolveRemotePath } from './api/paths.js';
import { NO_ACTIVE_SESSION_MESSAGE, SessionManager } from './config/sessions.js';
import { ConfigError } from './config/store.js';
import type { NamedSession } from './config/types.js';
import { Output } from './ui/output.js';

export interface GlobalOptions {
  session?: string;
  json?: boolean;
  color?: boolean;
  quiet?: boolean;
}

export interface Runtime {
  sessions: SessionManager;
  stdout?: Writable;
  stderr?: Writable;
  stdin?: NodeJS.ReadableStream;
  shellMode?: boolean;
  jsonMode?: boolean;
  prompt?: (message: string) => Promise<string>;
  sessionOverride?: string | null;
  onSessionEvent?: (event: SessionEvent) => void;
  exitCode: number;
}

export type SessionEvent =
  | { type: 'select'; name: string }
  | { type: 'delete'; name: string }
  | { type: 'rename'; from: string; to: string };

export interface CommandContext {
  named: NamedSession;
  sessions: SessionManager;
  output: Output;
  stdin: NodeJS.ReadableStream;
  endpoint: string;
  volume: string;
  cwd: string;
  shellMode: boolean;
  client(): AiryFSClient;
  path(input?: string): string;
}

export function createRuntime(overrides: Partial<Runtime> = {}): Runtime {
  return {
    sessions: overrides.sessions || new SessionManager(),
    stdout: overrides.stdout,
    stderr: overrides.stderr,
    stdin: overrides.stdin || process.stdin,
    shellMode: overrides.shellMode ?? false,
    jsonMode: overrides.jsonMode ?? false,
    prompt: overrides.prompt,
    sessionOverride: overrides.sessionOverride,
    onSessionEvent: overrides.onSessionEvent,
    exitCode: 0,
  };
}

export async function createContext(runtime: Runtime, options: GlobalOptions): Promise<CommandContext> {
  const named = await resolveRuntimeSession(runtime, options);
  const endpoint = named.session.endpoint;
  const volume = named.session.volume;
  const output = new Output({
    json: options.json,
    color: options.color,
    quiet: options.quiet,
    stdout: runtime.stdout,
    stderr: runtime.stderr,
  });

  return {
    named,
    sessions: runtime.sessions,
    output,
    stdin: runtime.stdin || process.stdin,
    endpoint,
    volume,
    cwd: named.session.cwd,
    shellMode: runtime.shellMode ?? false,
    client() {
      return new AiryFSClient(endpoint, volume);
    },
    path(input = '.') {
      return resolveRemotePath(named.session.cwd, input);
    },
  };
}

export async function resolveRuntimeSession(
  runtime: Runtime,
  options: GlobalOptions,
  requestedName?: string,
): Promise<NamedSession> {
  if (requestedName) return runtime.sessions.resolve(requestedName);
  if (options.session) return runtime.sessions.resolve(options.session);
  if (runtime.sessionOverride === null) throw new ConfigError(NO_ACTIVE_SESSION_MESSAGE);
  if (runtime.sessionOverride !== undefined) return runtime.sessions.resolve(runtime.sessionOverride);
  return runtime.sessions.resolve();
}
