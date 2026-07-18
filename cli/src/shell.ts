// ABOUTME: Implements the interactive AiryFS shell over the shared command program.
// ABOUTME: Persists history while pinning each shell to its own named session.

import { mkdir, readFile, writeFile } from 'node:fs/promises';
import { join } from 'node:path';
import { createInterface } from 'node:readline/promises';
import pc from 'picocolors';
import { AiryFSClient } from './api/client.js';
import { resolveRemotePath } from './api/paths.js';
import type { DirectoryEntry } from './api/types.js';
import type { SessionManager } from './config/sessions.js';
import type { Runtime } from './context.js';
import { createProgram, execute } from './program.js';

const MAX_HISTORY = 500;

export async function runShell(parentRuntime: Runtime, initialSession?: string): Promise<void> {
  const input = parentRuntime.stdin || process.stdin;
  const output = parentRuntime.stdout || process.stdout;
  const historyPath = join(parentRuntime.sessions.store.home, 'history');
  const history = await loadHistory(historyPath);
  const entered: string[] = [];
  let sessionName = initialSession;
  const completionProgram = createProgram({ ...parentRuntime, shellMode: true, exitCode: 0 });
  const commandNames = commandAndAliasNames(completionProgram.commands);
  const sessionCommand = completionProgram.commands.find((command) => command.name() === 'session');
  const sessionCommandNames = commandAndAliasNames(sessionCommand?.commands || []);

  const readline = createInterface({
    input,
    output,
    terminal: Boolean((input as NodeJS.ReadStream).isTTY && (output as NodeJS.WriteStream).isTTY),
    history,
    historySize: MAX_HISTORY,
    removeHistoryDuplicates: true,
    completer: (line: string) => {
      return completeShellLine(
        line,
        parentRuntime.sessions,
        sessionName,
        commandNames,
        sessionCommandNames,
      ).catch(() => [[], currentCompletionWord(line)] as [string[], string]);
    },
  });

  output.write('AiryFS interactive shell. Type `help` for commands, `exit` to leave.\n');
  const lines = readline[Symbol.asyncIterator]();
  try {
    while (true) {
      let selected;
      if (sessionName) {
        try {
          selected = await parentRuntime.sessions.resolve(sessionName);
        } catch (error) {
          if (!(error instanceof Error && error.message === `Session "${sessionName}" does not exist`)) throw error;
          (parentRuntime.stderr || process.stderr).write(
            `Session "${sessionName}" no longer exists; this shell now has no active session.\n`,
          );
          sessionName = undefined;
        }
      }
      const prompt = selected
        ? pc.cyan(`airyfs:${escapeControls(selected.name)}:${escapeControls(selected.session.volume)}:${escapeControls(selected.session.cwd)}$ `)
        : pc.cyan('airyfs:no-session$ ');
      readline.setPrompt(prompt);
      readline.prompt();
      const next = await lines.next();
      if (next.done) break;
      const line = next.value;
      const trimmed = line.trim();
      if (!trimmed) continue;
      entered.push(trimmed);
      if (trimmed === 'exit' || trimmed === 'quit') break;
      if (trimmed === 'clear') {
        output.write('\x1bc');
        parentRuntime.exitCode = 0;
        continue;
      }

      let args: string[];
      try {
        args = tokenize(trimmed);
      } catch (error) {
        (parentRuntime.stderr || process.stderr).write(`Error: ${error instanceof Error ? error.message : String(error)}\n`);
        parentRuntime.exitCode = 2;
        continue;
      }
      if (hasGlobalSessionOverride(args)) {
        (parentRuntime.stderr || process.stderr).write('Use `session use <name>` to switch this shell session.\n');
        parentRuntime.exitCode = 2;
        continue;
      }
      if (args[0] === 'help') args = args[1] ? [...args.slice(1), '--help'] : ['--help'];
      if (args[0] === 'shell') {
        (parentRuntime.stderr || process.stderr).write('Already in the AiryFS shell.\n');
        parentRuntime.exitCode = 2;
        continue;
      }

      const code = await execute(['node', 'airyfs', ...args], {
        ...parentRuntime,
        shellMode: true,
        exitCode: 0,
        sessionOverride: sessionName ?? null,
        onSessionEvent: (event) => {
          if (event.type === 'select') sessionName = event.name;
          else if (event.type === 'delete' && sessionName === event.name) sessionName = undefined;
          else if (event.type === 'rename' && sessionName === event.from) sessionName = event.to;
        },
        prompt: async (message) => {
          output.write(message);
          const answer = await lines.next();
          if (answer.done) throw new Error('Input closed while waiting for session configuration');
          return answer.value;
        },
      });
      parentRuntime.exitCode = code;
    }
  } finally {
    readline.close();
    await mkdir(parentRuntime.sessions.store.home, { recursive: true, mode: 0o700 });
    const combined = [...history.slice().reverse(), ...entered];
    const unique = combined.filter((line, index) => index === combined.length - 1 || line !== combined[index + 1]);
    await writeFile(historyPath, `${unique.slice(-MAX_HISTORY).join('\n')}\n`, { mode: 0o600 });
  }
}

export async function completeShellLine(
  line: string,
  sessions: SessionManager,
  sessionName: string | undefined,
  commandNames: string[],
  sessionCommandNames: string[],
  listDirectory?: (path: string) => Promise<DirectoryEntry[]>,
): Promise<[string[], string]> {
  const token = completionToken(line);
  const word = token.raw;
  let args: string[];
  try {
    args = token.before ? tokenize(token.before) : [];
  } catch {
    return [[], word];
  }

  if (args.length === 0) return [matches(commandNames, token.value), word];
  if (args[0] === 'session') {
    if (args.length === 1) return [matches(sessionCommandNames, token.value), word];
    const completesSessionName = (
      ['use', 'delete', 'rm', 'show', 'edit'].includes(args[1]) || args[1] === 'rename'
    ) && args.length === 2;
    if (completesSessionName) {
      const names = (await sessions.list()).sessions.map(({ name }) => name);
      return [matches(names, token.value).map((name) => formatCompletion(name, token.quote)), word];
    }
    return [[], word];
  }

  const remotePathPositions: Record<string, number[]> = {
    cd: [1], ls: [1], cat: [1], get: [1], put: [2], write: [1], mkdir: [1], rm: [1],
    mv: [1, 2], cp: [1, 2], ln: [1, 2], readlink: [1], truncate: [1], stat: [1],
    push: [2], pull: [1],
  };
  const position = args.slice(1).filter((argument) => !argument.startsWith('-')).length + 1;
  if (!remotePathPositions[args[0]]?.includes(position) || token.value.startsWith('-') || !sessionName) {
    return [[], word];
  }

  try {
    const selected = await sessions.resolve(sessionName);
    const decodedWord = token.value;
    const slash = decodedWord.lastIndexOf('/');
    const typedDirectory = slash >= 0 ? decodedWord.slice(0, slash + 1) : '';
    const namePrefix = slash >= 0 ? decodedWord.slice(slash + 1) : decodedWord;
    const directory = resolveRemotePath(selected.session.cwd, typedDirectory || '.');
    const entries = listDirectory
      ? await listDirectory(directory)
      : await new AiryFSClient(selected.session.endpoint, selected.session.volume).listDirectory(directory);
    const completions = entries
      .filter((entry) => entry.name.startsWith(namePrefix))
      .map((entry) => `${typedDirectory}${entry.name}${entry.type === 'directory' ? '/' : ''}`)
      .map((value) => formatCompletion(value, token.quote));
    return [completions, word];
  } catch {
    return [[], word];
  }
}

function commandAndAliasNames(commands: ReadonlyArray<{ name(): string; aliases(): string[] }>): string[] {
  return [...new Set(commands.flatMap((command) => [command.name(), ...command.aliases()]))].sort();
}

function currentCompletionWord(line: string): string {
  return completionToken(line).raw;
}

function matches(values: string[], prefix: string): string[] {
  return values.filter((value) => value.startsWith(prefix));
}

function formatCompletion(value: string, quote?: "'" | '"'): string {
  if (quote) return `"${value.replace(/([\\"$`])/g, '\\$1')}"`;
  return value.replace(/([\\\s'"$`])/g, '\\$1');
}

function completionToken(line: string): { before: string; raw: string; value: string; quote?: "'" | '"' } {
  let start = 0;
  let quote: "'" | '"' | undefined;
  let escaped = false;
  for (let index = 0; index < line.length; index++) {
    const character = line[index];
    if (escaped) {
      escaped = false;
      continue;
    }
    if (character === '\\' && quote !== "'") {
      escaped = true;
      continue;
    }
    if ((character === "'" || character === '"') && (!quote || quote === character)) {
      quote = quote ? undefined : character;
      continue;
    }
    if (/\s/.test(character) && !quote) start = index + 1;
  }

  const raw = line.slice(start);
  const openingQuote = raw[0] === "'" || raw[0] === '"' ? raw[0] as "'" | '"' : undefined;
  let value = '';
  let valueQuote: "'" | '"' | undefined;
  let valueEscaped = false;
  for (const character of raw) {
    if (valueEscaped) {
      value += character;
      valueEscaped = false;
    } else if (character === '\\' && valueQuote !== "'") {
      valueEscaped = true;
    } else if ((character === "'" || character === '"') && (!valueQuote || valueQuote === character)) {
      valueQuote = valueQuote ? undefined : character;
    } else {
      value += character;
    }
  }
  if (valueEscaped) value += '\\';
  return { before: line.slice(0, start).trim(), raw, value, quote: openingQuote };
}

function hasGlobalSessionOverride(args: string[]): boolean {
  for (let index = 0; index < args.length; index++) {
    const argument = args[index];
    if (argument === '-s' || /^-[^-]*s/.test(argument)
      || argument === '--session' || argument.startsWith('--session=')) return true;
    if (argument.startsWith('-')) continue;
    else return false;
  }
  return false;
}

function escapeControls(value: string): string {
  return value.replace(/[\x00-\x1f\x7f-\x9f]/g, (character) => {
    return `\\x${character.charCodeAt(0).toString(16).padStart(2, '0')}`;
  });
}

export function tokenize(input: string): string[] {
  const tokens: string[] = [];
  let token = '';
  let quote: 'single' | 'double' | null = null;
  let escaped = false;
  let started = false;

  for (const character of input) {
    if (escaped) {
      token += character;
      escaped = false;
      started = true;
      continue;
    }
    if (character === '\\' && quote !== 'single') {
      escaped = true;
      started = true;
      continue;
    }
    if (character === "'" && quote !== 'double') {
      quote = quote === 'single' ? null : 'single';
      started = true;
      continue;
    }
    if (character === '"' && quote !== 'single') {
      quote = quote === 'double' ? null : 'double';
      started = true;
      continue;
    }
    if (/\s/.test(character) && !quote) {
      if (started) tokens.push(token);
      token = '';
      started = false;
      continue;
    }
    token += character;
    started = true;
  }

  if (escaped) throw new Error('Trailing escape character');
  if (quote) throw new Error('Unterminated quote');
  if (started) tokens.push(token);
  return tokens;
}

async function loadHistory(path: string): Promise<string[]> {
  try {
    const lines = (await readFile(path, 'utf8')).split('\n').filter(Boolean);
    return lines.slice(-MAX_HISTORY).reverse();
  } catch (error) {
    if (error instanceof Error && 'code' in error && error.code === 'ENOENT') return [];
    throw error;
  }
}
