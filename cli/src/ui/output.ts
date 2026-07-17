// ABOUTME: Centralizes human-readable and JSON output for every CLI command.
// ABOUTME: Keeps color, quiet mode, tables, and errors consistent in one-shot and shell modes.

import Table from 'cli-table3';
import pc from 'picocolors';
import type { Writable } from 'node:stream';
import { AiryFSApiError } from '../api/errors.js';

export interface OutputOptions {
  json?: boolean;
  color?: boolean;
  quiet?: boolean;
  stdout?: Writable;
  stderr?: Writable;
}

export class Output {
  readonly stdout: Writable;
  readonly stderr: Writable;
  readonly json: boolean;
  readonly quiet: boolean;
  readonly color: boolean;

  constructor(options: OutputOptions = {}) {
    this.stdout = options.stdout || process.stdout;
    this.stderr = options.stderr || process.stderr;
    this.json = options.json ?? false;
    this.quiet = options.quiet ?? false;
    this.color = (options.color ?? true) && !this.json;
  }

  value(value: unknown): void {
    if (this.quiet) return;
    if (this.json || typeof value !== 'string') {
      this.stdout.write(`${JSON.stringify(value, null, this.json ? 2 : 0)}\n`);
    } else {
      this.stdout.write(`${value}\n`);
    }
  }

  text(value: string): void {
    if (!this.quiet) this.stdout.write(value);
  }

  success(message: string, value?: unknown): void {
    if (this.quiet) return;
    if (this.json) {
      this.value(value === undefined ? { ok: true, message } : { ok: true, ...asRecord(value) });
      return;
    }
    this.stdout.write(`${this.paint(pc.green, message)}\n`);
  }

  table(head: string[], rows: Array<Array<string | number>>): void {
    if (this.quiet) return;
    if (this.json) {
      this.value(rows.map((row) => Object.fromEntries(head.map((key, index) => [key, row[index]]))));
      return;
    }
    const table = new Table({
      head: head.map((cell) => this.paint(pc.bold, cell)),
      style: { head: [], border: this.color ? ['gray'] : [] },
    });
    table.push(...rows);
    this.stdout.write(`${table.toString()}\n`);
  }

  error(error: unknown): void {
    const normalized = normalizeError(error);
    if (this.json) {
      this.stderr.write(`${JSON.stringify({ error: normalized }, null, 2)}\n`);
      return;
    }
    const prefix = this.paint(pc.red, 'Error');
    const code = normalized.code ? this.paint(pc.dim, ` (${normalized.code})`) : '';
    this.stderr.write(`${prefix}${code}: ${normalized.message}\n`);
  }

  dim(value: string): string {
    return this.paint(pc.dim, value);
  }

  bold(value: string): string {
    return this.paint(pc.bold, value);
  }

  private paint(format: (value: string) => string, value: string): string {
    return this.color ? format(value) : value;
  }
}

function normalizeError(error: unknown): { message: string; code?: string; status?: number; path?: string } {
  if (error instanceof AiryFSApiError) {
    return { message: error.message, code: error.code, status: error.status, path: error.path };
  }
  if (error instanceof Error) return { message: error.message };
  return { message: String(error) };
}

function asRecord(value: unknown): Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value)
    ? value as Record<string, unknown>
    : { value };
}
