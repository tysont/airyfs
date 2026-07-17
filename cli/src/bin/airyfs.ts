#!/usr/bin/env node
// ABOUTME: Executable entry point for the AiryFS command-line client.
// ABOUTME: Delegates argument parsing and command execution to the CLI program.

import { run } from '../program.js';

void run(process.argv).catch((error: unknown) => {
  process.stderr.write(`${error instanceof Error ? error.message : String(error)}\n`);
  process.exitCode = 1;
});
