// ABOUTME: Hermetic tests for the AiryFS installer helpers.

import assert from 'node:assert/strict';
import test from 'node:test';
import {
  MIN_NODE_MAJOR,
  assertNodeSupported,
  installSteps,
  parseNodeMajor,
} from './install.mjs';

test('parses a Node major from version strings', () => {
  assert.equal(parseNodeMajor('v22.11.0'), 22);
  assert.equal(parseNodeMajor('23.1.0'), 23);
  assert.throws(() => parseNodeMajor('not-a-version'));
});

test('accepts supported Node and rejects older runtimes', () => {
  assert.equal(assertNodeSupported(`v${MIN_NODE_MAJOR}.0.0`), MIN_NODE_MAJOR);
  assert.equal(assertNodeSupported('v25.0.0'), 25);
  assert.throws(() => assertNodeSupported('v20.0.0'), /required/);
});

test('builds the SDK before the CLI and links last', () => {
  const steps = installSteps();
  assert.deepEqual(steps.map((step) => step.dir), ['sdk', 'cli', 'cli']);
  assert.deepEqual(steps.at(-1).commands, [['link']]);
});

test('omits the link step when link is disabled', () => {
  const steps = installSteps({ link: false });
  assert.deepEqual(steps.map((step) => step.dir), ['sdk', 'cli']);
  assert.ok(!steps.some((step) => step.commands.some((cmd) => cmd[0] === 'link')));
});
