// ABOUTME: Hermetic tests for the provisioner helpers (no wrangler or network access).

import assert from 'node:assert/strict';
import test from 'node:test';
import { parseDeployedUrl } from './deploy.mjs';
import { generateSecret, secretPutArgs } from './provision.mjs';

test('parses the workers.dev URL from wrangler output', () => {
  const output = [
    'Total Upload: 1234 KiB',
    'Deployed airyfs-int triggers (0.50 sec)',
    '  https://airyfs-int.example.workers.dev',
    'Current Version ID: abc',
  ].join('\n');
  assert.equal(parseDeployedUrl(output), 'https://airyfs-int.example.workers.dev');
});

test('returns null when no URL is present', () => {
  assert.equal(parseDeployedUrl('no url here'), null);
});

test('generates distinct high-entropy secrets', () => {
  const a = generateSecret();
  const b = generateSecret();
  assert.notEqual(a, b);
  assert.ok(a.length >= 40);
  assert.match(a, /^[A-Za-z0-9_-]+$/);
});

test('builds the secret put arguments for a valid env', () => {
  assert.deepEqual(secretPutArgs('int'), ['wrangler', 'secret', 'put', 'AIRYFS_AUTH_SECRET', '--env', 'int']);
  assert.throws(() => secretPutArgs('staging'));
});
