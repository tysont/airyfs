// ABOUTME: Hermetic tests for AiryFS environment rendering and deployment guardrails.

import assert from 'node:assert/strict';
import test from 'node:test';
import {
  assertDeployAllowed,
  generateConfig,
  parseDotEnv,
  resolveAccountId,
  sanitizeDockerConfig,
  validateEnvName,
} from './deploy.mjs';

const base = {
  name: 'airyfs',
  vars: { AIRYFS_ENVIRONMENT: 'local' },
  containers: [{ class_name: 'AiryFS', image: '../container/Dockerfile' }],
  durable_objects: { bindings: [{ name: 'AiryFS', class_name: 'AiryFS' }] },
};

test('validates safe environment names', () => {
  assert.equal(validateEnvName('int'), 'int');
  assert.equal(validateEnvName('prod'), 'prod');
  for (const value of ['', 'Prod', 'bad--name', '-bad', 'a'.repeat(21)]) {
    assert.throws(() => validateEnvName(value));
  }
});

test('parses dotenv values without requiring shell evaluation', () => {
  assert.deepEqual(parseDotEnv('A=one\nexport B="two"\n# C=no\n'), { A: 'one', B: 'two' });
});

test('resolves account from process environment before dev vars', () => {
  assert.equal(resolveAccountId({ processEnv: { CLOUDFLARE_ACCOUNT_ID: 'env' }, devVars: 'CLOUDFLARE_ACCOUNT_ID=file' }), 'env');
  assert.equal(resolveAccountId({ processEnv: {}, devVars: 'CLOUDFLARE_ACCOUNT_ID=file' }), 'file');
});

test('rejects explicit and ambient account disagreement', () => {
  assert.throws(() => resolveAccountId({
    explicit: 'explicit',
    processEnv: { CLOUDFLARE_ACCOUNT_ID: 'ambient' },
  }), /disagreement/);
});

test('generates isolated worker identity while preserving bindings', () => {
  const generated = generateConfig(base, 'int', 'account');
  assert.equal(generated.account_id, 'account');
  assert.equal(generated.name, 'airyfs-int');
  assert.equal(generated.vars.AIRYFS_ENVIRONMENT, 'int');
  assert.equal(generated.workers_dev, true);
  assert.equal(generated.preview_urls, false);
  assert.deepEqual(generated.containers, base.containers);
  assert.deepEqual(generated.durable_objects, base.durable_objects);
  assert.equal(base.name, 'airyfs');
});

test('removes Docker keychain helpers while preserving static configuration', () => {
  assert.deepEqual(sanitizeDockerConfig({
    auths: { registry: {} },
    credsStore: 'desktop',
    credHelpers: { registry: 'desktop' },
    currentContext: 'desktop-linux',
  }), {
    auths: { registry: {} },
  });
});

test('guards production and dirty deployments', () => {
  assert.throws(() => assertDeployAllowed('prod', {}, true), /--allow-prod/);
  assert.throws(() => assertDeployAllowed('prod', { allowProd: true, allowDirty: true }, false), /never accepts/);
  assert.throws(() => assertDeployAllowed('int', {}, false), /dirty tree/);
  assert.doesNotThrow(() => assertDeployAllowed('int', { allowDirty: true }, false));
  assert.doesNotThrow(() => assertDeployAllowed('prod', { dryRun: true }, true));
});
