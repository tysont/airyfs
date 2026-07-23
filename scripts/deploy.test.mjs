// ABOUTME: Hermetic tests for AiryFS deployment configuration and guardrails.

import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { join } from 'node:path';
import test from 'node:test';
import {
  REPO_ROOT,
  assertDeployAllowed,
  parseDotEnv,
  resolveAccountId,
  sanitizeDockerConfig,
  validateEnvName,
  wranglerArgs,
} from './deploy.mjs';

const wrangler = JSON.parse(readFileSync(join(REPO_ROOT, 'worker', 'wrangler.jsonc'), 'utf8'));

test('accepts only configured deployment environments', () => {
  assert.equal(validateEnvName('int'), 'int');
  assert.equal(validateEnvName('prod'), 'prod');
  for (const value of ['', 'Prod', 'staging', '-bad']) {
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

test('targets the native Wrangler environment with immediate Container rollout', () => {
  assert.deepEqual(wranglerArgs('int'), [
    'wrangler', 'deploy', '--env', 'int', '--containers-rollout', 'immediate',
  ]);
  assert.deepEqual(wranglerArgs('prod', { dryRun: true }), [
    'wrangler', 'deploy', '--env', 'prod', '--containers-rollout', 'immediate', '--dry-run',
  ]);
});

test('pins Worker, Container, binding, and migration identities', () => {
  assert.equal(wrangler.name, 'airyfs');
  assert.deepEqual(wrangler.migrations, [
    { tag: 'v1', new_sqlite_classes: ['AiryFS'] },
    { tag: 'v2', new_sqlite_classes: ['VolumeRegistry'] },
  ]);
  assert.equal(wrangler.containers[0].instance_type, 'standard-1');
  for (const env of ['int', 'prod']) {
    const config = wrangler.env[env];
    assert.equal(config.containers[0].name, `airyfs-${env}-airyfs`);
    assert.equal(config.containers[0].class_name, 'AiryFS');
    // Pin the provisioned instance type: the default "lite" tier (1/16 vCPU,
    // 256 MiB) starves the multi-process container and intermittently trips the
    // runtime watchdog under sustained FUSE/exec load.
    assert.equal(config.containers[0].instance_type, 'standard-1');
    assert.deepEqual(config.durable_objects.bindings, [
      { name: 'AiryFS', class_name: 'AiryFS' },
      { name: 'VolumeRegistry', class_name: 'VolumeRegistry' },
    ]);
  }
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
  assert.throws(() => assertDeployAllowed('prod', { allowProd: true, allowDirty: true }, false), /never accept/);
  assert.throws(() => assertDeployAllowed('int', {}, false), /dirty tree/);
  assert.doesNotThrow(() => assertDeployAllowed('int', { allowDirty: true }, false));
  assert.doesNotThrow(() => assertDeployAllowed('prod', { dryRun: true }, true));
  assert.doesNotThrow(() => assertDeployAllowed('prod', { dryRun: true, allowDirty: true }, false));
});
