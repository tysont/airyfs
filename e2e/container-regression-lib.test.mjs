// ABOUTME: Tests Container regression argument parsing and profile composition locally.
// ABOUTME: Prevents quick and broad deployed gates from silently losing scenarios.

import assert from 'node:assert/strict';
import test from 'node:test';
import { parseRegressionArgs, REGRESSION_PROFILES } from './container-regression-lib.mjs';

test('parses Container regression options', () => {
  assert.deepEqual(parseRegressionArgs([]), {
    profile: 'quick',
    prefix: 'container-regression',
    help: false,
  });
  assert.deepEqual(parseRegressionArgs(['--profile', 'broad', '--prefix', 'pre-push', '--help']), {
    profile: 'broad',
    prefix: 'pre-push',
    help: true,
  });
  assert.throws(() => parseRegressionArgs(['--profile', 'full']), /Unknown profile/);
  assert.throws(() => parseRegressionArgs(['--profile']), /requires a value/);
  assert.throws(() => parseRegressionArgs(['--prefix', 'not_valid']), /prefix/);
  assert.throws(() => parseRegressionArgs(['--unknown']), /Unknown argument/);
});

test('broad profile is a strict superset of quick', () => {
  assert.deepEqual(REGRESSION_PROFILES.broad.slice(0, REGRESSION_PROFILES.quick.length), REGRESSION_PROFILES.quick);
  assert(REGRESSION_PROFILES.broad.length > REGRESSION_PROFILES.quick.length);
  assert.equal(new Set(REGRESSION_PROFILES.broad).size, REGRESSION_PROFILES.broad.length);
});
