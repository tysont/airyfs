// ABOUTME: Tests the pure multi-mount helpers: port allocation, depth ordering, and command building.

import assert from 'node:assert/strict';
import { test } from 'node:test';
import {
  buildGuestMountCommand,
  buildPrimaryMountCommand,
  guestChannelPorts,
  orderMountsByDepth,
} from './mounts.js';

test('guestChannelPorts assigns non-overlapping quads per index', () => {
  const a = guestChannelPorts(0);
  const b = guestChannelPorts(1);
  assert.deepEqual(a, { dataTcpPort: 9100, dataHttpPort: 8100, invalidationTcpPort: 9200, invalidationHttpPort: 8200 });
  assert.deepEqual(b, { dataTcpPort: 9101, dataHttpPort: 8101, invalidationTcpPort: 9201, invalidationHttpPort: 8201 });
  const ports = new Set(Object.values(a).concat(Object.values(b)));
  assert.equal(ports.size, 8);
});

test('orderMountsByDepth mounts parents before nested children', () => {
  const ordered = orderMountsByDepth([
    { mountpoint: '/a/b/c' },
    { mountpoint: '/a' },
    { mountpoint: '/a/b' },
  ]);
  assert.deepEqual(ordered.map((m) => m.mountpoint), ['/a', '/a/b', '/a/b/c']);
});

test('buildPrimaryMountCommand points at the primary bridge ports and mount root', () => {
  const command = buildPrimaryMountCommand();
  assert.match(command, /--remote-url http:\/\/localhost:8080/);
  assert.match(command, /--invalidation-url http:\/\/localhost:8081/);
  assert.match(command, /volume \/volume$/);
});

test('buildGuestMountCommand grafts the target under the mount root with its ports and token', () => {
  const command = buildGuestMountCommand({
    mountpoint: '/data',
    targetVolume: 'big',
    dataHttpPort: 8100,
    invalidationHttpPort: 8200,
    authToken: 'tok-123',
  });
  assert.match(command, /--remote-url http:\/\/localhost:8100/);
  assert.match(command, /--invalidation-url http:\/\/localhost:8200/);
  assert.match(command, /--auth-token "tok-123"/);
  assert.match(command, /big \/volume\/data$/);
});
