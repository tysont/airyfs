// ABOUTME: Deployed regression for in-container guest FUSE (a volume mounted into another's namespace, visible via exec).
// ABOUTME: Requires the target deployment to have guest FUSE enabled; run N iterations to prove the two-daemon path is deterministic.

// Guest FUSE is gated off by default (see GUEST_FUSE_ENABLED). Run this only
// against a deployment where it is enabled — it verifies the two-agentfs-daemon
// path that once appeared to wedge the container and is now deterministically
// stable:
//   AIRYFS_URL=... node e2e/guest-fuse.mjs [runs]
// Each iteration, on fresh volumes: confirms the container stays healthy for a
// command that does not touch the mount, waits for the guest mount to become
// ready, reads the target's data through the nested FUSE mount, writes back
// through it, and confirms the write landed in the target volume.

import { AiryFSClient } from '../sdk/dist/index.js';

const endpoint = process.env.AIRYFS_URL;
if (!endpoint) throw new Error('AIRYFS_URL is required');
const token = process.env.AIRYFS_TOKEN;
const clientOptions = token ? { token } : {};
const runs = Number(process.argv[2] ?? '5');

let passed = 0;
let failed = 0;
let healthFailed = 0;
const created = [];

try {
  for (let i = 1; i <= runs; i++) {
    const suffix = `${Date.now()}-${i}`;
    const host = new AiryFSClient(endpoint, `guestfuse-host-${suffix}`, clientOptions);
    const target = new AiryFSClient(endpoint, `guestfuse-target-${suffix}`, clientOptions);
    created.push(host, target);

    await target.createVolume(256 * 1024);
    await target.writeFile('/hello.txt', `content-${i}`);
    await host.createMount('/data', { target: `guestfuse-target-${suffix}` });

    // Container must stay healthy for a command that does not touch the mount —
    // this is the wedge that once took down the whole command server.
    const health = await host.exec('echo HEALTH-OK');
    if (health.exitCode !== 0 || !health.stdout.includes('HEALTH-OK')) {
      healthFailed++;
      console.log(`run ${i}: FAIL container health (${JSON.stringify(health.stdout)})`);
      continue;
    }

    // Wait for the guest mount to become ready, then read + write through FUSE.
    const started = Date.now();
    const result = await host.exec(
      'for n in $(seq 1 20); do mountpoint -q /volume/data && break; sleep 1; done; '
      + 'cat /volume/data/hello.txt; echo GFWRITE > /volume/data/back.txt && echo WROTE-OK',
    );
    const elapsed = Date.now() - started;
    const back = await target.readFileText('/back.txt').catch(() => null);

    const ok = result.exitCode === 0
      && result.stdout.includes(`content-${i}`)
      && result.stdout.includes('WROTE-OK')
      && back === 'GFWRITE\n';
    if (ok) {
      passed++;
      console.log(`run ${i}: PASS (read+write through nested FUSE, ${elapsed}ms incl. mount wait)`);
    } else {
      failed++;
      console.log(`run ${i}: FAIL -> exit=${result.exitCode} stdout=${JSON.stringify(result.stdout)} back=${JSON.stringify(back)}`);
    }
  }
  console.log(`\nguest-fuse regression: ${passed}/${runs} passed, ${failed} failed, ${healthFailed} health failures`);
  if (passed !== runs) process.exitCode = 1;
} finally {
  await Promise.allSettled(created.map((client) => client.destroyContainer()));
  await Promise.allSettled(created.map((client) => client.deleteVolume()));
}
