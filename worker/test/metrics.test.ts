// ABOUTME: Verifies bounded Prometheus exposition for per-volume usage and health.
// ABOUTME: Covers optional quotas, runtime gauges, deterministic tables, and label escaping.

import { describe, expect, it } from 'vitest';
import { renderPrometheusMetrics, type MetricsSnapshot } from '../src/metrics';

describe('Prometheus metrics', () => {
  it('renders usage, runtime health, Hrana session counters, and sorted table rows', () => {
    const text = renderPrometheusMetrics(snapshot(), { z_table: 2, a_table: 1 });

    expect(text).toContain('# TYPE airyfs_filesystem_bytes_used gauge\nairyfs_filesystem_bytes_used 12');
    expect(text).toContain('airyfs_container_up 1');
    expect(text).toContain('airyfs_fuse_mounted 1');
    expect(text).toContain('airyfs_hrana_pipeline_requests 3');
    expect(text.indexOf('table="a_table"')).toBeLessThan(text.indexOf('table="z_table"'));
    expect(text.endsWith('\n')).toBe(true);
  });

  it('omits unconfigured quotas and escapes table labels', () => {
    const value = snapshot();
    value.filesystem.quotaBytes = null;
    value.filesystem.bytesAvailable = null;
    value.container.fuseMounted = undefined;

    const text = renderPrometheusMetrics(value, { 'quoted"table\\name': 1 });
    expect(text).not.toContain('airyfs_filesystem_quota_bytes ');
    expect(text).not.toContain('airyfs_fuse_mounted ');
    expect(text).toContain('table="quoted\\"table\\\\name"');
  });

  it('reports a failed Container health probe as down', () => {
    const value = snapshot();
    value.container.health = 'unhealthy';
    expect(renderPrometheusMetrics(value, {})).toContain('airyfs_container_up 0');
  });
});

function snapshot(): MetricsSnapshot {
  return {
    filesystem: {
      bytesUsed: 12,
      inodes: 4,
      quotaBytes: 100,
      quotaInodes: 10,
      bytesAvailable: 88,
      inodesAvailable: 6,
    },
    sqliteBytes: 4096,
    container: { state: 'healthy', hranaConnected: true, fuseMounted: true },
    hrana: { pipelineRequests: 3, sqlStatements: 7 },
  };
}
