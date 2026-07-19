// ABOUTME: Renders bounded per-volume AiryFS health and usage as Prometheus text.
// ABOUTME: Uses scrape-time gauges so metrics add no writes to filesystem hot paths.

export interface MetricsSnapshot {
  filesystem: {
    bytesUsed: number;
    inodes: number;
    quotaBytes: number | null;
    quotaInodes: number | null;
    bytesAvailable: number | null;
    inodesAvailable: number | null;
  };
  sqliteBytes: number;
  container: {
    state: unknown;
    hranaConnected?: unknown;
    fuseMounted?: unknown;
    [key: string]: unknown;
  };
  hrana: { pipelineRequests: number; sqlStatements: number };
}

export function renderPrometheusMetrics(snapshot: MetricsSnapshot, tableRows: Record<string, number>): string {
  const lines: string[] = [];
  gauge(lines, 'airyfs_filesystem_bytes_used', 'Logical bytes stored in the volume filesystem.', snapshot.filesystem.bytesUsed);
  gauge(lines, 'airyfs_filesystem_inodes', 'Inodes used by the volume filesystem.', snapshot.filesystem.inodes);
  optionalGauge(lines, 'airyfs_filesystem_quota_bytes', 'Configured logical byte quota.', snapshot.filesystem.quotaBytes);
  optionalGauge(lines, 'airyfs_filesystem_quota_inodes', 'Configured inode quota.', snapshot.filesystem.quotaInodes);
  optionalGauge(lines, 'airyfs_filesystem_bytes_available', 'Logical bytes remaining before quota.', snapshot.filesystem.bytesAvailable);
  optionalGauge(lines, 'airyfs_filesystem_inodes_available', 'Inodes remaining before quota.', snapshot.filesystem.inodesAvailable);
  gauge(lines, 'airyfs_sqlite_bytes', 'Physical Durable Object SQLite database size.', snapshot.sqliteBytes);
  const containerUp = snapshot.container.state === 'healthy' && snapshot.container.health !== 'unhealthy';
  gauge(lines, 'airyfs_container_up', 'Whether the volume Container reports healthy.', containerUp ? 1 : 0);
  gauge(lines, 'airyfs_hrana_connected', 'Whether the Container Hrana bridge is connected.', snapshot.container.hranaConnected ? 1 : 0);
  if (snapshot.container.fuseMounted !== undefined) {
    gauge(lines, 'airyfs_fuse_mounted', 'Whether AgentFS is mounted in the Container.', snapshot.container.fuseMounted ? 1 : 0);
  }
  gauge(lines, 'airyfs_hrana_pipeline_requests', 'Hrana pipeline requests in the current bridge session.', snapshot.hrana.pipelineRequests);
  gauge(lines, 'airyfs_hrana_sql_statements', 'SQL statements in the current Hrana bridge session.', snapshot.hrana.sqlStatements);

  lines.push('# HELP airyfs_table_rows Rows in an AiryFS-owned SQLite table.');
  lines.push('# TYPE airyfs_table_rows gauge');
  for (const [table, rows] of Object.entries(tableRows).sort(([left], [right]) => left.localeCompare(right))) {
    lines.push(`airyfs_table_rows{table="${escapeLabel(table)}"} ${finite(rows)}`);
  }
  return `${lines.join('\n')}\n`;
}

function optionalGauge(lines: string[], name: string, help: string, value: number | null): void {
  if (value !== null) gauge(lines, name, help, value);
}

function gauge(lines: string[], name: string, help: string, value: number): void {
  lines.push(`# HELP ${name} ${help}`);
  lines.push(`# TYPE ${name} gauge`);
  lines.push(`${name} ${finite(value)}`);
}

function finite(value: number): number {
  return Number.isFinite(value) ? value : 0;
}

function escapeLabel(value: string): string {
  return value.replaceAll('\\', '\\\\').replaceAll('\n', '\\n').replaceAll('"', '\\"');
}
