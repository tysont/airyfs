// ABOUTME: Pure helpers for multi-volume FUSE mounts inside the container.
// ABOUTME: Computes per-guest bridge ports, orders mounts parent-first, and builds agentfs commands.

/** The primary volume's data/invalidation bridge HTTP ports (AgentFS-facing). */
export const PRIMARY_DATA_HTTP_PORT = 8080;
export const PRIMARY_INVALIDATION_HTTP_PORT = 8081;
export const PRIMARY_DATA_TCP_PORT = 9000;
export const PRIMARY_INVALIDATION_TCP_PORT = 9001;

/** Where the primary volume is mounted; guest mountpoints are grafted beneath it. */
export const MOUNT_ROOT = '/volume';

/** A single bridge channel: a TCP port the DO connects to and the HTTP port AgentFS posts to. */
export interface ChannelSpec {
  tcpPort: number;
  httpPort: number;
}

/** A guest volume to mount beneath the primary volume. */
export interface GuestMountSpec {
  /** Absolute mountpoint within the primary volume, e.g. "/data". */
  mountpoint: string;
  /** Target volume id (a label passed to agentfs; routing is via the bridge). */
  targetVolume: string;
  /** Bridge HTTP port serving this guest's data channel. */
  dataHttpPort: number;
  /** Bridge HTTP port serving this guest's invalidation channel. */
  invalidationHttpPort: number;
  /** Bridge TCP port for this guest's data channel (DO connects here). */
  dataTcpPort: number;
  /** Bridge TCP port for this guest's invalidation channel. */
  invalidationTcpPort: number;
  /** Bearer token authorizing the DO's forwarded access to the target volume. */
  authToken: string;
}

/** Deterministic, non-overlapping bridge ports for guest mount index `i` (0-based). */
export function guestChannelPorts(index: number): {
  dataTcpPort: number;
  dataHttpPort: number;
  invalidationTcpPort: number;
  invalidationHttpPort: number;
} {
  return {
    dataTcpPort: 9100 + index,
    dataHttpPort: 8100 + index,
    invalidationTcpPort: 9200 + index,
    invalidationHttpPort: 8200 + index,
  };
}

/** Order mounts so a parent mountpoint is always mounted before a nested child. */
export function orderMountsByDepth<T extends { mountpoint: string }>(mounts: T[]): T[] {
  const depth = (path: string): number => path.split('/').filter(Boolean).length;
  return [...mounts].sort((a, b) => depth(a.mountpoint) - depth(b.mountpoint));
}

/** Build the agentfs command that mounts one guest volume over its stub directory. */
export function buildGuestMountCommand(
  spec: Pick<GuestMountSpec, 'mountpoint' | 'targetVolume' | 'dataHttpPort' | 'invalidationHttpPort' | 'authToken'>,
): string {
  const mountpoint = `${MOUNT_ROOT}${spec.mountpoint}`;
  return [
    'agentfs mount',
    `--remote-url http://localhost:${spec.dataHttpPort}`,
    `--invalidation-url http://localhost:${spec.invalidationHttpPort}`,
    `--auth-token "${spec.authToken}"`,
    '--cache-ttl-ms 1000',
    '--foreground',
    spec.targetVolume,
    mountpoint,
  ].join(' ');
}

/** Build the agentfs command that mounts the primary volume at the mount root. */
export function buildPrimaryMountCommand(): string {
  return [
    'agentfs mount',
    `--remote-url http://localhost:${PRIMARY_DATA_HTTP_PORT}`,
    `--invalidation-url http://localhost:${PRIMARY_INVALIDATION_HTTP_PORT}`,
    '--auth-token ""',
    '--cache-ttl-ms 1000',
    '--foreground',
    'volume',
    MOUNT_ROOT,
  ].join(' ');
}
