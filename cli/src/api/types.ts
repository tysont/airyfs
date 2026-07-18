// ABOUTME: Declares the response and request shapes used by the AiryFS HTTP API.
// ABOUTME: Keeps command handlers independent from raw JSON response parsing.

export interface FileStats {
  ino: number;
  mode: number;
  nlink: number;
  uid: number;
  gid: number;
  size: number;
  atime: number;
  mtime: number;
  ctime: number;
  type: 'file' | 'directory' | 'symlink' | 'other';
}

export interface DirectoryEntry extends FileStats {
  name: string;
}

export interface ExecResult {
  exitCode: number;
  stdout: string;
  stderr: string;
}

/**
 * A single NDJSON event from a streaming exec. `stdout`/`stderr` carry base64
 * so arbitrary bytes survive transport; the terminal `exit` event ends a run.
 */
export type ExecEvent =
  | { type: 'start'; id: string }
  | { type: 'stdout'; id: string; data: string }
  | { type: 'stderr'; id: string; data: string }
  | { type: 'exit'; id: string; exitCode: number; signal?: string; timedOut?: boolean };

export interface VolumeInfo {
  chunkSize: number;
}

export interface TreeSummary {
  files: number;
  directories: number;
  symlinks: number;
  bytes: number;
}

export interface SnapshotInfo {
  id: string;
  name: string;
  note: string | null;
  createdAt: number;
  chunkSize: number | null;
  inodeCount: number;
  fileCount: number;
  directoryCount: number;
  symlinkCount: number;
  byteCount: number;
}

export type SnapshotDiffChange = 'added' | 'removed' | 'modified';

export interface SnapshotDiffEntry {
  path: string;
  change: SnapshotDiffChange;
  kind: 'file' | 'directory' | 'symlink' | 'other';
}

export interface UploadStatus {
  id: string;
  path: string;
  size: number;
  offset: number;
  checksum: string;
  createdAt: number;
  updatedAt: number;
}

export interface UploadCompleteResult extends FileStats {
  path: string;
  checksum: string;
}

export interface ChecksumResult {
  algorithm: 'sha256';
  checksum: string;
  size: number;
  ino: number;
}

export type JobStatus = 'queued' | 'running' | 'succeeded' | 'failed' | 'canceled';

export interface Job {
  id: string;
  idempotencyKey: string;
  command: string;
  cwd: string;
  status: JobStatus;
  execId: string | null;
  exitCode: number | null;
  error: string | null;
  cancelRequested: boolean;
  outputBytes: number;
  outputTruncated: boolean;
  createdAt: number;
  updatedAt: number;
  startedAt: number | null;
  finishedAt: number | null;
}

export interface JobLogEntry {
  seq: number;
  stream: 'stdout' | 'stderr';
  data: string;
  timestamp: number;
}

export interface JobLogPage {
  entries: JobLogEntry[];
  next: number | null;
}

export type ChangeType = 'create' | 'modify' | 'remove' | 'rename';

export interface ChangeEvent {
  seq: number;
  type: ChangeType;
  path: string;
  oldPath: string | null;
  ino: number;
  timestamp: number;
}

export interface ChangePage {
  events: ChangeEvent[];
  cursor: number;
  latest: number;
  oldest: number;
  gap: boolean;
}

export interface ChangeQuery {
  since?: number | 'latest';
  limit?: number;
  path?: string;
  wait?: number;
  signal?: AbortSignal;
}

export interface PerfInfo {
  pipelineRequests: number;
  sqlStatements: number;
}

export interface ContainerHealth {
  state: 'connected' | 'stopped' | 'unhealthy';
  status?: string;
  bridgeStarted?: boolean;
  fuseMounted?: boolean;
  fuseExitCode?: number | null;
  cwd?: string;
  error?: string;
}

export interface UsageInfo {
  filesystem: Record<string, unknown>;
  sqliteBytes: number;
  container: ContainerHealth;
  hrana: PerfInfo;
}

export type DatabaseInfo = Record<string, number>;

export type Operation = 'read' | 'write' | 'exec' | 'admin';

export interface CapabilityInfo {
  id: string;
  volume: string;
  operations: Operation[];
  pathPrefixes: string[];
  expires: number;
}

export interface AuthStatus {
  auth: 'disabled' | 'root' | 'capability';
  volume: string;
  capability?: CapabilityInfo;
}

export interface MintCapabilityInput {
  operations: Operation[];
  pathPrefixes: string[];
  expiresInSeconds: number;
}

export interface MintedCapability extends CapabilityInfo {
  token: string;
}
