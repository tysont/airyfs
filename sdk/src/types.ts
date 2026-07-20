// ABOUTME: Public request, response, and helper types for the AiryFS SDK.
// ABOUTME: Uses web-standard types so the core package works in Node, browsers, and Workers.

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

export interface DirectoryEntry extends FileStats { name: string }
export interface DiskUsage { bytes: number; inodes: number }
export interface VolumeInfo { chunkSize: number }
export interface VolumeRecord extends VolumeInfo {
  name: string;
  createdAt: number;
}
export interface VolumePage {
  volumes: VolumeRecord[];
  nextCursor: string | null;
}
export interface QuotaInfo { bytes: number | null; inodes: number | null }
export interface TrashEntry {
  id: string;
  originalPath: string;
  trashPath: string;
  type: 'file' | 'directory' | 'symlink';
  size: number;
  deletedAt: number;
}
export interface RestoredTrashEntry extends TrashEntry { restoredPath: string }
export interface ExecResult { exitCode: number; stdout: string; stderr: string }
export type ExecEvent =
  | { type: 'start'; id: string }
  | { type: 'stdout'; id: string; data: string }
  | { type: 'stderr'; id: string; data: string }
  | { type: 'exit'; id: string; exitCode: number; signal?: string; timedOut?: boolean };

export interface TreeSummary { files: number; directories: number; symlinks: number; bytes: number }
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
export interface UploadCompleteResult extends FileStats { path: string; checksum: string }
export interface ChecksumResult { algorithm: 'sha256'; checksum: string; size: number; ino: number }

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
export interface JobLogEntry { seq: number; stream: 'stdout' | 'stderr'; data: string; timestamp: number }
export interface JobLogPage { entries: JobLogEntry[]; next: number | null }
export interface ServiceLogPage extends JobLogPage {
  generation: string | null;
  earliestSeq: number | null;
  reset: boolean;
  truncated: boolean;
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

export interface HranaCounters { pipelineRequests: number; sqlStatements: number }
export interface PerfInfo extends HranaCounters { sessionId: string | null }
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
  filesystem: {
    bytesUsed: number;
    inodes: number;
    quotaBytes: number | null;
    quotaInodes: number | null;
    bytesAvailable: number | null;
    inodesAvailable: number | null;
  };
  sqliteBytes: number;
  container: ContainerHealth;
  hrana: HranaCounters;
}
export interface UsageSample {
  sampledAt: number;
  bytesUsed: number;
  inodes: number;
  sqliteBytes: number;
  quotaBytes: number | null;
  quotaInodes: number | null;
}
export interface UsageHistoryPage {
  samples: UsageSample[];
  next: number | null;
}
export interface TreeViewEntry {
  path: string;
  name: string;
  depth: number;
  type: 'file' | 'directory' | 'symlink';
  size: number;
}
export interface TreeViewResponse {
  root: string;
  entries: TreeViewEntry[];
  truncated: boolean;
}
export type DatabaseInfo = Record<string, number>;

export type Operation = 'read' | 'write' | 'exec' | 'sql' | 'admin';
export type SqlValue = string | number | null | { base64: string };
export interface SqlResult {
  columns: string[];
  rows: SqlValue[][];
  rowsRead: number;
  rowsWritten: number;
  truncated: boolean;
}
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
export interface MintedCapability extends CapabilityInfo { token: string }

export interface AiryFSClientOptions {
  fetch?: typeof fetch;
  token?: string;
  headers?: HeadersInit;
}
export interface WatchChangesOptions extends Omit<ChangeQuery, 'wait'> {
  wait?: number;
  onGap?: (page: ChangePage) => void;
}
export interface TailFileOptions {
  lines?: number;
  bytes?: number;
  follow?: boolean;
  retry?: boolean;
  wait?: number;
  signal?: AbortSignal;
  onGap?: (page: ChangePage) => void;
}
export interface PtyExit { exitCode: number; signal?: number }
export interface PtySession {
  write(data: Uint8Array | string): void;
  resize(cols: number, rows: number): void;
  signal(name: string): void;
  onData(listener: (data: Uint8Array) => void): () => void;
  readonly closed: Promise<PtyExit>;
  close(): void;
}
export interface OpenPtyOptions { webSocket?: typeof WebSocket }
export interface ServiceRecord {
  name: string;
  command: string;
  cwd: string;
  env: Record<string, string>;
  port: number;
  enabled: boolean;
  public: boolean;
  createdAt: number;
}
export interface CreateServiceInput {
  name: string;
  command: string;
  cwd?: string;
  env?: Record<string, string>;
  public?: boolean;
}
export interface WaitForJobOptions {
  interval?: number;
  after?: number;
  signal?: AbortSignal;
  onLog?: (entry: JobLogEntry) => void;
}
export interface WaitForJobResult { job: Job; cursor?: number }
export interface ExecStreamResult {
  id: Promise<string>;
  events: AsyncIterable<ExecEvent>;
}
