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
export interface VolumeRecord extends VolumeInfo {
  name: string;
  createdAt: number;
}
export interface VolumePage {
  volumes: VolumeRecord[];
  nextCursor: string | null;
}

export interface QuotaInfo {
  bytes: number | null;
  inodes: number | null;
}

export interface TrashEntry {
  id: string;
  originalPath: string;
  trashPath: string;
  type: 'file' | 'directory' | 'symlink';
  size: number;
  deletedAt: number;
}

export interface RestoredTrashEntry extends TrashEntry { restoredPath: string }
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

export interface AssetInfo {
  algorithm: 'sha256';
  checksum: string;
  size: number;
  created: boolean;
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

export interface ServiceLogPage extends JobLogPage {
  generation: string | null;
  earliestSeq: number | null;
  reset: boolean;
  truncated: boolean;
}

export interface JobSchedule {
  id: string;
  name: string;
  cron: string;
  command: string;
  cwd: string;
  enabled: boolean;
  nextRun: number | null;
  lastRun: number | null;
  createdAt: number;
}

export type SqlValue = string | number | null | { base64: string };
export interface SqlResult {
  columns: string[];
  rows: SqlValue[][];
  rowsRead: number;
  rowsWritten: number;
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

export type WebhookEvent = ChangeType;

export interface WebhookInfo {
  id: string;
  url: string;
  pathPrefix: string;
  events: WebhookEvent[];
  createdAt: number;
}

export interface CreatedWebhook extends WebhookInfo {
  secret: string;
}

export interface CreateWebhookInput {
  url: string;
  pathPrefix?: string;
  events?: WebhookEvent[];
}

export interface SearchResult {
  path: string;
  type: 'file' | 'directory' | 'symlink';
  line?: number;
  column?: number;
  text?: string;
}

export interface SearchResponse {
  results: SearchResult[];
  truncated: boolean;
  scannedEntries: number;
  scannedBytes: number;
}

export interface SearchInput {
  mode: 'find' | 'glob' | 'grep';
  path?: string;
  pattern: string;
  regex?: boolean;
  ignoreCase?: boolean;
  limit?: number;
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
  hrana: PerfInfo;
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

export type DatabaseInfo = Record<string, number>;

export type Operation = 'read' | 'write' | 'exec' | 'sql' | 'admin';

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

export interface PasswordStatus {
  volume: string;
  authEnabled: boolean;
  passwordSet: boolean;
}

export interface SiteInfo {
  pathPrefix: string;
  indexDocument: string;
  spa: boolean;
  directoryListing: boolean;
  cacheControl: string | null;
  createdAt: number;
}

export interface SiteStatus {
  published: boolean;
  site: SiteInfo | null;
}

export interface PublishSiteInput {
  path: string;
  indexDocument?: string;
  spa?: boolean;
  directoryListing?: boolean;
  cacheControl?: string;
}

export interface ShareInfo {
  id: string;
  path: string;
  expiresAt: number | null;
  cacheControl: string | null;
  createdAt: number;
}

export interface CreateShareInput {
  path: string;
  expiresInSeconds?: number;
  cacheControl?: string;
}

export interface MintCapabilityInput {
  operations: Operation[];
  pathPrefixes: string[];
  expiresInSeconds: number;
}

export interface MintedCapability extends CapabilityInfo {
  token: string;
}
