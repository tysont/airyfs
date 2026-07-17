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

export interface VolumeInfo {
  chunkSize: number;
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
