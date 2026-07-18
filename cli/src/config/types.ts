// ABOUTME: Defines the persisted configuration and named-session data model.
// ABOUTME: Sessions keep endpoint, volume, and remote working directory together.

export const CONFIG_VERSION = 3;

/** Config versions this CLI can read; older ones migrate forward in memory. */
export const SUPPORTED_CONFIG_VERSIONS = [2, 3] as const;

export interface AiryFSSession {
  endpoint: string;
  volume: string;
  cwd: string;
  /** Optional bearer credential (root secret or capability token) for this session. */
  token?: string;
  createdAt: string;
  updatedAt: string;
}

export interface AiryFSConfig {
  version: typeof CONFIG_VERSION;
  currentSession?: string;
  sessions: Record<string, AiryFSSession>;
}

export interface NamedSession {
  name: string;
  session: AiryFSSession;
}

export function emptyConfig(): AiryFSConfig {
  return {
    version: CONFIG_VERSION,
    sessions: Object.create(null) as Record<string, AiryFSSession>,
  };
}
