// ABOUTME: Defines the persisted configuration and named-session data model.
// ABOUTME: Sessions keep endpoint, volume, and remote working directory together.

export const CONFIG_VERSION = 2;

export interface AiryFSSession {
  endpoint: string;
  volume: string;
  cwd: string;
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
