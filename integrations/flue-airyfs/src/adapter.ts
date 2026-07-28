import {
  createSandboxSessionEnv,
  SandboxOperationUnsupportedError,
} from "@flue/runtime";
import type {
  SandboxApi,
  SandboxFactory,
  SessionEnv,
  FileStat,
  ShellResult,
} from "@flue/runtime";
import { AiryFSClient, AiryFSApiError } from "airyfs-sdk";

import { MOUNT_ROOT, toSdkPath, shellQuote, defaultVolumeName } from "./paths.js";

/** AiryFS process-level exec deadline (container `EXEC_TIMEOUT_MS`), in seconds. */
const EXEC_CEILING_SECONDS = 300;

function apiCode(err: unknown): string | undefined {
  return err instanceof AiryFSApiError ? err.code : undefined;
}

/**
 * A {@link SandboxApi} implementation backed by a single AiryFS volume.
 *
 * Eight of the nine methods are served straight from the AiryFS *direct path*
 * (Durable Object SQLite, no Container) at sub-100ms latencies. Only
 * {@link AiryFSSandboxApi.exec} attaches / wakes the Container.
 *
 * The workspace is rooted at `/volume`; see `paths.ts` for the path-plane
 * rationale. File methods translate to volume-rooted SDK paths; `exec` runs in
 * the `/volume` mount plane untouched.
 */
export class AiryFSSandboxApi implements SandboxApi {
  constructor(private readonly client: AiryFSClient) {}

  async readFile(path: string): Promise<string> {
    return this.client.readFileText(toSdkPath(path));
  }

  async readFileBuffer(path: string): Promise<Uint8Array> {
    // airyfs-sdk returns bytes directly; no Blob round-trip needed.
    return this.client.readFileBytes(toSdkPath(path));
  }

  async writeFile(path: string, content: string | Uint8Array): Promise<void> {
    // Do NOT create parents here. AiryFS returns ENOENT for a missing parent;
    // createSandboxSessionEnv catches that, calls mkdir(parent,{recursive}) and
    // retries the write once. Both string and Uint8Array are valid BodyInit.
    await this.client.writeFile(toSdkPath(path), content);
  }

  async stat(path: string): Promise<FileStat> {
    // lstat does not follow a final symlink, so isSymbolicLink is meaningful.
    const s = await this.client.lstat(toSdkPath(path));
    const stat: FileStat = {
      isFile: s.type === "file",
      isDirectory: s.type === "directory",
      isSymbolicLink: s.type === "symlink",
      // AiryFS metadata is real POSIX data from fs_inode — never fabricated.
      // mtime is Unix *seconds* (float); FileStat wants a Date.
      size: s.size,
      mtime: new Date(s.mtime * 1000),
    };
    return stat;
  }

  async readdir(path: string): Promise<string[]> {
    const entries = await this.client.listDirectory(toSdkPath(path));
    return entries.map((e) => e.name);
  }

  async exists(path: string): Promise<boolean> {
    try {
      await this.client.lstat(toSdkPath(path));
      return true;
    } catch (err) {
      if (apiCode(err) === "ENOENT") return false;
      throw err;
    }
  }

  async mkdir(path: string, options?: { recursive?: boolean }): Promise<void> {
    const sdkPath = toSdkPath(path);
    if (!options?.recursive) {
      await this.client.makeDirectory(sdkPath);
      return;
    }
    // Recursive: segment-walk over the direct path. Tolerate EEXIST only when
    // the existing entry is a directory — a *file* occupying a segment must
    // propagate as an error, not be silently swallowed.
    const parts = sdkPath.split("/").filter(Boolean);
    let current = "";
    for (const part of parts) {
      current += "/" + part;
      try {
        await this.client.makeDirectory(current);
      } catch (err) {
        if (apiCode(err) !== "EEXIST") throw err;
        const existing = await this.client.lstat(current);
        if (existing.type !== "directory") {
          throw err; // e.g. ENOTDIR: a file sits where a directory segment is needed
        }
      }
    }
  }

  async rm(
    path: string,
    options?: { recursive?: boolean; force?: boolean },
  ): Promise<void> {
    const sdkPath = toSdkPath(path);
    let type: string;
    try {
      ({ type } = await this.client.lstat(sdkPath));
    } catch (err) {
      // force ignores a missing path; otherwise propagate ENOENT.
      if (options?.force && apiCode(err) === "ENOENT") return;
      throw err;
    }
    if (type === "directory") {
      // Soft delete (trash-backed, recoverable) — a strictly safer superset of
      // rm for an agent workspace. recursive is honored exactly.
      await this.client.removeDirectory(sdkPath, options?.recursive ?? false, false);
    } else {
      await this.client.deleteFile(sdkPath, false);
    }
  }

  async exec(
    command: string,
    options?: {
      cwd?: string;
      env?: Record<string, string>;
      timeoutMs?: number;
      signal?: AbortSignal;
    },
  ): Promise<ShellResult> {
    // Flue hands cwd as a /volume-plane absolute path; translate it to the
    // volume-rooted cwd the SDK's unified exec expects (`/volume/work` -> `/work`,
    // `/volume` -> `/`). Env is still exported inline (exec has no env option).
    const withEnv = this.applyEnv(command, options?.env);

    let full = withEnv;
    let wrappedSeconds: number | undefined;
    if (options?.timeoutMs && options.timeoutMs > 0) {
      // Round UP to whole seconds, never down (adapter-spec rule).
      const seconds = Math.ceil(options.timeoutMs / 1000);
      if (seconds <= EXEC_CEILING_SECONDS) {
        // Enforce the caller's deadline with timeout(1): exit 124 on expiry,
        // matching the cross-adapter / timeout(1) convention.
        wrappedSeconds = seconds;
        full = `timeout ${seconds}s sh -c ${shellQuote(withEnv)}`;
      }
      // else: deadline exceeds the platform's 300s process ceiling. Do NOT
      // throw (exec is best-effort per spec, and a thrown error mid-loop is
      // worse than a result the model can react to). Run unwrapped; the
      // platform's own 300s deadline still applies.
    }

    // Durable exec (SDK default): queues behind the single execution slot
    // instead of failing with EXEC_BUSY, which is friendlier for an agent
    // loop. The per-call idempotency key gives retry/replay robustness within
    // this one call (not cross-call dedupe). cwd goes through the SDK's unified
    // volume-rooted cwd option — the same path jobs use.
    const result = await this.client.exec(full, {
      idempotencyKey: crypto.randomUUID(),
      signal: options?.signal,
      cwd: options?.cwd ? toSdkPath(options.cwd) : undefined,
    });

    let stderr = result.stderr;
    if (wrappedSeconds !== undefined && result.exitCode === 124) {
      stderr += `\n[airyfs-flue] command timed out after ${wrappedSeconds}s`;
    }
    return { stdout: result.stdout, stderr, exitCode: result.exitCode };
  }

  private applyEnv(command: string, env?: Record<string, string>): string {
    const exports = Object.entries(env ?? {})
      .map(([k, v]) => `export ${k}=${shellQuote(v)}; `)
      .join("");
    return `${exports}${command}`;
  }
}

/** Configuration for the {@link airyfs} sandbox factory. */
export interface AiryFSSandboxConfig {
  /** AiryFS endpoint origin, e.g. `https://airyfs-int.example.workers.dev`. */
  endpoint: string;
  /**
   * Bearer credential. Omit for an unauthenticated endpoint. For production,
   * mint a *volume-scoped* `read,write,exec` capability rather than the root
   * admin secret so the isolation unit (one volume per agent) and the
   * credential unit coincide.
   */
  token?: string;
  /**
   * Map the harness id (agent instance id, or workflow run id) to a volume
   * name. Defaults to a collision-resistant `agent-<slug>-<hash>`. Keying on
   * the id gives each agent instance a durable, zero-provisioning workspace
   * that survives harness restarts and Container replacement.
   */
  volume?: (id: string) => string;
  /** Custom fetch (e.g. to inject a CA bundle in tests). */
  fetch?: typeof fetch;
}

/**
 * Build a Flue {@link SandboxFactory} that gives every harness a durable AiryFS
 * volume as its workspace. `createSessionEnv({ id })` is idempotent: repeated
 * calls for the same id resolve to the same durable volume (deliberate reuse,
 * per the SandboxFactory contract).
 */
export function airyfs(config: AiryFSSandboxConfig): SandboxFactory {
  const volumeFor = config.volume ?? defaultVolumeName;
  return {
    async createSessionEnv({ id }): Promise<SessionEnv> {
      const client = new AiryFSClient(config.endpoint, volumeFor(id), {
        token: config.token,
        fetch: config.fetch,
      });
      return createSandboxSessionEnv(new AiryFSSandboxApi(client), MOUNT_ROOT);
    },
  };
}

/**
 * Build a factory from an already-constructed {@link AiryFSClient}, mirroring
 * the `daytona(sandbox)` idiom for callers that own client creation (custom
 * headers, a shared client, a pre-scoped capability). The `id` is ignored — the
 * client's volume is authoritative.
 */
export function airyfsClient(client: AiryFSClient): SandboxFactory {
  return {
    async createSessionEnv(): Promise<SessionEnv> {
      return createSandboxSessionEnv(new AiryFSSandboxApi(client), MOUNT_ROOT);
    },
  };
}

export { SandboxOperationUnsupportedError };
