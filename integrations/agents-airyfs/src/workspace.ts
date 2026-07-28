import { AiryFSClient, AiryFSApiError, waitForJob } from "airyfs-sdk";
import type { Job } from "airyfs-sdk";

/** Container FUSE mount point; exec runs here, so shell paths live under it. */
export const MOUNT_ROOT = "/volume";
/** AiryFS process-level exec deadline (container EXEC_TIMEOUT_MS), in seconds. */
const EXEC_CEILING_SECONDS = 300;

export interface AiryFSWorkspaceOptions {
  /**
   * How many `pre-exec-*` guard snapshots to retain. Older ones are pruned
   * after each `bash` call so an agent that churns files does not slowly fill
   * the volume's capped SQLite database with snapshots. Default 3.
   */
  snapshotRetention?: number;
  /** Take a guard snapshot before each `bash` call. Default true. */
  snapshotBeforeBash?: boolean;
  fetch?: typeof fetch;
}

export interface ExecOutcome {
  exitCode: number;
  stdout: string;
  stderr: string;
}

export interface ListEntry {
  name: string;
  type: "file" | "directory" | "symlink" | "other";
  size: number;
}

function shellQuote(v: string): string {
  return "'" + v.replace(/'/g, "'\\''") + "'";
}

function dirname(path: string): string {
  const idx = path.replace(/\/+$/, "").lastIndexOf("/");
  return idx <= 0 ? "/" : path.slice(0, idx);
}

/**
 * A thin, model-agnostic wrapper around one AiryFS volume, shaped for use as
 * agent tools. Reads, writes, and listings run on the AiryFS *direct path*
 * (Durable Object SQLite, no Container); only `bash`/`runJob` touch the
 * Container. Paths are volume-rooted (`/foo`); inside `bash` the same file is
 * under `/volume/foo`, and `bash`'s cwd is `/volume`, so relative paths line up.
 */
export class AiryFSWorkspace {
  readonly client: AiryFSClient;
  private readonly snapshotRetention: number;
  private readonly snapshotBeforeBash: boolean;

  constructor(
    endpoint: string,
    volume: string,
    options: AiryFSWorkspaceOptions & { token?: string } = {},
  ) {
    this.client = new AiryFSClient(endpoint, volume, {
      token: options.token,
      fetch: options.fetch,
    });
    this.snapshotRetention = options.snapshotRetention ?? 3;
    this.snapshotBeforeBash = options.snapshotBeforeBash ?? true;
  }

  async readFile(path: string): Promise<string> {
    return this.client.readFileText(path);
  }

  async writeFile(path: string, content: string): Promise<void> {
    try {
      await this.client.writeFile(path, content);
    } catch (err) {
      // AiryFS does not create missing parents. For an agent tool, `write
      // path/to/file` should just work, so on ENOENT create the parent tree
      // and retry once (mirrors Flue's FlueFs.writeFile guarantee).
      if (!(err instanceof AiryFSApiError) || err.code !== "ENOENT") throw err;
      await this.mkdirp(dirname(path));
      await this.client.writeFile(path, content);
    }
  }

  /** Recursively create a directory, tolerating existing directories. */
  async mkdirp(path: string): Promise<void> {
    const parts = path.split("/").filter(Boolean);
    let current = "";
    for (const part of parts) {
      current += "/" + part;
      try {
        await this.client.makeDirectory(current);
      } catch (err) {
        if (!(err instanceof AiryFSApiError) || err.code !== "EEXIST") throw err;
        const existing = await this.client.lstat(current);
        if (existing.type !== "directory") throw err;
      }
    }
  }

  async listDir(path: string): Promise<ListEntry[]> {
    const entries = await this.client.listDirectory(path);
    return entries.map((e) => ({ name: e.name, type: e.type, size: e.size }));
  }

  async exists(path: string): Promise<boolean> {
    try {
      await this.client.lstat(path);
      return true;
    } catch (err) {
      if (err instanceof AiryFSApiError && err.code === "ENOENT") return false;
      throw err;
    }
  }

  /**
   * Run a shell command in the Container. Optionally snapshots the volume first
   * (recoverable checkpoint before destructive compute — FUSE-path deletes
   * bypass trash), then prunes old guard snapshots to a bounded count.
   */
  async bash(
    command: string,
    options: { timeoutMs?: number; idempotencyKey?: string } = {},
  ): Promise<ExecOutcome> {
    if (this.snapshotBeforeBash) {
      await this.snapshotGuard();
    }
    return this.exec(command, options);
  }

  /** Low-level durable exec with an explicit idempotency key (for fibers). */
  async exec(
    command: string,
    options: { timeoutMs?: number; idempotencyKey?: string } = {},
  ): Promise<ExecOutcome> {
    let full = command;
    let wrappedSeconds: number | undefined;
    if (options.timeoutMs && options.timeoutMs > 0) {
      const seconds = Math.ceil(options.timeoutMs / 1000);
      if (seconds <= EXEC_CEILING_SECONDS) {
        wrappedSeconds = seconds;
        full = `timeout ${seconds}s sh -c ${shellQuote(command)}`;
      }
      // > 300s: run unwrapped; the platform's 300s ceiling applies.
    }
    const r = await this.client.exec(full, {
      idempotencyKey: options.idempotencyKey ?? crypto.randomUUID(),
    });
    let stderr = r.stderr;
    if (wrappedSeconds !== undefined && r.exitCode === 124) {
      stderr += `\n[airyfs] command timed out after ${wrappedSeconds}s`;
    }
    return { exitCode: r.exitCode, stdout: r.stdout, stderr };
  }

  private async snapshotGuard(): Promise<void> {
    await this.client.createSnapshot(`pre-exec-${Date.now()}`);
    const guards = (await this.client.listSnapshots())
      .filter((s) => (s.name ?? "").startsWith("pre-exec-"))
      .sort((a, b) => b.createdAt - a.createdAt);
    for (const stale of guards.slice(this.snapshotRetention)) {
      await this.client.deleteSnapshot(stale.id);
    }
  }

  // --- Durable jobs (for work beyond the 300s exec ceiling) -----------------

  /**
   * Submit a durable job. Returns immediately; poll with {@link getJob}.
   * `cwd` is *volume-rooted* (`/` is the volume root); the job runner mounts it
   * at `/volume` internally, so pass `/` or `/subdir`, not `/volume/...`.
   */
  async runJob(command: string, cwd = "/"): Promise<Job> {
    return this.client.submitJob(command, cwd);
  }

  async getJob(id: string): Promise<Job> {
    return this.client.getJob(id);
  }

  /**
   * Block until a job settles. Prefer the Agent's `schedule()`-poll pattern in
   * a request handler (see the sample) so a long job does not hold a request
   * open; this blocking helper exists for scripts and tests.
   */
  async waitJob(
    id: string,
    options: { timeoutMs?: number } = {},
  ): Promise<Job> {
    const { job } = await waitForJob(this.client, id, {
      signal: options.timeoutMs ? AbortSignal.timeout(options.timeoutMs) : undefined,
    });
    return job;
  }

  // --- Artifacts served straight from the volume (no Container) -------------

  /**
   * Publish the volume (or a subtree) as a static site. Returns the public URL
   * on this endpoint's path-based route (`/s/<volume>/`). `airyfs-sdk` has no
   * wrapper for this route, so it is a direct authenticated fetch.
   */
  async publishSite(
    options: {
      path?: string;
      indexDocument?: string;
      spa?: boolean;
      directoryListing?: boolean;
    } = {},
  ): Promise<{ url: string }> {
    await this.apiFetch("/sites", "PUT", options);
    return { url: `${this.origin()}/s/${encodeURIComponent(this.client.volume)}/` };
  }

  /**
   * Mint an (optionally expiring) share link for one file. Returns the public
   * URL (`/d/<volume>/<id>`). Also a direct fetch — no SDK wrapper.
   */
  async shareLink(
    path: string,
    options: { expiresInSeconds?: number } = {},
  ): Promise<{ id: string; url: string; expiresAt: number | null }> {
    const share = (await this.apiFetch("/shares", "POST", {
      path,
      expiresInSeconds: options.expiresInSeconds,
    })) as { id: string; expiresAt: number | null };
    return {
      id: share.id,
      expiresAt: share.expiresAt,
      url: `${this.origin()}/d/${encodeURIComponent(this.client.volume)}/${share.id}`,
    };
  }

  private origin(): string {
    return this.client.endpoint.replace(/\/$/, "");
  }

  private async apiFetch(
    suffix: string,
    method: string,
    body: unknown,
  ): Promise<unknown> {
    const url = `${this.origin()}/v1/volumes/${encodeURIComponent(this.client.volume)}${suffix}`;
    const res = await fetch(url, {
      method,
      headers: { "content-type": "application/json" },
      body: JSON.stringify(body),
    });
    if (!res.ok) {
      throw new Error(`AiryFS ${method} ${suffix} failed: ${res.status} ${await res.text()}`);
    }
    return res.json();
  }
}
