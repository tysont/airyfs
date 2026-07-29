import { createSandboxSessionEnv } from "@flue/runtime";
import type {
  SandboxApi,
  SandboxFactory,
  SessionEnv,
  FileStat,
  ShellResult,
} from "@flue/runtime";
import { toSdkPath, MOUNT_ROOT, defaultVolumeName } from "@airyfs/flue-sandbox";

const EXEC_CEILING_SECONDS = 300;

/** POSIX stat DTO returned by the AiryFS Durable Object RPC methods. */
interface StatsDto {
  type: "file" | "directory" | "symlink" | "other";
  size: number;
  mtime: number; // Unix seconds
}

/**
 * Minimal structural view of the AiryFS Durable Object's RPC surface — only the
 * methods this adapter calls. Typed structurally so the example does not import
 * the entire AiryFS Worker (its class, container, and migrations). At runtime
 * the stub is the real `AiryFS` Durable Object, reached over a cross-script
 * Durable Object binding, so these calls are DO-to-DO RPC with no HTTP layer.
 */
export interface AiryFSRpcStub {
  readFile(path: string): Promise<string>;
  writeFile(path: string, content: string): Promise<void>;
  readFileStream(path: string): Promise<ReadableStream<Uint8Array>>;
  writeFileStream(path: string, stream: ReadableStream<Uint8Array>): Promise<void>;
  listDir(path: string): Promise<string[]>;
  lstatPath(path: string): Promise<StatsDto>;
  makeDir(path: string, recursive?: boolean): Promise<void>;
  removePath(path: string, recursive?: boolean): Promise<void>;
  exec(command: string): Promise<{ exitCode: number; stdout: string; stderr: string }>;
}

/** Just enough of DurableObjectNamespace to resolve a stub by name. */
export interface RpcNamespace {
  idFromName(name: string): unknown;
  get(id: unknown): AiryFSRpcStub;
}

function shellQuote(v: string): string {
  return "'" + v.replace(/'/g, "'\\''") + "'";
}

function rpcCode(err: unknown): string | undefined {
  // RPC re-throws errors as generic Errors; AiryFS encodes the errno in the
  // message (e.g. "ENOENT: ..."). Fall back to a message-prefix match.
  if (err && typeof err === "object" && "message" in err) {
    const m = String((err as { message: unknown }).message);
    const match = m.match(/^([A-Z]+):/);
    return match?.[1];
  }
  return undefined;
}

/**
 * A {@link SandboxApi} backed by the AiryFS Durable Object over Workers RPC.
 * Same path plane and semantics as the HTTP adapter (`@airyfs/flue-sandbox`),
 * but each call is a direct DO-to-DO method invocation instead of a fetch.
 */
export class AiryFSRpcApi implements SandboxApi {
  constructor(private readonly stub: AiryFSRpcStub) {}

  async readFile(path: string): Promise<string> {
    return this.stub.readFile(toSdkPath(path));
  }

  async readFileBuffer(path: string): Promise<Uint8Array> {
    const stream = await this.stub.readFileStream(toSdkPath(path));
    const chunks: Uint8Array[] = [];
    const reader = stream.getReader();
    for (;;) {
      const { done, value } = await reader.read();
      if (done) break;
      if (value) chunks.push(value);
    }
    const total = chunks.reduce((n, c) => n + c.length, 0);
    const out = new Uint8Array(total);
    let offset = 0;
    for (const c of chunks) {
      out.set(c, offset);
      offset += c.length;
    }
    return out;
  }

  async writeFile(path: string, content: string | Uint8Array): Promise<void> {
    const sdkPath = toSdkPath(path);
    if (typeof content === "string") {
      await this.stub.writeFile(sdkPath, content);
      return;
    }
    const bytes = content;
    const stream = new ReadableStream<Uint8Array>({
      start(controller) {
        controller.enqueue(bytes);
        controller.close();
      },
    });
    await this.stub.writeFileStream(sdkPath, stream);
  }

  async stat(path: string): Promise<FileStat> {
    const s = await this.stub.lstatPath(toSdkPath(path));
    return {
      isFile: s.type === "file",
      isDirectory: s.type === "directory",
      isSymbolicLink: s.type === "symlink",
      size: s.size,
      mtime: new Date(s.mtime * 1000),
    };
  }

  async readdir(path: string): Promise<string[]> {
    return this.stub.listDir(toSdkPath(path));
  }

  async exists(path: string): Promise<boolean> {
    try {
      await this.stub.lstatPath(toSdkPath(path));
      return true;
    } catch (err) {
      if (rpcCode(err) === "ENOENT") return false;
      throw err;
    }
  }

  async mkdir(path: string, options?: { recursive?: boolean }): Promise<void> {
    // Recursion runs in the DO in one RPC; no client-side segment-walk.
    await this.stub.makeDir(toSdkPath(path), options?.recursive ?? false);
  }

  async rm(
    path: string,
    options?: { recursive?: boolean; force?: boolean },
  ): Promise<void> {
    // removePath is type-agnostic (file or dir); force ignores a missing path.
    // No client-side lstat-then-branch.
    try {
      await this.stub.removePath(toSdkPath(path), options?.recursive ?? false);
    } catch (err) {
      if (options?.force && rpcCode(err) === "ENOENT") return;
      throw err;
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
    const exports = Object.entries(options?.env ?? {})
      .map(([k, v]) => `export ${k}=${shellQuote(v)}; `)
      .join("");
    const cd = options?.cwd ? `cd ${shellQuote(options.cwd)} && ` : "";
    const inner = `${exports}${cd}${command}`;

    let full = inner;
    let wrappedSeconds: number | undefined;
    if (options?.timeoutMs && options.timeoutMs > 0) {
      const seconds = Math.ceil(options.timeoutMs / 1000);
      if (seconds <= EXEC_CEILING_SECONDS) {
        wrappedSeconds = seconds;
        full = `timeout ${seconds}s sh -c ${shellQuote(inner)}`;
      }
    }

    const r = await this.stub.exec(full);
    let stderr = r.stderr;
    if (wrappedSeconds !== undefined && r.exitCode === 124) {
      stderr += `\n[airyfs-rpc] command timed out after ${wrappedSeconds}s`;
    }
    return { stdout: r.stdout, stderr, exitCode: r.exitCode };
  }
}

/**
 * Build a Flue {@link SandboxFactory} backed by a cross-script AiryFS Durable
 * Object binding. Resolve the stub by volume name via the namespace's
 * `idFromName` (the same id the AiryFS Worker uses internally).
 */
export function airyfsRpc(
  namespace: RpcNamespace,
  volume?: (id: string) => string,
): SandboxFactory {
  const volumeFor = volume ?? defaultVolumeName;
  return {
    async createSessionEnv({ id }): Promise<SessionEnv> {
      const stub = namespace.get(namespace.idFromName(volumeFor(id)));
      return createSandboxSessionEnv(new AiryFSRpcApi(stub), MOUNT_ROOT);
    },
  };
}
