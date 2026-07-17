// ABOUTME: Worker entrypoint and AiryFS Durable Object class.
// ABOUTME: Routes HTTP requests to named volumes, each backed by a DO with persistent SQLite.

import { Container, getContainer } from '@cloudflare/containers';
import { AgentFS, type CloudflareStorage } from 'agentfs-sdk/cloudflare';
import {
  ChunkSizeConflictError,
  configureChunkSize,
  initSchema,
  InvalidChunkSizeError,
  SCHEMA_TABLES,
} from './schema';
import { HranaServer, wrapSqlStorage } from './hrana-server';
import { MutationJournal } from './mutation-journal';
import {
  errorResponse,
  fileResponse,
  handleFilesystemRequest,
  HttpError,
  parseV1Route,
  readCommandRequest,
  readVolumeCreateRequest,
  toStatsDto,
  VolumeAccessCoordinator,
  writeFileStream,
  type StatsDto,
} from './files-api';

interface Env {
  AiryFS: DurableObjectNamespace<AiryFS>;
}

interface WorkerSocket {
  opened: Promise<unknown>;
  readable: ReadableStream<Uint8Array>;
  writable: WritableStream<Uint8Array>;
  close(): Promise<void>;
}

const STARTUP_TIMEOUT_MS = 60_000;
const CONTAINER_EXEC_TIMEOUT_MS = 310_000;
const DESTROY_TIMEOUT_MS = 10_000;

async function abortable<T>(promise: Promise<T>, signal: AbortSignal): Promise<T> {
  signal.throwIfAborted();
  return new Promise<T>((resolve, reject) => {
    const onAbort = (): void => reject(signal.reason);
    signal.addEventListener('abort', onAbort, { once: true });
    promise.then(
      (value) => {
        signal.removeEventListener('abort', onAbort);
        resolve(value);
      },
      (error) => {
        signal.removeEventListener('abort', onAbort);
        reject(error);
      }
    );
  });
}

export class AiryFS extends Container<Env> {
  defaultPort = 8080;
  sleepAfter = '30m';

  private fs: AgentFS | null = null;
  private startupPromise: Promise<void> | null = null;
  private startupAbort: AbortController | null = null;
  private destroyPromise: Promise<void> | null = null;
  private activeServePromise: Promise<void> | null = null;
  private invalidationServePromise: Promise<void> | null = null;
  private activeExec: symbol | null = null;
  private dataSocket: WorkerSocket | null = null;
  private invalidationSocket: WorkerSocket | null = null;
  private hranaServer: HranaServer | null = null;
  private readonly access = new VolumeAccessCoordinator();
  private readonly mutations: MutationJournal;

  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx as never, env);
    this.mutations = new MutationJournal(this.ctx.storage.sql);
    this.ctx.blockConcurrencyWhile(async () => {
      initSchema(this.ctx.storage.sql, (callback) => this.ctx.storage.transactionSync(callback));
    });
  }

  private filesystem(requestedChunkSize?: unknown): AgentFS {
    const configuration = this.ctx.storage.transactionSync(() =>
      configureChunkSize(this.ctx.storage.sql, requestedChunkSize)
    );
    if (!this.fs || this.fs.getChunkSize() !== configuration.chunkSize) {
      this.fs = AgentFS.create(this.ctx.storage as unknown as CloudflareStorage);
    }
    return this.fs;
  }

  private createVolume(requestedChunkSize: unknown): { chunkSize: number } {
    try {
      const fs = this.filesystem(requestedChunkSize);
      return { chunkSize: fs.getChunkSize() };
    } catch (error) {
      if (error instanceof InvalidChunkSizeError) {
        throw new HttpError(400, 'INVALID_CHUNK_SIZE', error.message);
      }
      if (error instanceof ChunkSizeConflictError) {
        throw new HttpError(409, 'CHUNK_SIZE_IMMUTABLE', error.message);
      }
      throw error;
    }
  }

  override onStop() {
    // Socket and request completion own cleanup. A delayed lifecycle callback
    // must not clear state belonging to a replacement Container generation.
  }

  // ---------------------------------------------------------------------------
  // Core API
  // ---------------------------------------------------------------------------

  /** Execute a shell command in the Container against the FUSE-mounted volume. */
  async exec(command: string, signal?: AbortSignal): Promise<unknown> {
    if (this.activeExec || this.destroyPromise) {
      throw new HttpError(503, 'EXEC_BUSY', 'Another command or Container lifecycle operation is already running');
    }
    const execution = Symbol('exec');
    this.activeExec = execution;
    try {
      await this.ensureContainer(signal);
      signal?.throwIfAborted();

      const commandSignals = [AbortSignal.timeout(
        command === ':' ? STARTUP_TIMEOUT_MS : CONTAINER_EXEC_TIMEOUT_MS
      )];
      if (command === ':' && signal) commandSignals.push(signal);
      let resp: Response;
      try {
        resp = await this.containerFetch(
          new Request('http://localhost/exec', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ command }),
            signal: AbortSignal.any(commandSignals),
          }),
          4000
        );
      } catch (error) {
        if (command !== ':') throw error;
        this.clearRuntimeConnections();
        await this.destroyBounded();
        throw new HttpError(503, 'CONTAINER_UNAVAILABLE', 'Container preflight timed out; retry startup');
      }

      if (!resp.ok) {
        const message = await resp.text();
        if (resp.status === 503 && message.includes('FUSE unavailable')) {
          this.clearRuntimeConnections();
          await this.destroyBounded();
          throw new HttpError(503, 'CONTAINER_UNAVAILABLE', message);
        }
        if (resp.status === 503) {
          throw new HttpError(503, 'EXEC_BUSY', 'Another command is already running');
        }
        throw new Error(`Container exec failed (${resp.status}): ${message}`);
      }

      return resp.json();
    } finally {
      if (this.activeExec === execution) this.activeExec = null;
    }
  }

  /** Read a file from the volume (direct DO access, no Container needed). */
  async readFile(path: string): Promise<string> {
    const fs = this.filesystem();
    const release = await this.access.acquireRead(path);
    try {
      return await fs.readFile(path, 'utf8');
    } finally {
      release();
    }
  }

  /** Write a file to the volume (direct DO access, no Container needed). */
  async writeFile(path: string, content: string): Promise<void> {
    const fs = this.filesystem();
    const release = await this.access.acquireWrite(path);
    try {
      await fs.writeFile(path, content);
      await this.mutations.record(fs, [path]);
    } finally {
      release();
    }
  }

  /** List a directory on the volume (direct DO access, no Container needed). */
  async listDir(path: string): Promise<string[]> {
    return this.filesystem().readdir(path);
  }

  /** Get serializable metadata for a file, directory, or symlink target. */
  async statPath(path: string): Promise<StatsDto> {
    return toStatsDto(await this.filesystem().stat(path));
  }

  /** List a directory with metadata in one AgentFS query. */
  async listDirDetailed(path: string): Promise<Array<{ name: string } & StatsDto>> {
    return (await this.filesystem().readdirPlus(path)).map((entry) => ({
      name: entry.name,
      ...toStatsDto(entry.stats),
    }));
  }

  async makeDir(path: string): Promise<void> {
    const fs = this.filesystem();
    const release = await this.access.acquireWrite(path);
    try {
      await fs.mkdir(path);
      await this.mutations.record(fs, [path]);
    } finally { release(); }
  }

  async removePath(path: string, recursive = false): Promise<void> {
    const fs = this.filesystem();
    const release = await this.access.acquireWrite(path);
    try {
      await fs.rm(path, { recursive });
      await this.mutations.record(fs, [path]);
    } finally { release(); }
  }

  async renamePath(from: string, to: string): Promise<void> {
    const fs = this.filesystem();
    const release = await this.access.acquireWrite([from, to]);
    try {
      await fs.rename(from, to);
      await this.mutations.record(fs, [from, to]);
    } finally { release(); }
  }

  async copyPath(from: string, to: string): Promise<void> {
    const fs = this.filesystem();
    const release = await this.access.acquireWrite([from, to]);
    try {
      await fs.copyFile(from, to);
      await this.mutations.record(fs, [to]);
    } finally { release(); }
  }

  async createSymlink(target: string, path: string): Promise<void> {
    const fs = this.filesystem();
    const release = await this.access.acquireWrite(path);
    try {
      await fs.symlink(target, path);
      await this.mutations.record(fs, [path]);
    } finally { release(); }
  }

  async readSymlink(path: string): Promise<string> {
    return this.filesystem().readlink(path);
  }

  /** Stream a binary file over Workers RPC. */
  async readFileStream(path: string): Promise<ReadableStream<Uint8Array>> {
    const response = await fileResponse(
      this.filesystem(),
      path,
      new Request('http://localhost', { method: 'GET' }),
      this.access
    );
    if (!response.body) {
      return new ReadableStream({
        type: 'bytes',
        start: (controller) => controller.close(),
      });
    }
    return response.body;
  }

  /** Atomically replace a file from a byte stream over Workers RPC. */
  async writeFileStream(path: string, stream: ReadableStream<Uint8Array>): Promise<void> {
    const fs = this.filesystem();
    await writeFileStream(fs, path, stream, this.access);
    await this.mutations.record(fs, [path]);
  }

  /** Get volume metadata: table row counts. */
  dbInfo(): Record<string, number> {
    const result: Record<string, number> = {};
    for (const table of SCHEMA_TABLES) {
      try {
        const row = this.ctx.storage.sql
          .exec<{ count: number }>(`SELECT count(*) as count FROM ${table}`)
          .one();
        result[table] = row.count;
      } catch {
        // Table may not exist if AgentFS.create() hasn't run yet
      }
    }
    return result;
  }

  /** Return logical filesystem usage, physical SQLite size, and runtime health. */
  async usage(): Promise<Record<string, unknown>> {
    const filesystem = await this.filesystem().statfs();
    const runtimeState = await this.getState();
    let container: Record<string, unknown> = {
      state: runtimeState.status,
      hranaConnected: Boolean(this.activeServePromise),
    };

    if (runtimeState.status === 'healthy' && this.activeServePromise && !this.destroyPromise) {
      try {
        const response = await this.ctx.container!.getTcpPort(4000).fetch(
          new Request('http://localhost/health', { signal: AbortSignal.timeout(5_000) }),
        );
        container = {
          state: runtimeState.status,
          hranaConnected: true,
          ...await response.json<Record<string, unknown>>(),
        };
      } catch (error) {
        container = {
          state: runtimeState.status,
          hranaConnected: Boolean(this.activeServePromise),
          health: 'unhealthy',
          error: error instanceof Error ? error.message : String(error),
        };
      }
    }

    return {
      filesystem,
      sqliteBytes: this.ctx.storage.sql.databaseSize,
      container,
      hrana: {
        pipelineRequests: this.hranaServer?.pipelineCount ?? 0,
        sqlStatements: this.hranaServer?.statementCount ?? 0,
      },
    };
  }

  /** Destroy the Container. Volume data persists in DO SQLite. */
  async destroyContainer(): Promise<void> {
    if (this.destroyPromise) return this.destroyPromise;
    const operation = (async (): Promise<void> => {
      this.startupAbort?.abort();
      await this.startupPromise?.catch(() => undefined);
      await this.destroyBounded();
      this.clearRuntimeState();
    })();
    this.destroyPromise = operation;
    try {
      await operation;
    } finally {
      if (this.destroyPromise === operation) this.destroyPromise = null;
    }
  }

  // ---------------------------------------------------------------------------
  // Container lifecycle
  // ---------------------------------------------------------------------------

  /**
   * Start the Container and set up bridge + FUSE mount via HTTP calls.
   *
   * The DO drives the sequence:
   *   1. Container starts with command-server.js (port 4000)
   *   2. POST /setup → bridge starts in-process (ports 9000 + 8080)
   *   3. DO connects TCP to :9000, starts HranaServer
   *   4. POST /mount → agentfs FUSE daemon at /volume
   *
   * If the Hrana TCP connection drops (container eviction, crash),
   * activeServePromise resets to null so the next call reconnects.
   */
  private async ensureContainer(requestSignal?: AbortSignal): Promise<void> {
    this.filesystem();
    if (this.startupPromise) return this.startupPromise;

    const startupAbort = new AbortController();
    const signals = [startupAbort.signal, AbortSignal.timeout(STARTUP_TIMEOUT_MS)];
    if (requestSignal) signals.push(requestSignal);
    const signal = AbortSignal.any(signals);
    const startup = this.prepareContainer(signal);
    this.startupAbort = startupAbort;
    this.startupPromise = startup;
    try {
      await startup;
    } catch (error) {
      startupAbort.abort();
      this.closeRuntimeSockets();
      // An explicit destroy owns cleanup after it has waited for this startup.
      const alreadyRecycled = error instanceof HttpError && error.code === 'CONTAINER_RECYCLED';
      if (!alreadyRecycled && !this.destroyPromise) await this.destroyBounded();
      throw error;
    } finally {
      if (this.startupPromise === startup) this.startupPromise = null;
      if (this.startupAbort === startupAbort) this.startupAbort = null;
    }
  }

  private async prepareContainer(signal: AbortSignal): Promise<void> {
    const state = await this.getState();
    if (state.status === 'stopped' || state.status === 'stopped_with_code') {
      await this.startContainer(signal);
      return;
    }
    if (state.status === 'stopping') {
      await this.destroyBounded();
      this.clearRuntimeConnections();
      throw new HttpError(
        503,
        'CONTAINER_RECYCLED',
        'Recycled an orphaned Container; retry startup',
        { 'Retry-After': '1' }
      );
    }

    await this.setupBridge(signal);
    await this.connectData(signal);
    await this.connectInvalidation(signal);
    try {
      const response = await this.containerFetch(
        new Request('http://localhost/health', { signal }),
        4000
      );
      const health = await response.json<{ fuseMounted?: boolean }>();
      if (health.fuseMounted) return;
    } catch {
      // Recycle below. A healthy Container without a usable mount cannot serve exec.
    }
    this.clearRuntimeConnections();
    await this.destroyBounded();
    throw new HttpError(
      503,
      'CONTAINER_RECYCLED',
      'Recycled a Container with an unavailable FUSE mount; retry startup',
      { 'Retry-After': '1' }
    );
  }

  private async startContainer(signal: AbortSignal): Promise<void> {

    this.entrypoint = ['node', '/app/dist/command-server.js'];

    // 1. Wait for command server
    await this.startAndWaitForPorts({
      ports: [4000],
      cancellationOptions: {
        abort: signal,
        instanceGetTimeoutMS: STARTUP_TIMEOUT_MS,
        portReadyTimeoutMS: STARTUP_TIMEOUT_MS,
      },
    });

    // 2. Start bridge in-process
    await this.setupBridge(signal);

    // 3. Connect FUSE data and invalidation traffic.
    const { socket, servePromise } = await this.connectData(signal);
    await this.connectInvalidation(signal);

    // 4. Mount FUSE — fire-and-forget, then poll for readiness.
    // Can't await because the mount blocks while FUSE queries flow through serve().
    this.containerFetch(
      new Request('http://localhost/mount', { method: 'POST', signal }),
      4000
    ).catch(() => {
      // Mount request itself failing is not fatal — the health poll below
      // will detect whether the mount actually succeeded.
    });

    // Poll until FUSE is mounted or we give up. The health endpoint checks
    // mountpoint -q and reports fuseExitCode if the daemon crashed.
    let mounted = false;
    let mountError: Error | null = null;
    for (let i = 0; i < 30; i++) {
      await abortable(new Promise<void>((resolve) => setTimeout(resolve, 1000)), signal);
      try {
        const healthResp = await this.containerFetch(
          new Request('http://localhost/health', { signal }),
          4000
        );
        const health = (await healthResp.json()) as {
          fuseMounted?: boolean;
          fuseExitCode?: number | null;
        };
        if (health.fuseMounted) {
          mounted = true;
          break;
        }
        // If daemon exited, stop polling
        if (health.fuseExitCode !== null && health.fuseExitCode !== undefined) {
          mountError = new Error(`FUSE daemon exited with code ${health.fuseExitCode}`);
          break;
        }
      } catch (err) {
        // Container not ready yet — keep polling
      }
    }

    if (!mounted) {
      await socket.close().catch(() => undefined);
      await this.invalidationSocket?.close().catch(() => undefined);
      if (this.activeServePromise === servePromise) this.activeServePromise = null;
      throw mountError ?? new Error('FUSE mount did not complete within 30 seconds');
    }
  }

  private async setupBridge(signal: AbortSignal): Promise<void> {
    const setupResp = await this.containerFetch(
      new Request('http://localhost/setup', { method: 'POST', signal }),
      4000
    );
    if (!setupResp.ok) {
      throw new Error(`Bridge setup failed (${setupResp.status}): ${await setupResp.text()}`);
    }
    const setupResult = (await setupResp.json()) as { ok: boolean; error?: string };
    if (!setupResult.ok) {
      throw new Error(`Bridge setup failed: ${setupResult.error}`);
    }
  }

  private async connectData(signal: AbortSignal): Promise<{
    socket: WorkerSocket;
    servePromise: Promise<void>;
  }> {
    const previousSocket = this.dataSocket;
    const socket = this.ctx.container!.getTcpPort(9000).connect('0.0.0.0:9000');
    this.dataSocket = socket;
    try {
      await abortable(socket.opened, signal);
    } catch (error) {
      if (this.dataSocket === socket) this.dataSocket = null;
      await socket.close().catch(() => undefined);
      throw error;
    }
    await previousSocket?.close().catch(() => undefined);

    this.hranaServer = new HranaServer({
      readable: socket.readable,
      writable: socket.writable,
      sql: wrapSqlStorage(this.ctx.storage.sql),
      writeLock: () => this.access.acquireWrite('*'),
    });

    const servePromise = this.hranaServer.serve();
    this.activeServePromise = servePromise;
    this.ctx.waitUntil(servePromise);
    const clearServePromise = (): void => {
      if (this.activeServePromise === servePromise) {
        this.activeServePromise = null;
        if (this.dataSocket === socket) this.dataSocket = null;
      }
    };
    void servePromise.then(clearServePromise, clearServePromise);
    return { socket, servePromise };
  }

  private async connectInvalidation(signal: AbortSignal): Promise<void> {
    if (this.invalidationServePromise) return;
    const socket = this.ctx.container!.getTcpPort(9001).connect('0.0.0.0:9001');
    this.invalidationSocket = socket;
    try {
      await abortable(socket.opened, signal);
    } catch (error) {
      if (this.invalidationSocket === socket) this.invalidationSocket = null;
      await socket.close().catch(() => undefined);
      throw error;
    }
    const server = new HranaServer({
      readable: socket.readable,
      writable: socket.writable,
      sql: wrapSqlStorage(this.ctx.storage.sql),
    });
    const servePromise = server.serve();
    this.invalidationServePromise = servePromise;
    this.ctx.waitUntil(servePromise);
    const clear = (): void => {
      if (this.invalidationServePromise === servePromise) {
        this.invalidationServePromise = null;
        if (this.invalidationSocket === socket) this.invalidationSocket = null;
      }
    };
    void servePromise.then(clear, clear);
  }

  private closeRuntimeSockets(): void {
    const sockets = [this.dataSocket, this.invalidationSocket];
    this.dataSocket = null;
    this.invalidationSocket = null;
    for (const socket of sockets) void socket?.close().catch(() => undefined);
  }

  private clearRuntimeConnections(): void {
    this.activeServePromise = null;
    this.invalidationServePromise = null;
    this.hranaServer = null;
    this.closeRuntimeSockets();
  }

  private clearRuntimeState(): void {
    this.startupAbort?.abort();
    this.startupAbort = null;
    this.startupPromise = null;
    this.activeExec = null;
    this.clearRuntimeConnections();
  }

  private async destroyBounded(): Promise<void> {
    try {
      await abortable(this.destroy(), AbortSignal.timeout(DESTROY_TIMEOUT_MS));
    } catch (error) {
      this.ctx.abort('Container destruction did not complete before its deadline');
      throw error;
    }
  }

  // ---------------------------------------------------------------------------
  // HTTP routing
  // ---------------------------------------------------------------------------

  override async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url);

    try {
      const v1Route = parseV1Route(url.pathname);
      if (v1Route) {
        if (v1Route.resource === 'volume') {
          if (request.method === 'GET') {
            return Response.json({ chunkSize: this.filesystem().getChunkSize() });
          }
          if (request.method === 'PUT') {
            return Response.json(this.createVolume(await readVolumeCreateRequest(request)), { status: 201 });
          }
          throw new HttpError(405, 'METHOD_NOT_ALLOWED', 'Method not allowed', { Allow: 'GET, PUT' });
        }

        const fs = this.filesystem();
        const filesystemResponse = await handleFilesystemRequest(
          request,
          v1Route,
          fs,
          this.access,
          (paths) => this.mutations.record(fs, paths)
        );
        if (filesystemResponse) return filesystemResponse;

        if (v1Route.resource === 'exec' && request.method === 'POST') {
          return Response.json(await this.exec(await readCommandRequest(request), request.signal));
        }

        if (v1Route.resource === 'usage' && request.method === 'GET') {
          return Response.json(await this.usage());
        }

        const allow = v1Route.resource === 'exec' ? 'POST' : 'GET';
        throw new HttpError(405, 'METHOD_NOT_ALLOWED', 'Method not allowed', { Allow: allow });
      }

      if (url.pathname === '/exec' && request.method === 'POST') {
        return Response.json(await this.exec(await readCommandRequest(request), request.signal));
      }

      if (url.pathname === '/destroy' && request.method === 'POST') {
        await this.destroyContainer();
        return new Response('ok');
      }

      if (url.pathname === '/fs/write' && request.method === 'POST') {
        const path = url.searchParams.get('path');
        if (!path) return new Response('Missing ?path=', { status: 400 });
        await writeFileStream(this.filesystem(), path, request.body, this.access);
        await this.mutations.record(this.filesystem(), [path]);
        return new Response('ok');
      }

      if (url.pathname === '/fs/read') {
        const path = url.searchParams.get('path');
        if (!path) return new Response('Missing ?path=', { status: 400 });
        return fileResponse(this.filesystem(), path, request, this.access);
      }

      if (url.pathname === '/fs/ls') {
        const path = url.searchParams.get('path') ?? '/';
        return Response.json(await this.listDir(path));
      }

      if (url.pathname === '/kv/set' && request.method === 'POST') {
        const key = url.searchParams.get('key');
        if (!key) return new Response('Missing ?key=', { status: 400 });
        const value = await request.text();
        this.ctx.storage.sql.exec(
          "INSERT OR REPLACE INTO kv_store (key, value, created_at, updated_at) VALUES (?, ?, unixepoch(), unixepoch())",
          key,
          value
        );
        return new Response('ok');
      }

      if (url.pathname === '/kv/get') {
        const key = url.searchParams.get('key');
        if (!key) return new Response('Missing ?key=', { status: 400 });
        const rows = this.ctx.storage.sql
          .exec<{ value: string }>("SELECT value FROM kv_store WHERE key = ?", key)
          .toArray();
        if (rows.length === 0) return new Response('Not found', { status: 404 });
        return new Response(rows[0].value);
      }

      if (url.pathname === '/perf') {
        return Response.json({
          pipelineRequests: this.hranaServer?.pipelineCount ?? 0,
          sqlStatements: this.hranaServer?.statementCount ?? 0,
        });
      }

      if (url.pathname === '/db-info') {
        return Response.json(this.dbInfo());
      }

      if (url.pathname === '/usage') {
        return Response.json(await this.usage());
      }

      return new Response('Not found', { status: 404 });
    } catch (error) {
      return errorResponse(error);
    }
  }
}

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const url = new URL(request.url);
    let volume: string | null;
    try {
      volume = parseV1Route(url.pathname)?.volume ?? url.searchParams.get('volume');
    } catch (error) {
      if (error instanceof URIError) {
        return Response.json(
          { error: { code: 'INVALID_PATH', message: 'Path contains invalid URL encoding' } },
          { status: 400 }
        );
      }
      return errorResponse(error);
    }

    if (!volume) {
      return new Response('Missing ?volume= parameter', { status: 400 });
    }

    const stub = getContainer<AiryFS>(env.AiryFS, volume);
    return stub.fetch(request);
  },
};
