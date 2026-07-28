import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { AiryFSClient, AiryFSApiError } from "airyfs-sdk";
import { createSandboxSessionEnv } from "@flue/runtime";
import type { SessionEnv } from "@flue/runtime";
import { AiryFSSandboxApi } from "../src/adapter.js";
import { toSdkPath, defaultVolumeName } from "../src/paths.js";

const ENDPOINT =
  process.env.AIRYFS_ENDPOINT ?? "https://airyfs-int.tyson-s-sandbox.workers.dev";
const TOKEN = process.env.AIRYFS_TOKEN;

// Track volumes we create so we can tear them down.
const createdVolumes: AiryFSClient[] = [];

function freshVolumeName(tag: string): string {
  const rand = Math.random().toString(36).slice(2, 8);
  return `flue-it-${tag}-${Date.now().toString(36)}-${rand}`;
}

function makeHarness(tag: string): {
  client: AiryFSClient;
  api: AiryFSSandboxApi;
  env: SessionEnv;
  volume: string;
} {
  const volume = freshVolumeName(tag);
  const client = new AiryFSClient(ENDPOINT, volume, { token: TOKEN });
  createdVolumes.push(client);
  const api = new AiryFSSandboxApi(client);
  // Same wiring Flue uses: base cwd is the /volume mount root.
  const env = createSandboxSessionEnv(api, "/volume");
  return { client, api, env, volume };
}

afterAll(async () => {
  await Promise.allSettled(createdVolumes.map((c) => c.deleteVolume()));
});

describe("path plane", () => {
  it("strips the /volume prefix to volume-rooted SDK paths", () => {
    expect(toSdkPath("/volume")).toBe("/");
    expect(toSdkPath("/volume/")).toBe("/");
    expect(toSdkPath("/volume/src/main.py")).toBe("/src/main.py");
    expect(toSdkPath("/src/main.py")).toBe("/src/main.py");
    expect(toSdkPath("rel.txt")).toBe("/rel.txt");
  });

  it("derives collision-resistant, valid volume names", () => {
    const a = defaultVolumeName("user/123 ABC");
    const b = defaultVolumeName("user-123-abc");
    expect(a).toMatch(/^agent-[a-z0-9-]+-[0-9a-f]{8}$/);
    // Distinct ids that slug identically must not collide.
    expect(defaultVolumeName("A_B")).not.toBe(defaultVolumeName("a-b"));
    expect(a).not.toBe(b);
  });
});

describe("direct-path filesystem methods (no Container)", () => {
  let h: ReturnType<typeof makeHarness>;
  beforeAll(() => {
    h = makeHarness("fs");
  });

  it("writes and reads back UTF-8 text", async () => {
    await h.api.writeFile("/volume/hello.txt", "hello world");
    expect(await h.api.readFile("/volume/hello.txt")).toBe("hello world");
  });

  it("round-trips binary content byte-for-byte", async () => {
    const bytes = new Uint8Array([0, 1, 2, 253, 254, 255, 0x89, 0x50]);
    await h.api.writeFile("/volume/blob.bin", bytes);
    const back = await h.api.readFileBuffer("/volume/blob.bin");
    expect(Array.from(back)).toEqual(Array.from(bytes));
  });

  it("stat returns honest POSIX metadata (real size + Date mtime)", async () => {
    const body = "0123456789";
    await h.api.writeFile("/volume/sized.txt", body);
    const s = await h.api.stat("/volume/sized.txt");
    expect(s.isFile).toBe(true);
    expect(s.isDirectory).toBe(false);
    expect(s.size).toBe(body.length);
    expect(s.mtime).toBeInstanceOf(Date);
    // mtime is derived from Unix *seconds*; sanity-check it's a recent, valid date.
    expect(s.mtime!.getTime()).toBeGreaterThan(Date.now() - 10 * 60_000);
    expect(Number.isNaN(s.mtime!.getTime())).toBe(false);
  });

  it("readdir returns entry names only", async () => {
    await h.api.mkdir("/volume/listme");
    await h.api.writeFile("/volume/listme/a.txt", "a");
    await h.api.writeFile("/volume/listme/b.txt", "b");
    const names = await h.api.readdir("/volume/listme");
    expect(names.sort()).toEqual(["a.txt", "b.txt"]);
  });

  it("exists reports true/false without throwing", async () => {
    await h.api.writeFile("/volume/present.txt", "x");
    expect(await h.api.exists("/volume/present.txt")).toBe(true);
    expect(await h.api.exists("/volume/nope-does-not-exist")).toBe(false);
  });

  it("mkdir recursive creates nested directories and is idempotent on dirs", async () => {
    await h.api.mkdir("/volume/a/b/c", { recursive: true });
    expect((await h.api.stat("/volume/a/b/c")).isDirectory).toBe(true);
    // Re-running over existing directories must not throw (EEXIST tolerated).
    await h.api.mkdir("/volume/a/b/c", { recursive: true });
  });

  it("mkdir recursive propagates when a file occupies a segment", async () => {
    await h.api.writeFile("/volume/afile", "not a dir");
    await expect(
      h.api.mkdir("/volume/afile/child", { recursive: true }),
    ).rejects.toBeInstanceOf(AiryFSApiError);
  });

  it("rm deletes files and directories; force ignores missing", async () => {
    await h.api.writeFile("/volume/rmme.txt", "x");
    await h.api.rm("/volume/rmme.txt");
    expect(await h.api.exists("/volume/rmme.txt")).toBe(false);

    await h.api.mkdir("/volume/rmdir/inner", { recursive: true });
    await h.api.writeFile("/volume/rmdir/inner/f.txt", "x");
    await h.api.rm("/volume/rmdir", { recursive: true });
    expect(await h.api.exists("/volume/rmdir")).toBe(false);

    // force: missing path is a no-op.
    await h.api.rm("/volume/never-existed", { force: true });
    // without force: missing path throws ENOENT.
    await expect(h.api.rm("/volume/never-existed")).rejects.toMatchObject({
      code: "ENOENT",
    });
  });

  it("surfaces ENOENT as a typed AiryFSApiError for missing reads", async () => {
    await expect(h.api.readFile("/volume/missing.txt")).rejects.toMatchObject({
      code: "ENOENT",
    });
  });
});

describe("path-plane consistency across file ops and exec", () => {
  it("a file written at /volume/x is readable by cat /volume/x AND cat x", async () => {
    const h = makeHarness("plane");
    await h.env.writeFile("/volume/plane.txt", "plane-ok");

    const abs = await h.env.exec("cat /volume/plane.txt");
    expect(abs.exitCode).toBe(0);
    expect(abs.stdout.trim()).toBe("plane-ok");

    // Default exec cwd is /volume (the mount), so the relative path resolves
    // to the same file — one namespace, no two conventions.
    const rel = await h.env.exec("cat plane.txt");
    expect(rel.exitCode).toBe(0);
    expect(rel.stdout.trim()).toBe("plane-ok");
  });

  it("FlueFs.writeFile into a missing directory drives the adapter mkdir-and-retry loop", async () => {
    const h = makeHarness("retry");
    // The parent dirs do not exist; createSandboxSessionEnv must catch the
    // ENOENT, call adapter.mkdir(parent,{recursive:true}), and retry the write.
    await h.env.writeFile("/volume/deep/nested/dir/file.txt", "made it");
    expect(await h.env.readFile("/volume/deep/nested/dir/file.txt")).toBe("made it");
    expect((await h.env.stat("/volume/deep/nested/dir")).isDirectory).toBe(true);
  });
});

describe("exec semantics", () => {
  it("honors cwd and env in the /volume plane", async () => {
    const h = makeHarness("execenv");
    await h.api.mkdir("/volume/work", { recursive: true });
    const r = await h.api.exec('echo "$GREETING from $(pwd)"', {
      cwd: "/volume/work",
      env: { GREETING: "hi" },
    });
    expect(r.exitCode).toBe(0);
    expect(r.stdout.trim()).toBe("hi from /volume/work");
  });

  it("enforces timeoutMs <= 300s with exit 124 and a stderr note", async () => {
    const h = makeHarness("timeout");
    const r = await h.api.exec("sleep 5", { timeoutMs: 1000 });
    expect(r.exitCode).toBe(124);
    expect(r.stderr).toContain("timed out after 1s");
  });

  it("durable exec queues concurrent commands instead of failing EXEC_BUSY", async () => {
    const h = makeHarness("durable");
    // Two overlapping durable execs on one volume. The single execution slot
    // serializes them; neither should reject with EXEC_BUSY.
    const [a, b] = await Promise.all([
      h.api.exec("echo first"),
      h.api.exec("echo second"),
    ]);
    expect(a.exitCode).toBe(0);
    expect(b.exitCode).toBe(0);
    expect([a.stdout.trim(), b.stdout.trim()].sort()).toEqual(["first", "second"]);
  });
});
