import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { AiryFSClient } from "airyfs-sdk";
import { AiryFSWorkspace } from "../src/workspace.js";
import { createWorkspaceTools, type WorkspaceTool } from "../src/tools.js";

const ENDPOINT =
  process.env.AIRYFS_ENDPOINT ?? "https://airyfs-int.tyson-s-sandbox.workers.dev";
const TOKEN = process.env.AIRYFS_TOKEN;

const cleanup: AiryFSClient[] = [];
function freshVolume(tag: string): string {
  return `agtk-it-${tag}-${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 7)}`;
}
function makeWorkspace(tag: string, opts = {}): AiryFSWorkspace {
  const ws = new AiryFSWorkspace(ENDPOINT, freshVolume(tag), { token: TOKEN, ...opts });
  cleanup.push(ws.client);
  return ws;
}
async function run<S extends WorkspaceTool>(tool: S, args: unknown): Promise<unknown> {
  return tool.execute(args as never);
}

afterAll(async () => {
  await Promise.allSettled(cleanup.map((c) => c.deleteVolume()));
});

describe("AiryFSWorkspace direct-path methods", () => {
  let ws: AiryFSWorkspace;
  beforeAll(() => {
    ws = makeWorkspace("fs");
  });

  it("write/read round-trip and list", async () => {
    await ws.writeFile("/a.txt", "alpha");
    await ws.writeFile("/dir/b.txt", "beta");
    expect(await ws.readFile("/a.txt")).toBe("alpha");
    const listing = await ws.listDir("/dir");
    expect(listing.map((e) => e.name)).toContain("b.txt");
    expect(listing.find((e) => e.name === "b.txt")?.type).toBe("file");
  });

  it("exists is boolean, never throws for missing", async () => {
    expect(await ws.exists("/a.txt")).toBe(true);
    expect(await ws.exists("/nope")).toBe(false);
  });
});

describe("tool descriptors execute end to end", () => {
  it("read_file / write_file / list_dir tools operate on the volume", async () => {
    const ws = makeWorkspace("tools");
    const tools = createWorkspaceTools(ws);

    const w = (await run(tools.write_file!, { path: "/hello.txt", content: "hi there" })) as {
      ok: boolean;
    };
    expect(w.ok).toBe(true);
    expect(await run(tools.read_file!, { path: "/hello.txt" })).toBe("hi there");

    const entries = (await run(tools.list_dir!, { path: "/" })) as Array<{ name: string }>;
    expect(entries.map((e) => e.name)).toContain("hello.txt");
  });

  it("bash tool runs in the Container at /volume and sees written files", async () => {
    const ws = makeWorkspace("bash");
    const tools = createWorkspaceTools(ws, { includeBash: true });
    await ws.writeFile("/data.txt", "one two three");
    const r = (await run(tools.bash!, { command: "wc -w < data.txt" })) as {
      exitCode: number;
      stdout: string;
    };
    expect(r.exitCode).toBe(0);
    expect(r.stdout.trim()).toBe("3");
  });

  it("bash timeout yields exit 124 with a note", async () => {
    const ws = makeWorkspace("bto", { snapshotBeforeBash: false });
    const r = await ws.bash("sleep 5", { timeoutMs: 1000 });
    expect(r.exitCode).toBe(124);
    expect(r.stderr).toContain("timed out");
  });
});

describe("bounded snapshot retention", () => {
  it("keeps at most N pre-exec guard snapshots", async () => {
    const ws = makeWorkspace("snap", { snapshotRetention: 2 });
    await ws.writeFile("/x", "x");
    // 4 bash calls => 4 guard snapshots created, but retention prunes to 2.
    for (let i = 0; i < 4; i++) await ws.bash(`echo run-${i}`);
    const snaps = (await ws.client.listSnapshots()).filter((s) =>
      (s.name ?? "").startsWith("pre-exec-"),
    );
    expect(snaps.length).toBeLessThanOrEqual(2);
  });
});

describe("durable jobs", () => {
  it("submits a job and waits for completion", async () => {
    const ws = makeWorkspace("job");
    const job = await ws.runJob("echo durable-hello");
    expect(job.id).toBeTruthy();
    const done = await ws.waitJob(job.id, { timeoutMs: 120_000 });
    expect(done.status).toBe("succeeded");
    expect(done.exitCode).toBe(0);
  });
});

describe("artifacts served from the volume", () => {
  it("publishes a static site reachable at its public URL", async () => {
    const ws = makeWorkspace("site");
    await ws.writeFile("/index.html", "<h1>airyfs site ok</h1>");
    const { url } = await ws.publishSite({ path: "/" });
    expect(url).toMatch(/\/s\/agtk-it-site-/);
    const res = await fetch(url);
    expect(res.status).toBe(200);
    expect(await res.text()).toContain("airyfs site ok");
  });

  it("mints a share link that serves the file", async () => {
    const ws = makeWorkspace("share");
    await ws.writeFile("/report.txt", "shared-content-123");
    const { id, url } = await ws.shareLink("/report.txt");
    expect(id).toBeTruthy();
    expect(url).toMatch(/\/d\/agtk-it-share-/);
    const res = await fetch(url);
    expect(res.status).toBe(200);
    expect(await res.text()).toContain("shared-content-123");
  });
});
