import { describe, it, expect, beforeAll, afterAll } from "vitest";
import { connect } from "@tursodatabase/serverless";
import { createServerlessAdapter } from "../src/integrations/serverless/index.js";
import { AgentFS } from "../src/index_node.js";

const SQLD_URL = process.env.SQLD_URL || "http://localhost:8080";

// Check if sqld is reachable before running tests
async function isSqldAvailable(): Promise<boolean> {
  try {
    const resp = await fetch(`${SQLD_URL}/version`);
    return resp.ok;
  } catch {
    return false;
  }
}

const sqldAvailable = await isSqldAvailable();

describe.skipIf(!sqldAvailable)("Serverless Integration", () => {
  let agent: Awaited<ReturnType<typeof AgentFS.openWith>>;

  beforeAll(async () => {
    const conn = connect({ url: SQLD_URL });
    const db = createServerlessAdapter(conn);
    agent = await AgentFS.openWith(db);
  });

  afterAll(async () => {
    try {
      await agent.fs.rm("/test-serverless", { recursive: true, force: true });
    } catch {}
    await agent.close();
  });

  it("should write and read a file", async () => {
    await agent.fs.writeFile("/test-serverless/hello.txt", "Hello from serverless!");
    const content = await agent.fs.readFile("/test-serverless/hello.txt", "utf8");
    expect(content).toBe("Hello from serverless!");
  });

  it("should list directory contents", async () => {
    await agent.fs.writeFile("/test-serverless/a.txt", "a");
    await agent.fs.writeFile("/test-serverless/b.txt", "b");
    const entries = await agent.fs.readdir("/test-serverless");
    expect(entries).toContain("a.txt");
    expect(entries).toContain("b.txt");
  });

  it("should stat a file", async () => {
    await agent.fs.writeFile("/test-serverless/stat-test.txt", "test content");
    const stats = await agent.fs.stat("/test-serverless/stat-test.txt");
    expect(stats.isFile()).toBe(true);
    expect(stats.isDirectory()).toBe(false);
    expect(stats.size).toBe(12);
  });

  it("should use kv store", async () => {
    await agent.kv.set("test-key", { value: 42 });
    const result = await agent.kv.get("test-key");
    expect(result).toEqual({ value: 42 });
  });

  it("should delete a file", async () => {
    await agent.fs.writeFile("/test-serverless/delete-me.txt", "bye");
    await agent.fs.unlink("/test-serverless/delete-me.txt");
    await expect(agent.fs.stat("/test-serverless/delete-me.txt")).rejects.toThrow();
  });
});
