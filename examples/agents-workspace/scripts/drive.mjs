// Drive the deployed WorkspaceAgent against a running `wrangler dev`.
// Usage: PORT=8799 NAME=demo node scripts/drive.mjs
const PORT = process.env.PORT ?? "8799";
const NAME = process.env.NAME ?? "demo";
const BASE = `http://127.0.0.1:${PORT}/agents/workspace-agent/${NAME}`;

async function act(action, extra = {}) {
  const res = await fetch(BASE, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ action, ...extra }),
  });
  const json = await res.json();
  console.log(`\n# ${action} -> ${res.status}`);
  console.log(JSON.stringify(json, null, 1));
  return json;
}

await act("write", { path: "/notes.txt", content: "hello from the agents sdk" });
await act("read", { path: "/notes.txt" });
await act("write", { path: "/src/data.txt", content: "one two three four" });
await act("list", { path: "/src" });
await act("bash", { command: "wc -w < src/data.txt" });
await act("build", { command: "echo building && echo ok > build.log && cat build.log" });
await act("write", { path: "/index.html", content: "<h1>Served from an AiryFS volume</h1>" });
await act("publish", { path: "/" });
await act("share", { path: "/notes.txt" });
await act("job", { command: "echo durable-work-done" });
