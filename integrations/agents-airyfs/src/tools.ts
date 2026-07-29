import { z } from "zod";
import type { AiryFSWorkspace } from "./workspace.js";

/**
 * A framework-neutral tool descriptor. The shape (`description` +
 * `parameters` zod schema + `execute`) matches what the Vercel AI SDK's
 * `tool()` helper accepts, so a consumer can adapt it with one line:
 *
 * ```ts
 * import { tool } from "ai";
 * const aiTools = Object.fromEntries(
 *   Object.entries(createWorkspaceTools(ws)).map(([n, t]) => [n, tool(t)]),
 * );
 * ```
 *
 * Keeping the toolkit free of an `ai`/`agents` dependency avoids their peer
 * version churn; the consumer supplies the exact `tool()` for their version.
 */
export interface WorkspaceTool<Schema extends z.ZodTypeAny = z.ZodTypeAny> {
  description: string;
  parameters: Schema;
  execute: (args: z.infer<Schema>) => Promise<unknown>;
}

export interface WorkspaceToolsOptions {
  /** Include the Container-backed `bash` tool. Default true. */
  includeBash?: boolean;
  /** Include durable-job tools (`run_job`, `job_status`). Default true. */
  includeJobs?: boolean;
  /** Include artifact tools (`publish_site`, `share_link`). Default false. */
  includeArtifacts?: boolean;
  /** Default per-command timeout applied by the `bash` tool, in ms. */
  bashTimeoutMs?: number;
}

/**
 * Build the AiryFS workspace tools for an agent. Reads/writes/listing hit the
 * direct path; `bash` attaches the Container.
 */
export function createWorkspaceTools(
  ws: AiryFSWorkspace,
  options: WorkspaceToolsOptions = {},
): Record<string, WorkspaceTool> {
  const includeBash = options.includeBash ?? true;
  const includeJobs = options.includeJobs ?? true;
  const includeArtifacts = options.includeArtifacts ?? false;

  const tools: Record<string, WorkspaceTool> = {
    read_file: {
      description:
        "Read a UTF-8 text file from the durable workspace. Paths are rooted at the volume, e.g. /src/main.py.",
      parameters: z.object({ path: z.string().describe("Volume-rooted file path") }),
      execute: async ({ path }) => ws.readFile(path),
    },
    write_file: {
      description:
        "Write a UTF-8 text file to the durable workspace, replacing any existing content.",
      parameters: z.object({
        path: z.string().describe("Volume-rooted file path"),
        content: z.string(),
      }),
      execute: async ({ path, content }) => {
        await ws.writeFile(path, content);
        return { ok: true, path };
      },
    },
    list_dir: {
      description: "List the entries (name, type, size) of a directory in the workspace.",
      parameters: z.object({ path: z.string().default("/") }),
      execute: async ({ path }) => ws.listDir(path),
    },
    read_lines: {
      description:
        "Read a line-addressed slice of a text file (fast, no container). Use mode 'head' (first count), " +
        "'tail' (last count), or 'range' (1-based inclusive start..end). Prefer this over reading whole files.",
      parameters: z.object({
        path: z.string(),
        mode: z.enum(["head", "tail", "range"]).default("head"),
        count: z.number().int().positive().optional(),
        start: z.number().int().positive().optional(),
        end: z.number().int().positive().optional(),
      }),
      execute: async ({ path, mode, count, start, end }) =>
        ws.readLines(path, { mode, count, start, end }),
    },
    line_stats: {
      description: "Count the lines, words, and bytes of a text file (fast, no container).",
      parameters: z.object({ path: z.string() }),
      execute: async ({ path }) => ws.lineStats(path),
    },
    json_query: {
      description:
        "Extract a value from a JSON file by JSONPath (e.g. $.items[0].name), fast and no container. " +
        "JSONPath subset only (not a jq/query language). Returns { value, type, found }.",
      parameters: z.object({ path: z.string(), query: z.string() }),
      execute: async ({ path, query }) => ws.jsonQuery(path, query),
    },
    replace_text: {
      description:
        "Find/replace text in a file (server-side, atomic). Global by default. Runs as a dry run " +
        "unless dryRun is false — inspect the returned match count first, then re-run with dryRun:false to write.",
      parameters: z.object({
        path: z.string(),
        pattern: z.string(),
        replacement: z.string(),
        ignoreCase: z.boolean().default(false),
        literal: z.boolean().default(false),
        // Model-facing default is a preview: the risk is a regex matching more than intended.
        dryRun: z.boolean().default(true),
      }),
      execute: async ({ path, pattern, replacement, ignoreCase, literal, dryRun }) =>
        ws.replaceText(path, pattern, replacement, { ignoreCase, literal, dryRun }),
    },
  };

  if (includeBash) {
    tools.bash = {
      description:
        "Run a shell command in the workspace Container (real Linux, cwd is /volume, ~300s max). " +
        "A recoverable snapshot is taken before the command runs. Prefer read_file/list_dir for inspection.",
      parameters: z.object({ command: z.string() }),
      execute: async ({ command }) =>
        ws.bash(command, { timeoutMs: options.bashTimeoutMs }),
    };
  }

  if (includeJobs) {
    tools.run_job = {
      description:
        "Submit a durable background job for work that may exceed the ~300s exec limit. " +
        "Returns a job id immediately; poll with job_status. cwd is volume-rooted (/ is the root).",
      parameters: z.object({ command: z.string(), cwd: z.string().default("/") }),
      execute: async ({ command, cwd }) => {
        const job = await ws.runJob(command, cwd);
        return { id: job.id, status: job.status };
      },
    };
    tools.job_status = {
      description: "Get the status, exit code, and error (if any) of a durable job by id.",
      parameters: z.object({ id: z.string() }),
      execute: async ({ id }) => {
        const job = await ws.getJob(id);
        return { id: job.id, status: job.status, exitCode: job.exitCode, error: job.error };
      },
    };
  }

  if (includeArtifacts) {
    tools.publish_site = {
      description:
        "Publish the workspace (or a subtree) as a public static website. Returns the URL.",
      parameters: z.object({
        path: z.string().default("/"),
        spa: z.boolean().default(false),
      }),
      execute: async ({ path, spa }) => ws.publishSite({ path, spa }),
    };
    tools.share_link = {
      description: "Create a public (optionally expiring) share link for one file. Returns the URL.",
      parameters: z.object({
        path: z.string(),
        expiresInSeconds: z.number().int().positive().optional(),
      }),
      execute: async ({ path, expiresInSeconds }) => ws.shareLink(path, { expiresInSeconds }),
    };
  }

  return tools;
}
