import {
  defineAgent,
  defineWorkflow,
  type WorkflowRouteHandler,
} from "@flue/runtime";
import * as v from "valibot";
import { airyfs } from "@airyfs/flue-sandbox";

interface Env {
  AI: unknown;
  AIRYFS_ENDPOINT?: string;
  AIRYFS_TOKEN?: string;
  AIRYFS_VOLUME?: string;
  FLUE_MODEL?: string;
}

const DEFAULT_ENDPOINT = "https://airyfs-int.tyson-s-sandbox.workers.dev";
const DEFAULT_MODEL = "@cf/openai/gpt-oss-120b";

/** Expose the workflow at `POST /workflows/summarize`. */
export const route: WorkflowRouteHandler = async (_c, next) => next();

const worker = defineAgent<Env>(({ env }) => ({
  model: `cloudflare/${env.FLUE_MODEL ?? DEFAULT_MODEL}`,
  sandbox: airyfs({
    endpoint: env.AIRYFS_ENDPOINT ?? DEFAULT_ENDPOINT,
    token: env.AIRYFS_TOKEN,
    ...(env.AIRYFS_VOLUME ? { volume: () => env.AIRYFS_VOLUME! } : {}),
  }),
}));

/**
 * A finite workflow that exercises the AiryFS sandbox adapter end-to-end
 * through Flue on Workers AI, without relying on model-driven tool calls:
 *
 *   1. Application code stages an input file into the durable AiryFS volume via
 *      `harness.fs.writeFile` — that call goes through the adapter's SessionEnv
 *      to real Durable-Object-backed storage.
 *   2. It reads the file back through the adapter (`harness.fs.readFile`).
 *   3. The model produces a one-line summary (a single, tool-free generation
 *      turn — robust across Workers AI models).
 *   4. The artifact is written back into the volume via the adapter.
 *
 * The returned `summary` and the `summary.txt` file left behind in the volume
 * are both proof the adapter drove durable storage through Flue.
 */
export default defineWorkflow({
  agent: worker,
  input: v.object({ text: v.string() }),

  async run({ harness, input }) {
    await harness.fs.writeFile("input.txt", input.text);
    const staged = await harness.fs.readFile("input.txt");

    const response = await (
      await harness.session()
    ).prompt(
      `Summarize the following text in one short sentence. ` +
        `Reply with only the sentence.\n\n${staged}`,
    );
    const summary = response.text.trim();

    await harness.fs.writeFile("summary.txt", summary);

    return { summary, inputChars: staged.length };
  },
});
