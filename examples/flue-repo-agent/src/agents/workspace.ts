import { defineAgent, type AgentRouteHandler } from "@flue/runtime";
import { airyfs } from "@airyfs/flue-sandbox";

/** Expose the agent over HTTP at `POST /agents/workspace/:id`. */
export const route: AgentRouteHandler = async (_c, next) => next();

/**
 * Bindings available to the Worker. `AI` is the Workers AI binding that Flue's
 * built-in `cloudflare/...` provider uses (no API key needed). The AiryFS
 * settings point the sandbox adapter at a durable-filesystem deployment.
 */
interface Env {
  AI: unknown;
  AIRYFS_ENDPOINT?: string;
  AIRYFS_TOKEN?: string;
  /**
   * Pin every agent instance to one fixed volume. Set this to demonstrate that
   * the durable workspace is decoupled from agent/conversation identity: a
   * brand-new agent instance (new DO, empty history) mounting the same volume
   * still sees files written by an earlier, unrelated instance. Leave unset for
   * the default one-durable-volume-per-agent-instance behavior.
   */
  AIRYFS_VOLUME?: string;
  /** Optional Workers AI model id override, e.g. `@cf/meta/llama-3.3-70b-instruct-fp8-fast`. */
  FLUE_MODEL?: string;
}

const DEFAULT_ENDPOINT = "https://airyfs-int.tyson-s-sandbox.workers.dev";
// gpt-oss-120b streams OpenAI-compatible responses (with finish_reason) through
// the Workers AI binding that Flue requires, and is strong at tool use. Many
// other @cf/* models either 400 or omit finish_reason on the binding path.
const DEFAULT_MODEL = "@cf/openai/gpt-oss-120b";

/**
 * A continuing agent whose workspace is a durable AiryFS volume.
 *
 * The key property this sample demonstrates: the agent DO (holding
 * conversation state) and the AiryFS volume (holding files) are *separate*
 * durable resources, both keyed to the same agent instance id. The volume is
 * `airyfs()`'s default `agent-<slug(id)>-<hash>`, so re-invoking the same
 * instance id — even after the Worker restarts or the AiryFS Container has
 * been evicted — lands on the exact same files. Conversation durability comes
 * from Flue; workspace durability comes from AiryFS; they are decoupled.
 */
export default defineAgent<Env>(({ env }) => ({
  model: `cloudflare/${env.FLUE_MODEL ?? DEFAULT_MODEL}`,
  sandbox: airyfs({
    endpoint: env.AIRYFS_ENDPOINT ?? DEFAULT_ENDPOINT,
    token: env.AIRYFS_TOKEN, // undefined against the unauthenticated int endpoint
    ...(env.AIRYFS_VOLUME ? { volume: () => env.AIRYFS_VOLUME! } : {}),
  }),
  instructions: [
    "You work inside a durable POSIX filesystem mounted at /volume; it is your",
    "persistent workspace and survives between our conversations.",
    "",
    "Conventions:",
    "- Prefer the file tools (read/write/edit) over shell for inspecting or",
    "  editing files — they hit the durable storage directly and are fast.",
    "- Use the bash tool for real work that needs a shell (running programs,",
    "  git, counting with wc, etc.). Commands run in /volume.",
    "- When asked to remember a fact for later, write it to notes.txt.",
    "- When asked to recall something, read the relevant file back first.",
  ].join("\n"),
}));
