/**
 * Path-plane translation between Flue's workspace namespace and AiryFS.
 *
 * The adapter roots the Flue workspace at `/volume` — the exact path where the
 * AiryFS FUSE filesystem is mounted inside the execution Container
 * (`MOUNT_POINT = '/volume'` in the container command server). AiryFS's own
 * direct HTTP/SDK surface, by contrast, is *volume-rooted*: the file the
 * Container sees at `/volume/src/main.py` is addressed as `/src/main.py`
 * through the SDK.
 *
 * Keeping the workspace root at `/volume` means there is a single path plane
 * the model ever sees:
 *
 *   - Flue resolves relative paths against `env.cwd` (`/volume`), so `src/a`
 *     becomes `/volume/src/a`.
 *   - A shell command's default cwd is also `/volume` (the mount point), so
 *     `cat src/a` reads the same byte range the file tools wrote.
 *   - Absolute `/volume/...` paths the model emits line up with the mount.
 *
 * File-op methods translate the `/volume`-rooted workspace path back to the
 * volume-rooted SDK path with {@link toSdkPath}. Exec does **not** translate —
 * its cwd and command already live in the `/volume` mount plane.
 */

export const MOUNT_ROOT = "/volume";

/**
 * Translate a Flue workspace path (rooted at `/volume`) into the volume-rooted
 * path the AiryFS SDK expects.
 *
 *   `/volume`            -> `/`
 *   `/volume/src/a.txt`  -> `/src/a.txt`
 *   `/src/a.txt`         -> `/src/a.txt`  (bare-absolute: treated as volume-rooted)
 *   `rel/a.txt`          -> `/rel/a.txt`  (defensive; Flue normally resolves first)
 *
 * Bare-absolute paths outside `/volume` (e.g. a model that emits `/etc/hosts`)
 * are treated as volume-rooted rather than errored: the volume *is* the
 * filesystem, so there is nowhere else for them to live. This is documented as
 * an edge case; the natural relative-path / `/volume/...` conventions are fully
 * consistent between the file tools and the shell.
 */
export function toSdkPath(workspacePath: string): string {
  let p = workspacePath;
  if (!p.startsWith("/")) p = "/" + p;
  // Collapse a trailing slash except for the root itself.
  if (p.length > 1 && p.endsWith("/")) p = p.replace(/\/+$/, "");
  if (p === MOUNT_ROOT) return "/";
  if (p.startsWith(MOUNT_ROOT + "/")) {
    const stripped = p.slice(MOUNT_ROOT.length);
    return stripped === "" ? "/" : stripped;
  }
  return p;
}

/** Single-quote a string for safe inclusion in a `sh -c` command. */
export function shellQuote(value: string): string {
  return "'" + value.replace(/'/g, "'\\''") + "'";
}

/**
 * Deterministic, dependency-free 32-bit FNV-1a hash rendered as 8 hex chars.
 * Used to make sanitized volume names collision-resistant across ids that
 * would otherwise sanitize to the same slug. Synchronous and runtime-portable
 * (no node:crypto / no async crypto.subtle), so the factory works unchanged in
 * Node, Workers, and browsers.
 */
export function shortHash(input: string): string {
  let h = 0x811c9dc5;
  for (let i = 0; i < input.length; i++) {
    h ^= input.charCodeAt(i);
    // 32-bit FNV prime multiply via shifts to stay in the int range.
    h = (h + ((h << 1) + (h << 4) + (h << 7) + (h << 8) + (h << 24))) >>> 0;
  }
  return h.toString(16).padStart(8, "0");
}

/**
 * Derive a valid, collision-resistant AiryFS volume name from an agent/run id.
 * AiryFS volume names must not start with `_`; we prefix `agent-` and restrict
 * to `[a-z0-9-]`, then append a short hash of the *raw* id so that two distinct
 * ids can never map to the same volume.
 */
export function defaultVolumeName(id: string): string {
  const slug = id
    .toLowerCase()
    .replace(/[^a-z0-9-]+/g, "-")
    .replace(/^-+|-+$/g, "")
    .slice(0, 40);
  const base = slug.length > 0 ? slug : "id";
  return `agent-${base}-${shortHash(id)}`;
}
