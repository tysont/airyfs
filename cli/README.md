# AiryFS CLI

The AiryFS CLI provides local, stateful access to a remote AiryFS volume. It sends direct filesystem operations to the Durable Object HTTP API and runs `exec` commands in the attached Container against the same volume.

## Install

Node.js 22 or newer is required.

From the repository root, one command builds the SDK and CLI and links both binaries:

```bash
./install.sh
```

This installs two equivalent executables: `airyfs` and the short alias `airy`. To install manually:

```bash
cd cli
npm install
npm run build
npm link
```

`npm link` adds the `airyfs` and `airy` executables to the active Node installation. For development without linking, use `npm run dev -- <arguments>`.

## Quick Start

```bash
airyfs session create work \
  --endpoint https://airyfs.example.com \
  --volume my-volume
airyfs volume create

airyfs pwd
airyfs mkdir -p src/app
airyfs cd src
airyfs put ./package.json
airyfs ls -l
airyfs exec git init
airyfs exec git add package.json
airyfs exec git -c user.name=AiryFS -c user.email=airyfs@localhost \
  commit -m "Initial commit"
```

The Worker does not currently require authentication. Do not expose a AiryFS endpoint to untrusted callers without adding authentication and authorization.

## Sessions

A named session is the CLI execution context. It keeps an endpoint, volume, and current remote directory together. Settings live in `~/.airyfs/config.json`; set `AIRYFS_HOME` to use a different directory.

```bash
airyfs session create int --endpoint https://airyfs-int.example.com --volume scratch
airyfs session create prod --endpoint https://airyfs.example.com --volume project
airyfs session list
airyfs session use prod
airyfs session show
airyfs session edit prod --volume another-project
airyfs session rename prod production
airyfs session delete production
```

The active session is resolved in this order:

1. `--session <name>`
2. `AIRYFS_SESSION`
3. The persisted current session

There is no implicit default session. `session create` automatically selects the new session. Deleting the active session leaves no session selected, and commands such as `ls`, `pwd`, and `exec` fail until another session is created or selected. Deleting a session removes only local CLI state; it never deletes the remote volume.

In a TTY, omitted `session create` values are prompted interactively:

```text
$ airyfs session create
Session name: work
Endpoint: https://airyfs.example.com
Volume: project
```

Scripts and other non-interactive callers must provide the name, `--endpoint`, and `--volume` explicitly.

This allows separate terminals to stay pinned to different sessions:

```bash
AIRYFS_SESSION=int airyfs shell
AIRYFS_SESSION=prod airyfs status
```

`airyfs session edit [name] --endpoint <url>` changes a session endpoint. `airyfs session edit [name] --volume <volume>` changes its volume and resets its remote directory to `/`.

The CLI remembers volume names in sessions because Durable Object namespaces do not provide an API for enumerating named volumes.

To move a session to another computer, export it to a portable blob and import it there:

```bash
airyfs session export prod            # prints an `airyfs1:...` blob (contains the token)
airyfs session import airyfs1:... home # recreates and selects the session as `home`
```

The blob embeds the session's bearer token, so treat it as a credential and share it only over a trusted channel. Alternatively, recreate the session from scratch and authenticate with the volume password (see Authentication).

## Authentication

A volume can require a password without redeploying the Worker. The deployment must have `AIRYFS_AUTH_SECRET` set for password auth to function; the secret stays the root/admin credential and the capability signing key.

```bash
airyfs volume create --password           # create the volume, set a password, and log in with a scoped token
airyfs auth passwd <new-password>         # set or rotate the password (root, admin, or --current <password>)
airyfs auth login --password              # exchange the password for a token on this machine
airyfs auth login <token>                 # or store a bearer token (root secret or capability) directly
airyfs auth status                        # show the session's authentication state
airyfs auth logout                        # clear the stored token
```

`volume create --password` stores only the resulting scoped token in the session, not the password, so day-to-day use is least-privilege. From a second computer, create a session pointing at the same endpoint and volume, then run `airyfs auth login --password` to obtain your own token.

## Navigation

Remote paths use POSIX semantics and resolve relative to the active session's current directory.

```bash
airyfs pwd
airyfs ls
airyfs cd src/app
airyfs ls ../tests
```

`cd` validates the target against the remote directory API before persisting it in the active session.

## Filesystem Commands

| Command | Purpose |
|---|---|
| `ls [path]` | List a directory; use `-l`, `-a`, or global `--json` |
| `cat <path>` | Stream raw file bytes to stdout |
| `get <remote> [local]` | Download a file without overwriting unless `--force` is used |
| `put <local> [remote]` | Stream a local file into the volume |
| `download <remote> [local]` | Download a file or (with `-r`) a directory tree, auto-detecting which |
| `upload <local> [remote]` | Upload a file or (with `-r`) a directory tree, auto-detecting which |
| `write <remote>` | Stream stdin into a remote file |
| `mkdir [-p] <path>` | Create a directory or parent chain |
| `rm [-r] <path>` | Remove a file, link, or directory |
| `mv <from> <to>` | Move or rename a path |
| `cp <from> <to>` | Copy a file |
| `ln -s <target> <path>` | Create a symbolic link |
| `readlink <path>` | Print a symbolic-link target |
| `truncate <path> <size>` | Resize a file; sizes accept `k`, `m`, and `g` suffixes |
| `stat <path>` | Show path metadata |

`cat` emits raw bytes and therefore cannot be combined with `--json` or `--quiet`. Use `get` for binary files that should not be written directly to the terminal.

`upload` and `download` are the ergonomic unified verbs. `upload` inspects the local path and streams a single file (like `put`, with `--resume`) or, with `-r/--recursive`, pushes a directory tree as a transactional archive (like `push`, with `--replace`). `download` inspects the remote path and retrieves a single file (like `get`, with `--resume`) or, with `-r/--recursive`, pulls a directory tree (like `pull`). The lower-level `put`/`get` and `push`/`pull` commands remain available.

## Execution

`exec` runs in `/volume` plus the session's current remote directory. It prints captured stdout and stderr and exits with the remote process's exit code.

```bash
airyfs warm
airyfs cd repository
airyfs exec git status
airyfs exec --timeout 60s git status  # Wait up to 60s for startup/admission
airyfs exec --no-wait npm test       # Fail immediately if another exec is active
```

`airyfs warm` (alias `airyfs wake`) starts the Container and mounts the selected volume by executing the shell no-op `:`. It does not change volume contents. Use it before latency-sensitive commands to pay the Container startup cost early.

Before an arbitrary command, the CLI runs the retry-safe shell no-op `:` to start or reconnect the Container. Transport failures and transient HTTP `502`, `503`, and `504` responses retry only that preflight. The user command is submitted at most once after ambiguous failures; it retries only for `EXEC_BUSY`, which confirms that the server did not admit it. Startup and busy-wait retries run for up to 90 seconds by default. `--timeout` controls only that startup and admission window; the server applies the remote process runtime limit.

Put CLI-specific exec options before the remote command. Options after the first remote command argument pass through unchanged:

```bash
airyfs exec --timeout 60s tool --json --output result.json
```

When a shell expression is supplied as one argument, it is sent as written:

```bash
airyfs exec 'find . -type f | sort'
```

## Interactive Shell

```bash
airyfs shell
```

The prompt includes the session, volume, and current remote directory:

```text
airyfs:prod:project:/src$ ls
airyfs:prod:project:/src$ exec git status
airyfs:prod:project:/src$ cd ..
airyfs:prod:project:/$ exit
```

The shell accepts the same commands and options as one-shot invocation. It supports single quotes, double quotes, backslash escaping, `help`, `clear`, `exit`, and `quit`. History is stored in `~/.airyfs/history`.

Tab completion uses the actual Commander command tree for commands and subcommands, the local session store for session names, and the active volume API for remote paths. For example, Tab completes `wa` to `warm`, `session use sa` to a matching session, and `cd dem` to a matching remote directory.

The shell can start without an active session. Its prompt shows `airyfs:no-session$`, session administration remains available, and remote commands report the same shared active-session error as one-shot commands. `session create`, `session use`, and `session delete` update the shell context immediately.

`write`, valueless `kv set`, and interactive `destroy` prompts are unavailable inside the shell because readline owns stdin. Use `put`, provide the KV value as an argument, or run `destroy --force` after confirming the selected session.

## Volume And Diagnostics

| Command | Purpose |
|---|---|
| `volume create [--chunk-size 256k]` | Create/configure the selected volume |
| `volume info` | Show the immutable chunk size |
| `context` / `config` | Show the selected session, endpoint, volume, and remote directory |
| `status` / `doctor` | Check endpoint, Container, FUSE, SQLite, and Hrana status |
| `usage` | Show raw filesystem and runtime usage |
| `perf` | Show Hrana request and SQL statement counters |
| `db-info` | Show per-table Durable Object SQLite row counts |
| `destroy [--force]` | Destroy the Container while preserving volume data |
| `warm` / `wake` | Start and mount the Container with a no-op command |
| `kv set/get` | Access the volume's key-value table |

## Web Hosting

Serve a volume publicly without a token. Nothing is exposed until you publish a site or create a share.

```bash
airyfs upload -r ./dist /site
airyfs site publish /site --spa --cache "public, max-age=300"
airyfs site status
airyfs site unpublish

airyfs share /reports/q3.pdf --expires 24h   # prints a /d/<volume>/<id> URL
airyfs share list
airyfs share rm <id>
```

Sites are served at `<endpoint>/s/<volume>/` and shares at `<endpoint>/d/<volume>/<id>` on any deployment. If the Worker sets `SITES_ZONE` and has a matching wildcard route, `<volume>.<zone>` also serves the published site.

## Deployment

Run from within the AiryFS repository to deploy the Worker to your Cloudflare account and get a ready session:

```bash
export CLOUDFLARE_API_TOKEN=your-api-token
airyfs deploy int --allow-dirty              # deploy, set the auth secret, create a session
airyfs init int --volume myproject --password # deploy + session + a secured volume in one step
```

`deploy` and `init` wrap `scripts/provision.mjs`, so they build the Worker and Container from source and must run inside the repo. `deploy` stores the deployment's root secret in the created session; `init` additionally creates a volume and downgrades the session to a password-scoped token.

## Global Options

Global options must appear before the command:

```bash
airyfs --session int --json ls
```

| Option | Purpose |
|---|---|
| `-s, --session <name>` | Select a session for one invocation |
| `--json` | Emit structured output where supported |
| `--no-color` | Disable ANSI colors |
| `-q, --quiet` | Suppress non-error output |

## Development

```bash
cd cli
npm run typecheck
npm test
npm run build
```

Tests use isolated temporary config directories and a local mock HTTP server. They never access `~/.airyfs` or a deployed AiryFS endpoint.
