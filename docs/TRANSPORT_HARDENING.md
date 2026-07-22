# Transport Hardening

AiryFS keeps the Hrana wire protocol as length-prefixed JSON and hardens each independent FIFO channel without multiplexing correlated envelopes over the Durable Object socket.

## Admission And Ordering

- Data and invalidation traffic retain separate HTTP and TCP channels.
- Each channel reserves capacity for at most 16 HTTP requests before reading their bodies, holds that capacity until each response finishes or closes, and serializes socket writes while allowing bounded FIFO pipelining.
- Bridge-local monotonic request IDs are returned as `X-AiryFS-Request-ID`; they are not sent over TCP.
- One `exec` may run per volume. An overlapping command receives `503 EXEC_BUSY` before it can replace the active TCP connections.
- Sequential execs switch new work to a fresh data generation while the bridge drains any admitted work on the retired socket.

## Cancellation And Failure

- Disconnecting queued HTTP requests removes them before TCP dispatch.
- Disconnecting a dispatched request rejects its HTTP waiter, then consumes and discards the eventual response without shifting later FIFO responses.
- Running Durable Object SQLite statements cannot be preempted.
- Active timeouts, write failures, malformed or oversized responses, and socket closure fail the connection and all admitted work because FIFO alignment can no longer be guaranteed.
- Buffered transient exec probes a dedicated control-plane endpoint and quarantines the runtime after three consecutive failures.
- Streaming exec requires heartbeat or output bytes at least every 15 seconds. Losing an admitted stream records the durable command as `unknown`; AiryFS never automatically replays it.
- Runtime generations prevent stale failures from destroying a replacement Container. Three infrastructure failures inside two minutes open a 30-second circuit, followed by one half-open recovery attempt.

## Bounds

- Individual request and response payloads and aggregate partial-frame buffers are limited to 8 MiB plus framing overhead.
- Oversized HTTP requests receive `413`.
- Oversized TCP frames terminate the channel.
- Every active request has a 30-second response deadline.

The 8 MiB limit accommodates a configured 1 MiB filesystem chunk after JSON and base64 expansion while bounding memory in both the Container and Worker.

## Operational Diagnostics

The Container health response reports whether the data bridge has a DO TCP connection, aggregate pending, queued, and admitted request counts across active and retired generations, and Node.js process memory/resource usage. A bridge connection failure with admitted work emits a structured `bridge_connection_failed` log. Worker errors at status 500 or above emit `request_failed` with the edge request ID, route, status, Hrana session, and session epoch; command bodies and SQL text are never logged.

A July 2026 integration investigation reproduced intermittent buffered exec hangs under sustained filesystem activity on both `lite` and `basic` instances. Immediately before a hang, the bridge was connected with no pending, queued, or admitted work. During the hang, independent probes to the bridge and command-server ports both failed with `Error proxying request to container: The operation was aborted due to timeout`, while the Durable Object remained responsive and its Hrana counters stopped advancing with no active operation or filesystem lock holder. This localizes that failure mode to an unresponsive Container process, VM, or network proxy, below edge routing and the Durable Object SQL/Hrana server. A mounted FUSE check or Hrana socket probe before the command cannot prevent a runtime that becomes unresponsive after admission.

## Verification

Run local transport and protocol tests with:

```sh
cd container && npm test
cd ../worker && npm test && npm run typecheck
```

The deployed gate also verifies sequential reconnects, rejects overlapping `exec` calls, and reruns direct/FUSE coherence, same-volume Git, and Container replacement.
