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

## Bounds

- Individual request and response payloads and aggregate partial-frame buffers are limited to 8 MiB plus framing overhead.
- Oversized HTTP requests receive `413`.
- Oversized TCP frames terminate the channel.
- Every active request has a 30-second response deadline.

The 8 MiB limit accommodates a configured 1 MiB filesystem chunk after JSON and base64 expansion while bounding memory in both the Container and Worker.

## Verification

Run local transport and protocol tests with:

```sh
cd container && npm test
cd ../worker && npm test && npm run typecheck
```

The deployed gate also verifies sequential reconnects, rejects overlapping `exec` calls, and reruns direct/FUSE coherence, same-volume Git, and Container replacement.
