# Commands v1: phase-two API and adapter alignment

ShenLink canonical imports: `shenlink.v1` for commands/sensors and
`shenlink.v1.video` for video. The legacy imports below remain compatible and
reference identical classes; see the root README for the current distribution name.

## Public surface

Use `multiflow_comm.v1` for the simple command API. Video remains
`video_stream.v1`. Both expose the **same** TransportAdapter, callbacks,
capabilities, states, send statuses, UdpTransport and TcpTransport classes.
There is no second wire protocol, delivery engine, codec or priority API here.

```python
from multiflow_comm.v1 import (
    CommandSender, CommandReceiver, CommandSenderSettings, CommandReceiverSettings,
    UdpTransport, TicketStatus,
)

# On the robot: a fresh, exclusively owned transport per path.
receiver = CommandReceiver(
    CommandReceiverSettings([
        UdpTransport("a", ("127.0.0.1", 9202), ("127.0.0.1", 9201)),
    ]),
    on_command=lambda command: print(command.decode_object()),
)
receiver.start()

# On the station, with matching channel ID and reversed addresses.
sender = CommandSender(CommandSenderSettings([
    UdpTransport("a", ("127.0.0.1", 9201), ("127.0.0.1", 9202)),
]))
sender.start()
ticket = sender.send_object({"action": "move", "speed": 0.2})
status = ticket.wait(2)
# RECEIVED means receiver acceptance, NOT actuator execution.
print(status)
sender.stop()
receiver.stop()
```

Add paths B/C to each settings list to use redundant multipath sending. Replace
UDP with TcpTransport (sender `mode='connect'`, receiver `mode='listen'`) without
changing command policy. No video/native media modules are imported when using
commands alone. Import and construction do not allocate transport resources;
start does. Context managers also start/stop the endpoints.

`send_bytes`, `send_text`, and `send_object` return the existing DeliveryTicket.
Objects default to JSON; supply a Codec to use another serialization. Callbacks
receive the existing ReceivedMessage with payload, sequence, session, path,
`text()` and `decode_object()`. Optional `on_error`, `on_state`, `on_session`
(receiver) and `on_blocked` (sender) provide notifications. `path_states` reports
command-verified sender availability, not just a locally open socket.

## Independent adapter contract

Implement the structural TransportAdapter interface; no inheritance is needed:

- `path_id`, `capabilities: TransportCapabilities`, `state: TransportState`.
- `open(TransportCallbacks)`: initiate setup with bounded work.
- `send(bytes) -> SendStatus`: thread-safe and bounded. ACCEPTED is local
  acceptance; BACKPRESSURE/UNAVAILABLE mean this message was not accepted.
- `reconnect()`: reconnect without mixing old and new connection bytes.
- `close()`: idempotent, release resources and quiesce transport callbacks.
- Incoming callbacks contain complete bounded immutable byte messages, including
  an ACK return direction. A TCP implementation must frame its byte stream.

The developer controls transport, security and network configuration. The
command library schedules probes, verifies command HELLO ACKs, excludes failed
paths, retries and recovers them using reconnect; the application need not poll
or manually switch paths. UDP blackholes do not require a socket reconnect while
the socket remains usable. Custom adapters must meet their declared I/O bounds;
structural validation is not proof of runtime conformance.

The byte-only contract and existing UDP/TCP implementations now live in
`multiflow_comm.byte_transport`, `byte_udp`, `byte_tcp`. Historical video module
paths re-export the same classes. The implementation move is mechanical;
network send/read loops are unchanged. The adaptive video runner's evidence
hash list includes the new actual implementation locations.

## Preserved delivery and bounds

- Commands default to ORDERED_ALL / REDUNDANT / RECEIVED. The phase-one core and
  reused dispatch workers perform ordering, deduplication and retries; the
  facade introduces no dispatch worker and does not serialize commands twice.
- Receipt is acceptance, not execution. Lost ACKs can leave the outcome unknown.
  No exactly-once execution across crashes, persistent queue or transaction is
  promised. See [phase-one contract](command-phase1.md).
- Default payload bound is 1024 bytes; default queue bound is 256. Settings
  reserve the wire envelope against **every** path's declared message limit
  before opening it. Oversized sends fail before consuming a sequence. Larger
  commands require adapters with larger declared bounds, not silent fragmentation.
- Receiver payload bounds are enforced before session/order processing, receipt
  and application delivery. Sender and receiver must configure compatible bounds;
  there is no automatic payload-bound negotiation. A rejected oversized command
  will not become RECEIVED.
- Default ACK/probe timeout is 0.5s; probe interval 1s; receiver reconnect interval
  2s. Confirmed ordered retries remain unlimited unless max_ordered_retries is
  configured. With a finite budget, unrecoverable failure blocks later commands.
  `restart_session()` explicitly resets sender sequence/session state; this is
  not a safe automatic replay of application operations with unknown outcomes.
- `ticket.wait(timeout)` only bounds observation; it does not cancel the command.
  stop/close fails outstanding tickets. Stopped endpoints are not restartable;
  create fresh paths/settings/endpoints. Receiver callbacks can request stop
  without the delivery thread attempting to join itself.
- A transport instance belongs to one endpoint. Video and commands can use the
  same physical routes and adapter implementation, but **not the same socket or
  adapter instance**. Shared-connection multiplexing is not implemented.
- Built-in UDP/TCP are trusted-network adapters, not authenticated/encrypted
  channels. Capability flags do not manufacture security or end-to-end guarantees.
- No priority API, DDS adapter, execution-result protocol or CPU optimization
  is added in this phase. Legacy advanced message/telemetry APIs remain unchanged.

## Runnable example

On two terminals, start receiver before sender:

```sh
python3 -I -B examples/commands_v1.py receiver
python3 -I -B examples/commands_v1.py sender
```

Both accept `--transport tcp`, `--count 10`, `--channel commands`, and repeated
`--path ID,LOCAL_IP,LOCAL_PORT,PEER_IP,PEER_PORT`. These are functional examples,
not authenticated robot-control applications. The receiver prints delivery, not
an actuator result. On separate hosts specify reachable route-specific addresses.

Rootless loopback check:

```sh
python3 -I -B tools/commands_v1_smoke.py
```

## Pi acceptance (2026-10-09)

- 181 targeted tests plus 24 subtests passed. Includes 22 new endpoint cases,
  phase-one/dispatch/scheduler/core regressions and video contract/API tests.
  No skips or deselections in that run; this is not the entire registered suite.
- UDP tests prove 30 actual DATA copies on each of three paths, ordered single
  application delivery, duplicate suppression, blackhole restoration without
  replay, and real socket recovery. TCP and two-process example checks passed.
- Explicit admission bounds, receiver rejection before receipt, backpressure
  without false delivery, and stop-from-callback tests passed.
- Original first-command redundancy test passed 50 consecutive repetitions with
  its assertions unchanged.
- Standalone three-path command check: UDP 30/30 deliveries and receipts; TCP
  30/30 deliveries and receipts. This does not certify physical-network delivery.
- Native video ordered-TCP check: 121/121 contiguous frames, one-path loss
  without missing frames, graceful completion and explicit total-disconnect
  error. This is an integration regression check, not an SRT/freshness benchmark.
- Pre-deployment Pi backup:
  `/home/edgelif2/.cache/mfl-video-runtime/commands-before-phase2.4Armzz`.

No new CPU/RSS, SRT or DDS superiority claim is made. Phase three is now
complete as a rootless simultaneous packet-blackhole
integration check. It does not claim the privileged emulator's bandwidth/RTT
conditions. See [scope, raw evidence and final usage checklist](video-commands-integration.md).
