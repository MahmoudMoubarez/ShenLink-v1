# Video v1: transport contract and usage shape

The byte-message transport contract and built-in UDP/TCP adapters are also
available from `multiflow_comm.v1` for commands. Both namespaces expose the
same classes, with historical video imports preserved. Use separate transport
instances/connections for commands and video; shared-connection multiplexing
is not provided. See [Commands v1](commands-v1-api.md).
The [phase-three combined check](video-commands-integration.md) documents
native video and commands running together through individual A/C/B packet
blackholes, with raw timing evidence and explicit loopback-only limitations.

## Latest-video quality replacements

Latest-video callbacks deliver a new capture immediately (`frame.delivery_kind`
is `new_frame`). If a higher-resolution decoded copy of that same capture
arrives before a newer capture is displayed, `on_frame` may be called again
with `delivery_kind='quality_upgrade'`, the same capture ID and timestamp, and
larger dimensions. Renderers should replace the current image, not append it as
a new capture. Different capture timestamps or sessions cannot be upgraded;
older captures never replace newer ones. Ordered delivery remains unchanged.

Recorded-video benchmarks log these replacements separately from fresh capture
deliveries: they affect resolution exposure, not FPS, freshness gaps or age of
information. The current 25 FPS clips use receiver minimum=maximum=25; this is
a benchmark choice, not removal of developer-configurable FPS ranges.

## Canonical public entry point

Use `video_stream.v1` for the new implementation:

```python
from video_stream.v1 import (
    VideoSender, VideoReceiver, SenderSettings, ReceiverSettings,
    UdpTransport, TransportAdapter, TransportCallbacks, TransportCapabilities,
    TransportState, SendStatus, ReceiverRequirements, DelayPolicy, CameraSource,
)
```

These exports are the actual existing endpoint classes, not another implementation
or performance wrapper. The historical `from video_stream import VideoReceiver`
still resolves to the old receiver; do not use that root import for v1.
Old modules remain available, but new developer examples use this entry point.
Importing v1 does not start codecs or open sockets. Native GStreamer/H264 plugins
and PyGObject are required when endpoints start. Dynamic FPS is available as an
opt-in (see the dynamic FPS section below); fixed FPS remains the default. Use
`CameraSource(fps=30)` with default receiver settings (the historical source
class by itself defaults to 24 FPS, not the v1 receiver's default 30).

## Run the minimal example

From the project directory on Linux, run these in two terminals concurrently:

```sh
# Terminal 1: receiver (decoded-frame statistics; no display)
/usr/bin/python3 -I -B examples/video_v1.py receiver --seconds 10
# Terminal 2: sender (synthetic moving video, not a hardware camera)
/usr/bin/python3 -I -B examples/video_v1.py sender --seconds 10
```

The default uses one loopback path on UDP ports 7000/7001. No sudo is needed.
Start the second command within the 12-second negotiation timeout. The runtime
begins after negotiation. A PASS is functional transfer, not a latency benchmark.
Use `--camera` on the sender to opt into hardware capture; camera availability
and device selection are platform-specific and not certified by the synthetic
check. Use `--strict` on the receiver only to reject over-age frames.

For three independent routes, repeat `--path` with
`ID,LOCAL_IP,LOCAL_PORT,PEER_IP,PEER_PORT` on both processes. For example:

```sh
# Robot: replace these addresses with actual reachable interface addresses.
/usr/bin/python3 -I -B examples/video_v1.py sender \
  --path a,10.1.0.1,9000,10.1.0.2,9000 \
  --path b,10.2.0.1,9000,10.2.0.2,9000 \
  --path c,10.3.0.1,9000,10.3.0.2,9000
# Station: reverse each route's local/peer addresses.
/usr/bin/python3 -I -B examples/video_v1.py receiver \
  --path a,10.1.0.2,9000,10.1.0.1,9000 \
  --path b,10.2.0.2,9000,10.2.0.1,9000 \
  --path c,10.3.0.2,9000,10.3.0.1,9000
```

Different UDP ports alone do not establish independent physical paths. Routing
and connectivity must exist on the hosts. Example callbacks only count decoded
frames; rendering and application processing are the developer's responsibility.

The byte-message contract, UDP adapter and latest-frame endpoint lifecycle are
implemented. The existing adaptive codec/quality engine is reused, not rewritten.
Preserve-order delivery and a bounded TCP adapter are also implemented, as a
separate opt-in fixed-quality mode. Latest-frame UDP remains the default.

## What a developer supplies

Implement `TransportAdapter` structurally: no subclass is required.
Provide `path_id`, `capabilities`, `state`, `open(callbacks)`,
`send(bytes)`, `reconnect()`, and `close()`.
The adapter owns protocol framing, transport reliability, security, bounded
queues and transport-specific reconnection. TCP adapters must reconstruct whole
messages rather than deliver arbitrary socket reads. UDP adapters must respect
their declared maximum message size.

The library owns video encoding/decoding, baseline/enhancement multiplexing,
media identity/epochs, deduplication, freshness, quality policy, receiver feedback
and the decision to recover a path. Reliable transport is not an acknowledgment
that an application received a frame; disconnects must be reported.

`send` is nonblocking and returns ACCEPTED, BACKPRESSURE, or UNAVAILABLE.
ACCEPTED means locally accepted only. BACKPRESSURE/UNAVAILABLE means not accepted.
The latest-frame policy may drop work; preserve-order uses non-leaky encoder
queues and explicit bounded-pressure/gap errors rather than silently skipping.
Adapters declare reliability, ordering, security and maximum message size.
There are deliberately no processing-priority or network-priority fields.
An adapter can offer optional protocol-specific network marking internally.

## Working configuration API in this stage

```python
from video_stream.v1 import SenderSettings, ReceiverSettings

# adapters are developer instances implementing TransportAdapter;
# source implements the existing source dimensions/FPS/build_elements contract.
sender_settings = SenderSettings(source=camera, paths=adapters)
receiver_settings = ReceiverSettings(paths=adapters)
# These validate configuration only. They do not open sockets or start video.
```

Sender and receiver use their respective local adapter instances, not the same
instances across machines. Defaults retain the tested 360–720p, 30fps,
600–1800kbps policy and best-available delivery with a 150ms age target.
Custom receiver bounds generate a quality ladder within those bounds.

## Working latest-frame endpoints

```python
from video_stream.v1 import UdpTransport, VideoSender, VideoReceiver, CameraSource

# Robot: use real robot/station addresses instead of these example addresses.
paths = [UdpTransport("a", ("192.168.1.10", 9000), ("192.168.1.20", 9000))]
sender = VideoSender(SenderSettings(CameraSource(fps=30), paths))
sender.start()  # waits up to 12 seconds for the station, then starts codecs
# ...application runs...
sender.stop()

# Station (a separate process/machine):
paths = [UdpTransport("a", ("192.168.1.20", 9000), ("192.168.1.10", 9000))]
receiver = VideoReceiver(ReceiverSettings(paths), on_frame=lambda frame: display(frame))
receiver.start()
# ...application runs...
receiver.stop()
```

Add more adapter instances with distinct matching path IDs at both endpoints.
Each adapter carries baseline, enhancement and control in one bidirectional
connection. The receiver sends its requirements automatically; the sender
intersects them with source dimensions/FPS. A complete configuration hash and
new epoch are confirmed before codecs start. Both applications must run their
start calls concurrently (separate processes, or separate threads in a test).
Start is synchronous and bounded; endpoint instances are single-use.
Use `on_error(exception)`, `on_state(path_id,state,reason)` and optional
`on_delivery_status(status)` callbacks. User callbacks must not block.
Default latest-frame behavior may discard frames under pressure.

UDP is not authenticated/encrypted. Peer address checking is not authentication.
The current bridge requires message bounds of at least
`shared.core.HEADER.size + shared.core.CHUNK + 3` bytes and room for handshake
messages; smaller-limit adapters are rejected, not silently overrun.
UDP send success means kernel acceptance, not remote delivery.
Control messages are retried and duplicates are handled; video reliability
continues to be the chosen adapter's responsibility.

Path socket recovery and packet-blackhole recovery use the existing media probes.
Latest-frame endpoints automatically renegotiate when the peer application
restarts, by default (`auto_recover=False` opts out). The surviving endpoint
quiesces its old codecs, retains its adapters and starts a new negotiated epoch.
Recovery negotiation is bounded by `timeout_s` (default 12 seconds) after restart
detection. A receiver sends a small discovery control message once per second
per path; this is not a media replay or a guarantee of interruption-free output.
Custom sources must support building fresh pipeline elements for each session.

`on_session(event)` reports `starting` and `interrupted`, with epoch and generation.
Callbacks must be quick and must not call lifecycle methods synchronously.
`counters()['session_generation']` increments when codecs are rebuilt. Public
latest frames carry one common `session_id` across both quality tiers; frame IDs
may restart only when that session changes. Frames during downtime are not
replayed. Keep ordering checks scoped to the session, as in the public example.
Preserve-order mode does **not** silently recover an interrupted application:
completeness is no longer provable. Its explicit failure requires new endpoints
and a new session, not a claim that missing old frames were delivered.

`/usr/bin/python3 -I -B tools/video_restart_smoke.py` checks real sender and
receiver process replacement with native codecs on UDP and TCP loopback, without
sudo or SRT. Confirmation time includes restart, negotiation and ten decoded
frames; it is not a per-frame latency benchmark.

### Optional dynamic frame rate (latest-frame only)

Enable `ReceiverSettings(dynamic_fps=True, adaptation_priority=...)` with either
`AdaptationPriority.FRAME_RATE_FIRST` (default priority) or
`AdaptationPriority.QUALITY_FIRST`. There are no percentage weights. The default
`dynamic_fps=False` preserves the verified fixed-cadence policy for existing users.

```python
from video_stream.v1 import AdaptationPriority, ReceiverRequirements, ReceiverSettings, SenderSettings

# Capture must support the requested ceiling. The camera is still captured at60;
# branch rate limiting occurs before scaling/encoding, not inside the camera.
sender_settings = SenderSettings(source_60fps, sender_paths, min_fps=24, max_fps=60)
receiver_settings = ReceiverSettings(
    receiver_paths,
    requirements=ReceiverRequirements(min_fps=24, max_fps=60, max_frame_age_ms=150),
    dynamic_fps=True,
    adaptation_priority=AdaptationPriority.FRAME_RATE_FIRST,
)
```

The negotiated range is the intersection of sender delivery bounds, receiver
bounds and source capture ceiling. An empty intersection fails before allocating
codecs. The selected profile never drops below the negotiated minima; successful
negotiation cannot guarantee actual delivered FPS over an unavailable network.

Baseline copies on all paths keep minimum quality/minimum FPS. A single high
branch follows a ladder changing only FPS or quality at each step. Frame-rate
first increases FPS at minimum quality, then increases quality at maximum FPS;
quality-first reverses that order. Downgrades reverse the ladder, and a new primary
starts at the minimum. Existing feedback expiry, hysteresis and failed-level retry
guards remain. Lowering FPS trades temporal freshness for less encoding/network
work; it does not guarantee fresher output or proportional byte savings.

Each branch uses drop-only rate limiting: retained capture IDs and source times
are real, no old frame is duplicated to manufacture FPS. Each stream still has
two encoders and two decoders total, not one per transport path. Dynamic output
does not change the preserve-order policy, which remains fixed-cadence.

Run `/usr/bin/python3 -I -B tools/video_dynamic_fps_smoke.py` for real24..60 FPS
and resolution transitions on three native loopback paths under both priorities.
This is a functional check, not a routed SRT benchmark.

The routed runner's `--compare-fps` mode compares both priorities, a fixed30 SDK
control, and unchanged SRT-high, with balanced reversed order for two repetitions.
It intentionally uses capture30, range24..30, and the previous network schedule
to match historical `c548c521`. This exercises FPS adaptation without conflating
it with a doubled camera rate. The fixed control uses the current implementation
with adaptation disabled; the old binary is backed up separately, not silently
claimed to be re-executed. Reports include freshness, actual public FPS, resources,
network, resolution exposure and sender adaptation targets; no equal-quality or
statistical-superiority claim is made.

Native rootless integration check on Linux:
```sh
/usr/bin/python3 -I -B tools/video_v1_smoke.py
/usr/bin/python3 -I -B tools/video_v1_smoke.py --custom-bounds
```
These verify decoded video, three baseline paths and actual two-codec-per-endpoint
inventory, monotonic public frames, bounded negotiated dimensions and actual
quality adaptation. Maximum quality observed is reported separately: reaching
maximum within eight seconds is not guaranteed under host load. A conservative
downgrade is not an integration failure. These checks do not establish sustained
capacity, performance parity, or the routed SRT comparison.

Default delivery remains latest-decodable-frame. Preserve-order is a separate
mode with explicit queue pressure and reliable ordered transport requirements.
No compatibility with the old adapter API is promised for the first release.

## Ordered TCP usage and completion

```python
from video_stream.v1 import TcpTransport, DeliveryMode

# Robot: connect mode. Station uses matching addresses, mode="listen".
path = TcpTransport("a", ("192.168.1.10", 9000), ("192.168.1.20", 9000))
sender = VideoSender(SenderSettings(CameraSource(fps=30), [path]))

# In the separate station process:
path = TcpTransport("a", ("192.168.1.20", 9000),
                    ("192.168.1.10", 9000), mode="listen")
receiver = VideoReceiver(ReceiverSettings([path],
    delivery_mode=DeliveryMode.PRESERVE_ORDER), on_frame=consume)
```

Receiver negotiation selects the mode on the sender; sender adapters must also
declare reliable ordered delivery. UDP is rejected for this mode. Start both
endpoints concurrently as above. Multiple TCP adapters with matching IDs are
supported; encoded copies are merged before one decoder.

Ordered mode currently uses **one fixed negotiated profile**: the highest level
within receiver/source bounds, not the two-quality adaptive live policy. Set
matching min/max resolution and bitrate to request a particular fixed profile.
It has one encoder and one decoder, preserves contiguous library capture IDs,
and does not discard a frame solely because it exceeds the age target. Unknown
clock age is reported as unknown instead of causing a silent frame discard.
FPS remains fixed. This is not a recording guarantee for physical camera frames
that never entered the library capture stage.

Queues are finite: two queued encoded units per path plus one in flight;
TCP defaults to 256 KiB accepted-but-not-written bytes; decoder/reorder bounds
are eight units. Reorder gaps exceeding two seconds, capture-ID skips and
decoder/transport pressure that cannot be served cause explicit failure.
Absence of decoded progress for `max(2 seconds, 3/FPS)` is also an explicit
stall error while streaming (not after confirmed finish), including when no
later packet arrives to expose a gap. This mode is a continuous-video contract,
not a sparse-event stream that can be idle indefinitely.
The application must keep callbacks short. There is no unbounded archival queue
and no added UDP retransmission protocol. TCP does not make a total disconnect
or process crash lossless, and restarting/reconnecting a stream with missing
frames is not silently treated as success.

On the sender, call `sender.finish(timeout_s=5)` **before** `stop()` for successful
ordered completion. This sends EOS through the codec, drains it, then waits for
the receiver to confirm its callback completed for the last captured frame.
The acknowledgment confirms this callback, not rendering, disk durability or
downstream processing. It is an end-of-stream confirmation, not a payload retry
mechanism. Timeout/disconnection raises an error. `stop()` and context-manager
exit remain cancellation/resource cleanup; they do not implicitly promise that
queued video was delivered. Endpoint instances remain single-use.

Runnable example (receiver lifetime must outlast sender plus drain time):

```sh
# Terminal 1, trusted loopback TCP listener:
/usr/bin/python3 -I -B examples/video_v1.py receiver --transport tcp \
  --delivery preserve_order --seconds 12
# Terminal 2, connecting sender, automatically negotiates ordered mode:
/usr/bin/python3 -I -B examples/video_v1.py sender --transport tcp --seconds 4
```

The sender example calls `finish`; the receiver requires confirmed completion
before its deadline. TCP has four-byte bounded message framing, explicit local
backpressure and TCP_NODELAY, but **no TLS, peer authentication, NAT traversal
or durable storage**. Use trusted networks or a developer-provided secure adapter.

Rootless native regression check:

```sh
/usr/bin/python3 -I -B tools/video_ordered_tcp_smoke.py
```

This checks complete contiguous capture-to-callback delivery over three TCP
paths, deduplication, one-path loss, graceful finish and a separate active-stream
total-disconnect error. It does not establish physical-network reliability or
performance equivalence to latest-frame mode/SRT.

## Contract tests are not a transport certification

The bounded memory adapter in the tests is a fixture, not a production adapter.
Structural validation cannot prove thread safety, encryption, reliable delivery,
reconnection or callback quiescence. Each production adapter needs conformance
tests plus real transfer/fault tests when integrated.
