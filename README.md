# ShenLink v1 — release candidate

Independent source project extracted from the verified Linux implementation.
Install with `python -m pip install .`. The distribution name is `ShenLink`.
Canonical public APIs:

- `shenlink.v1.video`: VideoSender/VideoReceiver; adaptive quality and FPS.
- `shenlink.v1`: CommandSender/CommandReceiver; SensorSender/SensorReceiver.
- Shared opaque TransportAdapter contract and UDP/TCP implementations.

```python
from shenlink.v1 import CommandSender, SensorReceiver, UdpTransport
from shenlink.v1.video import VideoSender, VideoReceiver
```

The legacy `multiflow_comm.v1` and `video_stream.v1` imports remain supported
and reference the exact same classes. Existing demos need no import migration.
This branding change does not modify wire formats, delivery logic or timings.
The folder name and historical extraction manifest are retained for provenance.
When migrating an environment installed under the old `multiflow-v1` distribution
name, uninstall that distribution before installing ShenLink; both distributions
own the same compatibility modules and should not be installed side by side.

Each endpoint owns separate adapter instances. UDP/TCP defaults are not encrypted
or authenticated; use a trusted network or implement a secure adapter. Command
receipts mean receiver acceptance, not actuator execution. Sensors are latest-only,
not an all-history guarantee. Old root APIs are intentionally not exported.

Video requires OS-installed PyGObject/GStreamer (Gst, GstApp, GstVideo), H264
encoder/decoder plugins and Pillow. Python pip installation alone does not install
that native runtime. On Linux use a Python environment that can import `gi`.
See docs and the two runnable sender/receiver examples. Sensor usage is in docs.

Tests and benchmarks remain in the original repository; extraction provenance
and source hashes are in extraction-manifest.json. This is not a new SRT/DDS
performance claim. Hardware cameras, Internet paths and target application
integration need their own validation. Before public redistribution, select a
license and confirm third-party license obligations; no license is invented here.

## Local recovery update — 2026-10-10

The working source includes a targeted `messages.py` update: steady-state sender
health probes run independently per path in a dedicated bounded pool, rather
than serially waiting for unavailable paths. Each path retains its configured
probe timeout and interval. Data fanout workers are separate. Health results are
discarded after shutdown or session change; shutdown wakeups are not ACKs.
No video, transport, wire format, receipt semantics or GUI safety limits changed.

The extraction manifest and previously distributed zip describe the original
snapshot, not this locally patched working copy. The previous source is preserved
in the mission demo's `recovery-before-messages.py`. Six recovery regressions,
seven standalone checks and 21 mission tests passed on the Pi; rootless SDK/HTTP
integration also passed with mocked network mutations. Real netem recovery
acceptance remains a user-run check. In the same controlled fake-path experiment,
two delayed-restoration cases improved from approximately 1011/1711 ms to 711/711
ms, retaining the 500 ms timeout. These are not physical-radio performance claims.
