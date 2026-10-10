# Sensor endpoints v1

`multiflow_comm.v1` exports SensorSender / SensorReceiver, frozen settings and
the existing TelemetrySample, MeasurementExpectation and FreshnessPolicy types.
Video, commands and sensors use the identical TransportAdapter contract and
UdpTransport / TcpTransport classes. Each endpoint exclusively owns its own
adapter instances/connections: do not share a live instance across flows.

```python
from multiflow_comm.v1 import (
    SensorSender, SensorReceiver, SensorSenderSettings, SensorReceiverSettings,
    UdpTransport, TelemetrySample, SampleStatus, MeasurementExpectation,
)
import time

# Run the two endpoints on their respective hosts; localhost here is an example.
rx = SensorReceiver(SensorReceiverSettings([
    UdpTransport("a", ("127.0.0.1", 9002), ("127.0.0.1", 9001)),
]), on_observation=lambda obs: print(obs.sample.value_bytes, obs.is_live))
rx.register_measurement("robot", "battery", MeasurementExpectation("voltage", 1, "V", "raw"))
tx = SensorSender(SensorSenderSettings([
    UdpTransport("a", ("127.0.0.1", 9001), ("127.0.0.1", 9002)),
]))
try:
    rx.start()
    tx.start()
    ticket = tx.publish(TelemetrySample(
        "robot", "battery", "boot-1", 1, SampleStatus.GOOD,
        b"12.4", "raw", "voltage", 1, unit="V"))
    time.sleep(1)  # Demo only: production should keep its acquisition loop running.
finally:
    tx.stop()
    rx.stop()
```

Publishing is asynchronous: keep the application running and observe ticket
status; immediate shutdown is not a flush. start() initiates verification,
not a blocking all-path readiness guarantee. path_states reports verified peer
states. A dispatch ticket is not an application receipt or execution guarantee.

The real TelemetryPublisher/Subscriber engines remain unchanged. Default send:
LATEST_ONLY, REDUNDANT, receipt NONE, queue 256, peer probe interval 0.2s.
Pending readings coalesce per source/measurement; this is not an all-deliver
sensor history API. Oversized encoded frames fail through the core's bounded
transport dispatch: the adapter message bound includes metadata and framing,
not just value_bytes. No fragmentation or automatic protocol fallback is added.

Register each measurement's schema/unit/codec before publishing. Unknown
cross-host source age defaults to ACCEPT_WITH_WARNING, not a made-up latency.
Default silence threshold is 1000ms; set an application-specific FreshnessPolicy
and age_estimator for stricter requirements. get_state() reevaluates liveness;
it never pretends the clocks of different devices are synchronized. BAD/stale
observations retain the existing telemetry contract; no new rejection policy.
Source epoch must change on source restart; sequence increases per measurement.

Use start/stop (or context managers), publish(sample), register_measurement,
get_state, get_metrics, on_observation/on_error/on_state/on_session callbacks.
stop is terminal; create fresh endpoints and adapters to restart. This release
does not add processing/network priority configuration or a native DDS backend.

## Accepted baseline (before this facade)

Pi run c548c418, A/C/B individual packet blackholes and restoration, 50Hz,
128-byte values. SDK LATEST_ONLY/REDUNDANT/NONE versus native Cyclone DDS
BEST_EFFORT/VOLATILE/KEEP_LAST(256), three shaped paths (30/30Mbps RTT10ms;
10/50Mbps RTT20ms; 5/25Mbps RTT60ms). All six trials VALID; cleanup PASS.
Full report: /run/mfl-sensor-efficiency/c548c418/outages-summary.json.
Persistent report copy: logs/sensor-baselines/c548c418.json on the Pi.
Evidence: /run/mfl-sensor-efficiency/c548c418/outages-2efb23e82219.

During-fault mean AoI (SDK/DDS): A20.9377/20.3516ms,
C15.9136/15.4260ms, B15.8735/15.5032ms. All generated readings received:
SDK2898/2898, DDS2909/2909. Full-trial maximum fresh gap: SDK23.9083ms,
DDS26.2922ms. Arithmetic mean of per-trial combined endpoint CPU on one-core
scale: SDK8.4010%, DDS3.2687%. Peak sampled combined RSS: SDK38.5065MB,
DDS63.0374MB (decimal MB).

One pair per path is exploratory, not statistical superiority. CPU includes
worker/logging overhead; throughput calibration was not rerun. This baseline
does not prove post-facade performance: integration tests establish behavior,
and a matched rerun is required for numeric regression claims.

## Facade verification

On the Pi, 113 targeted tests and 37 subtests passed: sensor endpoints, command
endpoints, existing telemetry facade/codec/events, video v1 import/API and shared
transport contract. The new sensor tests use three real UDP paths with A/C/B
blackholes and recovery, and three TCP paths; they check unique delivery,
independent measurement freshness, stale-source rejection, unchanged serialized
payload, default policies, no second publish encoding, and terminal cleanup.
These socket tests are rootless loopback, not a fresh shaped DDS benchmark.

Baseline core hashes remain unchanged after deployment:

- messages.py: 1e3a457ef6917d177a07de23e4e92a55e767c9d53bde486f25abf133eaade9cf
- telemetry.py: 1a0209d6db8e9c62898d54e69fb650a2a83a7e8f5314d5d8aabbdce73baa57f0
- scheduler.py: 8075a507847dd5d0d64d12031d3b3f272975090de48a57818707afc75a89dd45
- udp_adapter.py: 6ea5369bf8174d86d54b0a0ac269f5ec83efe4e2524018c0719cf944c88d0cb8
