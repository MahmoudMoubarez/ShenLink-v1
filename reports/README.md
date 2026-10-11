# ShenLink test reports: video freshness and path outages

[Download PDF](ShenLink-test-report.pdf) | [Raw evidence](raw/) | [Derived CSV](capacity-results.csv) | [Checksums](evidence-manifest.json)

## Executive summary

ShenLink is a reusable multipath communications foundation for video, commands and sensors. This report publishes historical video measurements against a specific native SRT Broadcast configuration, together with the underlying archived summaries.

During reduced capacity, mean latest-capture lag was 33.53 ms for ShenLink versus 158.56 ms for SRT: 78.85% lower across two repetitions per compared variant.

A separate individual-path outage run recorded a largest fresh-update gap of 87.75 ms for ShenLink and zero gaps above 100 ms. These observations are specific to the measured lab runs, not a promise of zero latency, zero packet loss or uninterrupted operation under all failures.

Experiments: 8 October 2026. Report compiled: 11 October 2026. Run IDs: c548c522 and c548c523. No new performance experiment was conducted for this report.

## Method and measurement boundaries

Platform: Linux aarch64, kernel 6.8.0-1065-raspi. Three routed namespace paths shaped with Linux kernel tools independently of the SDK; not three physical radios. This report does not establish a two-physical-computer video comparison.

Source: synthetic moving zone plate, 720p at 30 FPS, same source for compared candidates. Native H.264 low-latency processing. Path A/B/C uplinks: 6/4/1.4 Mbps; RTT: 10/20/60 ms; jitter: 1/2/5 ms. These are historical scenario profiles, not current demo settings or military-network averages.

ShenLink: public video endpoints, dynamic frame-rate-first policy, negotiated 24-30 FPS and 360p-720p range. SRT: native 1.5.3 Broadcast group, three connected members, fixed 720p at 1800 kbps, configured latency buffer 120 ms and peer idle 60 s.

Latest-capture lag is the time-weighted timestamp difference between the newest captured source and the freshest application-decoded frame; silence is included. It is not RTT, exact path-switch time, command-response time or camera-to-screen latency.

Fresh-update gap is elapsed time without a newer public decoded frame. Frame age and latest-capture lag are different metrics. Quality upgrades/duplicates are not additional captured frames. No image-fidelity score was collected in these runs.

## Capacity reduction - c548c522

Path A was reduced from 6 to 1.3 Mbps, then restored. Nominal steady windows: initial 6 s, degraded 8 s, restored 6 s, plus verified transition intervals. Two trials per variant; comparison order was reversed in the second block. The full raw run includes four variants and eight VALID trials; the headline compares frame-rate-first with SRT-high.

The table reports the arithmetic mean of the two per-trial time-weighted latest-capture lag values, in milliseconds. Values are not pooled across every timestamp or across phases.

Reduced-capacity repetitions: ShenLink 33.1524 and 33.9121 ms; SRT 158.3920 and 158.7263 ms. Calculation: 100 x (1 - mean(ShenLink) / mean(SRT)) = 78.8519%.

SRT restored-window lag varied from 177.3972 to 1409.5897 ms. Show this variation with the 793.49 ms mean; do not use that mean as a general recovery-time claim. ShenLink adapts quality/FPS while SRT stays fixed at 720p. The improvement is not demonstrated at identical visual quality or against optimally tuned SRT.

| Metric / phase | ShenLink | Tested SRT |
| --- | ---: | ---: |
| Initial | 27.30 | 154.72 |
| Reduced capacity | 33.53 | 158.56 |
| Restored | 46.36 | 793.49 |

## Individual outages - c548c523

One trial per candidate. Cut A, then C, then B, one at a time for 6 s, restoring for 10 s between cuts; two alternatives remain available. Cuts are verified 100% bidirectional router packet loss, not radio carrier failure. Whole-trial measurement is approximately 62 s, including mutation/readback intervals.

ShenLink continued producing fresh frames during the three individual outages. Public video was available in its final restored window. The tested SRT pipeline did not produce public video in its final restored window.

The SRT pipeline stalled for a long interval; the cause remains unresolved and could involve the bridge, decoder or configuration. The later B outage is confounded by that earlier stall and is not an independent comparison. Connected transport members do not prove decoded-video recovery. CPU during that stall is not a fair efficiency comparison.

| Metric / phase | ShenLink | Tested SRT |
| --- | ---: | ---: |
| New decoded frames | 1760 | 849 |
| New-frame FPS | 28.46 | 13.72 |
| Frame age median (ms) | 35.41 | 153.35 |
| Frame age p95 (ms) | 52.09 | 319.26 |
| Largest fresh-update gap (ms) | 87.75 | 33363.58 |
| Gaps above 100 ms | 0 | 2 |

## Evidence, version mapping and limitations

The raw folder contains four unedited archived JSON summaries, including counters, event timing brackets, implementation hashes and measured results. It is not the complete archive, packet capture collection or every per-frame endpoint log. Derived capacity-results.csv permits checking the headline from individual repetitions.

Raw report SHA-256 values are listed in evidence-manifest.json. The source archive checksum was previously verified. Its filename is video-checkpoint-c548c522-c548c523-20261008.tar.gz; it is not bundled here.

These results belong to the implementation hashes recorded in the historical runs. Several measured modules match the later standalone SDK; others, including dual.py, changed afterwards. The current ShenLink 0.1.0rc1 and mission GUI have not been freshly benchmarked against SRT by this report.

Small sample sizes, different buffering/quality policies, unmeasured perceptual quality, unresolved SRT stalls and lab-only networking limit conclusions. No field roaming, security certification, physical drone safety, zero packet loss, universal protocol superiority or guarantee through total disconnection is established.

The current mission GUI uses different uplink profiles (10/6/4 Mbps) and a recorded 360-degree source. Its commands/sensors and automatic cuts are a separate demonstration; do not attach these historical video numbers to that scenario as newly measured outcomes.

Rebuild: install reportlab, then run python reports/build_report.py from the SDK checkout. This recalculates derived values and formats the report; it does not rerun SRT. Historical runner/native bridge dependencies are not packaged here, so full benchmark reproduction is not a one-command capability of this SDK repository.

## Raw file SHA-256

- `c548c522-adaptive-video-summary.json`: `8a983e4e04c223ade22d0268eeef0b5a2f93a1d4614aa2b2f5cef0972e130263`
- `c548c522-latest-frame-summary.json`: `de9710048163d47b62f949884d1cc3cda7024d1f673bd49297addc78ca9d224b`
- `c548c523-adaptive-video-summary.json`: `91095f323e57bddcffa3d3beac14cfdd3e7d39c84d98d62adff251901280c999`
- `c548c523-latest-frame-summary.json`: `231026e8140ac698251f354577723d8b192aa995434b3af283c90a58c6e3ac6e`
