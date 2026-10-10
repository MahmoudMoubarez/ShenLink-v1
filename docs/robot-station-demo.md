# Headless robot / station browser demo

Robot and station are separate Pi processes using the installed independent
v1 library. No desktop or display server is needed. Robot sends real native
video replay and two simulated position measurements (X/Y) over three loopback
UDP paths. Station sends ordered commands back. The white dot uses only received
sensor values. Left/right changes X; up/down changes Y, bounded to0..100.
The replay content does not change with robot position: this is not a3D game.

Run from Windows PowerShell (keep the terminal open):

```powershell
ssh -t -o ExitOnForwardFailure=yes -i "$env:USERPROFILE\.ssh\xubuntu_lab_ed25519" -L 8765:127.0.0.1:8765 edgelif2@100.75.198.67 '/home/edgelif2/.cache/mfl-video-runtime/standalone-v1-env/bin/python -I -B /home/edgelif2/work/multiflow-v1-release/examples/robot_station_demo.py both --source-replay /home/edgelif2/work/video-test-inputs/replay-61413-fullhd/source.json'
```

Open http://127.0.0.1:8765 in the Windows browser. Use the arrow keys or buttons.
Ctrl+C stops the remote supervisor and its two child processes. Default runtime
limit is1hour. Use the newly prepared Full HD second video, not the older720p manifests.
No sudo, X11 forwarding, RDP or GUI installation required.

The white interface places the large video above three A/B/C lines. Green marks
the source path of the latest JPEG prepared for preview, red marks an actual
packet cut, grey marks an available path. Status polling and browser buffering
mean the indicator is not exact browser presentation-time synchronization.

Set Cuts per second (0..5), then Start. The station scheduler cuts the path that
produced the most recent fresh preview, for100ms, restoring it automatically.
Only one cut can be active. The next path is selected by the SDK, never by the
fault injector. Late ticks do not create catch-up bursts. Stale preview sources
cause skipped ticks; requested frequency is not guaranteed under system load.
Stop / restore all cancels cuts and restores all paths immediately. Automatic
cuts are off at startup and continue server-side until stopped or demo exits.

Cuts drop all packets in both directions for video, sensors, commands, probes
and receipts on that path via owned UDP relays. Forward/drop and cut counters
count actual events. Preview source-change counts may also include normal SDK
selection/quality changes; they are not a one-to-one count of outage failovers.
These are simulated loopback packet blackholes, not physical interface failures.

Minimum resolution is320p (568x320), maximum is Full HD1080p (1920x1080).
The input remains the native1080p second video; only outgoing profiles are scaled
down, never upscaled. Adaptive bitrate ranges from300 to6000kbps at25fps.
Browser JPEGs retain the selected frame resolution, quality85.
The GUI also accepts Minimum resolution (p) / Maximum resolution (p), with
Apply quality. Values are vertical pixels, even integers144..1080, minimum <=
maximum. Width is computed at the native16:9 aspect ratio (even-rounded). The
SDK may round a negotiated profile upwards within the bounds; the displayed
actual dimensions are authoritative. Bitrate floors scale with minimum pixel
count, up to the unchanged6000kbps ceiling.

Applying bounds stops automatic cuts and restores paths, creates a new real
receiver endpoint and re-negotiates with the existing sender. Video pauses
briefly, the recorded replay restarts from the beginning, and freshness indicators
reset honestly. Commands and sensors retain their separate endpoints. Applying
is asynchronous; the GUI shows applying / ready and rejects overlapping requests.
If application fails, rollback attempts the last accepted bounds and reports the
error, rather than pretending the requested bounds are active. Settings are
session-local; a complete demo restart restores320..1080 defaults. Resume cuts
explicitly with Start after application is ready.
This increases network/CPU requirements relative to720p. Native source preparation
uses tools/prepare_demo_full_hd.py and a separate replay-61413-fullhd directory;
existing benchmark inputs and library settings are unchanged.

HTTP binds only127.0.0.1. SSH protects the browser-to-station hop. Commands require
a session token and same-origin requests. Do not expose this test server on a
public IP; it is not an authenticated production web gateway. Library UDP/TCP
defaults are not encrypted. This demo's UDP routes are all loopback, not three
physical Wi-Fi/5G/satellite networks and do not emulate differing capacities.

Station decodes library frames, then one bounded latest-frame worker generates
JPEG for browser display. No image conversion in the library callback. The browser
hop adds conversion/rendering latency: its smoothness is not the accepted SDK/SRT
latency metric. Stale/disconnected readings are labelled, never painted as live.
Command receipts are receiver acceptance, not execution; location feedback proves
the simulated motion independently. Both processes release codecs, mmap and sockets
on exit. No library engine changes or artificial failover counters are included.

## Automatic-cut smoke check

Pi check: native1080p second replay, two requested cuts/second for approximately
eight seconds. Observed15 actual cuts, no skipped ticks, all three frame-source
paths,24 preview source changes and42 new JPEG previews during the interval.
Video, sensor delivery, Stop/restoration and clean process shutdown passed;
15 unit checks passed. This is functional evidence, not a smoothness benchmark.
Maximum server JPEG-generation gap was375ms including startup. Browser buffering,
JPEG conversion and concurrent Pi workloads affect visible smoothness; do not
claim interruption-free display or use these values as SDK transport latency.

Quality-control native smoke check:720..720 negotiated1280x720, then320..720
and320..1080 delivered frames within each range. Automatic cuts stopped during
application, paths restored, sensors continued, commands still moved the dot,
cuts resumed on request, and shutdown passed without errors.25 unit checks passed.
