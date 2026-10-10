#!/usr/bin/env python3
"""Headless robot/station demo using installed v1 endpoints, not a benchmark.

Robot and station are separate processes. HTTP/JPEG is only the browser display
hop; robot video/sensors/commands traverse the library's three UDP paths.
All addresses are loopback. Access the web station through an SSH tunnel.
"""
import argparse
from io import BytesIO
import json
import math
import mmap
from pathlib import Path
import secrets
import select
import signal
import socket
import subprocess
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

DIRECTIONS = {"left": (-5, 0), "right": (5, 0), "up": (0, -5), "down": (0, 5)}


def quality_bounds(minimum, maximum):
    """GUI values are vertical pixels; source is native 1920x1080."""
    if (type(minimum) is not int or type(maximum) is not int
            or not 144 <= minimum <= maximum <= 1080 or minimum % 2 or maximum % 2):
        raise ValueError("Use even resolutions between144 and1080; minimum must not exceed maximum")
    def dimensions(height): return (2*math.ceil(height*16/9/2), height)
    return dimensions(minimum), dimensions(maximum)


class RobotPosition:
    def __init__(self):
        self.lock = threading.Lock()
        self.x = self.y = 50
        self.executed = 0

    def move(self, direction):
        if direction not in DIRECTIONS:
            raise ValueError("Unknown direction")
        dx, dy = DIRECTIONS[direction]
        with self.lock:
            self.x = min(100, max(0, self.x + dx))
            self.y = min(100, max(0, self.y + dy))
            self.executed += 1

    def snapshot(self):
        with self.lock:
            return dict(x=self.x, y=self.y, executed=self.executed)


class ReplaySource:
    """Paced source from the already prepared video; loops content, not time."""
    def __init__(self, manifest):
        path = Path(manifest).resolve()
        value = json.loads(path.read_text())
        if (value.get("format"), value.get("width"), value.get("height"), value.get("fps")) != ("I420", 1920, 1080, 25):
            raise ValueError("Full HD demo requires a prepared 1080p25 I420 source.json; 720p is not upscaled")
        if type(value.get("frames")) is not int or value["frames"] < 30:
            raise ValueError("Insufficient source frames")
        raw_name = value["raw_file"]
        if Path(raw_name).name != raw_name:
            raise ValueError("Unsafe replay filename")
        self.width, self.height, self.fps = 1920, 1080, 25
        self.frames, self.index = value["frames"], 0
        self.stream = (path.parent / raw_name).open("rb")
        if self.stream.seek(0, 2) != self.frames * self.width * self.height * 3 // 2:
            self.stream.close()
            raise ValueError("Replay file size mismatch")
        self.mapped = mmap.mmap(self.stream.fileno(), 0, access=mmap.ACCESS_READ)

    def build_elements(self, pipeline):
        import gi
        gi.require_version("Gst", "1.0")
        from gi.repository import Gst
        self.index = 0  # A new replay pipeline has a new running-time origin.
        size = self.width * self.height * 3 // 2
        source = Gst.ElementFactory.make("appsrc", "demo-replay")
        source.set_property("is-live", True)
        source.set_property("format", Gst.Format.TIME)
        source.set_property("block", True)
        source.set_property("max-bytes", size * 2)
        source.set_property("caps", Gst.Caps.from_string("video/x-raw,format=I420,width=1920,height=1080,framerate=25/1"))
        def need_data(appsrc, _length):
            position = self.index % self.frames
            buffer = Gst.Buffer.new_allocate(None, size, None)
            buffer.fill(0, self.mapped[position*size:(position+1)*size])
            buffer.pts = buffer.dts = self.index * Gst.SECOND // self.fps
            buffer.duration = (self.index+1)*Gst.SECOND//self.fps - buffer.pts
            self.index += 1
            appsrc.emit("push-buffer", buffer)
        source.connect("need-data", need_data)
        pace = Gst.ElementFactory.make("identity", "demo-pace")
        pace.set_property("sync", True)
        convert = Gst.ElementFactory.make("videoconvert", "demo-convert")
        caps = Gst.ElementFactory.make("capsfilter", "demo-caps")
        caps.set_property("caps", Gst.Caps.from_string("video/x-raw,format=RGB,width=1920,height=1080,framerate=25/1"))
        for element in (source, pace, convert, caps): pipeline.add(element)
        if not (source.link(pace) and pace.link(convert) and convert.link(caps)):
            raise RuntimeError("Cannot build replay pipeline")
        return source, caps

    def close(self):
        self.mapped.close()
        self.stream.close()


def paths(flow, sender, base):
    from multiflow_comm.v1 import UdpTransport
    offset = {"video": 0, "sensors": 6, "commands": 12}[flow]
    return [UdpTransport(name, ("127.0.0.1", base+offset+2*i+(0 if sender else 1)),
                         ("127.0.0.1", base+18+offset//2+i))
            for i, name in enumerate("abc")]


class PathRelays:
    """Owned loopback fault injector; cuts packets, never the management HTTP."""
    def __init__(self, base):
        self.base = base
        self.lock = threading.Lock()
        self.stopped = threading.Event()
        self.entries = {}
        self.thread = None
        self.state = {p: dict(enabled=True, forwarded=0, dropped=0, changes=0) for p in "abc"}

    def start(self):
        try:
            for offset in (0, 6, 12):
                for i, path in enumerate("abc"):
                    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                    self.entries[sock] = (path, ("127.0.0.1", self.base+offset+2*i),
                                          ("127.0.0.1", self.base+offset+2*i+1))
                    sock.bind(("127.0.0.1", self.base+18+offset//2+i))
                    sock.setblocking(False)
            self.thread = threading.Thread(target=self.run, daemon=True)
            self.thread.start()
        except Exception:
            self.stop()
            raise

    def run(self):
        while not self.stopped.is_set():
            ready, _, _ = select.select(list(self.entries), [], [], .05)
            for sock in ready:
                for _ in range(64):
                    try: data, peer = sock.recvfrom(65535)
                    except BlockingIOError: break
                    path, first, second = self.entries[sock]
                    if peer not in (first, second): continue
                    with self.lock:
                        state = self.state[path]
                        if not state["enabled"]:
                            state["dropped"] += 1
                            continue
                        try: sock.sendto(data, second if peer == first else first)
                        except OSError:
                            state["dropped"] += 1
                        else: state["forwarded"] += 1

    def set_enabled(self, path, enabled):
        if path not in self.state or type(enabled) is not bool:
            raise ValueError("Use path a/b/c and a boolean enabled")
        with self.lock:
            state = self.state[path]
            if state["enabled"] != enabled:
                state["changes"] += 1
                state["enabled"] = enabled

    def snapshot(self):
        with self.lock: return {p: dict(s) for p, s in self.state.items()}

    def stop(self):
        self.stopped.set()
        if self.thread is not None: self.thread.join(3)
        for sock in self.entries: sock.close()
        self.entries.clear()


def stop_event():
    stopped = threading.Event()
    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, lambda *_: stopped.set())
    return stopped


class AutomaticCuts:
    """Demo faults only. The SDK, not this controller, selects replacement frames."""
    duration = .1

    def __init__(self, relays, active_path):
        self.relays, self.active_path = relays, active_path
        self.lock = threading.Lock()
        self.rate = 0.0
        self.next_due = self.restore_at = None
        self.cut_path = None
        self.cuts = self.skipped = 0
        self.events = []

    def configure(self, rate, now=None):
        if type(rate) not in (int, float) or not math.isfinite(rate) or not 0 <= rate <= 5:
            raise ValueError("Cuts per second must be between 0 and 5")
        now = time.monotonic() if now is None else now
        with self.lock:
            for path in "abc": self.relays.set_enabled(path, True)
            self.rate = float(rate)
            self.next_due = now + 1/rate if rate else None
            self.cut_path = self.restore_at = None

    def step(self, now=None):
        now = time.monotonic() if now is None else now
        with self.lock:
            if self.cut_path is not None and now >= self.restore_at:
                self.relays.set_enabled(self.cut_path, True)
                self.cut_path = self.restore_at = None
            if not self.rate or now < self.next_due: return
            # No catch-up bursts, overlap or cuts before a real fresh preview exists.
            self.next_due = now + 1/self.rate
            path = self.active_path()
            states = self.relays.snapshot()
            if self.cut_path is not None or path not in states or not all(s["enabled"] for s in states.values()):
                self.skipped += 1
                return
            self.relays.set_enabled(path, False)
            self.cut_path, self.restore_at = path, now + self.duration
            self.cuts += 1
            self.events = (self.events + [dict(path=path, at_monotonic=now)])[-20:]

    def snapshot(self):
        with self.lock:
            return dict(rate_hz=self.rate, cut_duration_ms=100, cuts=self.cuts,
                        skipped=self.skipped, cut_path=self.cut_path, events=list(self.events))


def robot(args):
    from multiflow_comm.v1 import (SensorSender, SensorSenderSettings, CommandReceiver,
        CommandReceiverSettings, TelemetrySample, SampleStatus)
    from video_stream.v1 import VideoSender, SenderSettings, CameraSource
    stopped, model = stop_event(), RobotPosition()
    source = ReplaySource(args.source_replay) if args.source_replay else CameraSource(test_pattern=True, width=1920, height=1080, fps=25)
    errors = []
    def error(exc):
        errors.append(str(exc)); stopped.set()
    def command(value):
        try: model.move(value.decode_object()["direction"])
        except Exception as exc: error(exc)
    video = VideoSender(SenderSettings(source, paths("video", True, args.base_port), min_fps=25), on_error=error)
    sensors = SensorSender(SensorSenderSettings(paths("sensors", True, args.base_port)), on_error=error)
    commands = CommandReceiver(CommandReceiverSettings(paths("commands", False, args.base_port)), on_command=command, on_error=error)
    epoch, sequence = secrets.token_hex(8), 0
    try:
        commands.start(); sensors.start(); video.start()
        print(json.dumps(dict(role="robot", status="READY", replay=bool(args.source_replay))), flush=True)
        until = time.monotonic() + args.seconds
        while not stopped.is_set() and time.monotonic() < until:
            sequence += 1
            state = model.snapshot()
            for axis in ("x", "y"):
                sensors.publish(TelemetrySample("robot", axis, epoch, sequence, SampleStatus.GOOD,
                    json.dumps(state[axis]).encode(), "json", "demo.coordinate", 1, unit="percent"))
            if sequence % 5 == 0:
                counters = video.counters()["video"]
                primary = counters.get("adaptation", {}).get("primary") if counters else None
                if primary in ("a", "b", "c"):
                    sensors.publish(TelemetrySample("robot", "video_primary", epoch, sequence,
                        SampleStatus.GOOD, json.dumps(primary).encode(), "json", "demo.video_path", 1))
            stopped.wait(.1)
    finally:
        for endpoint in (video, sensors, commands): endpoint.stop()
        if isinstance(source, ReplaySource): source.close()
    print(json.dumps(dict(role="robot", status="STOPPED", position=model.snapshot(), errors=errors)), flush=True)
    if errors: raise RuntimeError(errors[0])


HTML = '''<!doctype html><html lang="en"><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Robot · Live Station</title><style>
*{box-sizing:border-box}body{margin:0;background:white;color:#222;font:16px system-ui}main{max-width:1250px;margin:auto;padding:20px}
header{display:flex;justify-content:space-between;gap:16px;align-items:center}h1{font-size:24px}small{color:#666}section{display:block}
.card{padding:10px 0}.video{display:block;width:100%;aspect-ratio:16/9;object-fit:contain;background:#eee}
canvas{width:180px;background:#f5f5f5}button,input{background:white;color:#222;border:1px solid #aaa;border-radius:4px;padding:8px;font-size:16px}button{cursor:pointer}
.controls{display:flex;gap:8px;align-items:center;margin:10px 0;flex-wrap:wrap}.stat{padding:8px 0}.good{color:#16803c}.bad{color:#c62828}
.tracks{display:grid;grid-template-columns:repeat(3,1fr);gap:20px;margin:18px 0}.track{border-top:8px solid #bbb;padding-top:8px}.track.active{border-color:#16803c}.track.cut{border-color:#d32f2f}.secondary{display:flex;gap:20px;align-items:center;border-top:1px solid #ddd;margin-top:20px}
@media(max-width:700px){main{padding:12px}.secondary{flex-wrap:wrap}}</style>
<main><header><div><h1>ROBOT / LIVE STATION</h1><small>Recorded-video replay · simulated position · real library transport</small></div><b id="connected">Starting…</b></header>
<section><div class="card"><img class="video" src="/stream" alt="Waiting for robot video"><div class="tracks"><div class="track" data-track="a">A</div><div class="track" data-track="b">B</div><div class="track" data-track="c">C</div></div><div class="controls"><label>Cuts per second <input id="rate" type="number" min="0" max="5" step="0.5" value="1"></label><button id="start">Start</button><button id="stop">Stop / restore all</button><span id="auto">Stopped</span></div><div class="stat" id="video">Waiting for decoded frames</div><div id="faults"></div><small>Green: sender's high-quality primary path. Red: cut. Grey: available. The latest decoded preview frame may come from another path; its source is shown separately below. Automatic cuts target the latest preview frame source. Each cut lasts 100ms. Simulated local paths, not three physical networks. Browser JPEG rendering adds latency; this display is not a performance benchmark.</small></div>
<div class="secondary"><canvas id="map" width="360" height="240"></canvas><div><div class="stat" id="position">X — / Y —</div><div class="stat" id="samples">Waiting for two location sensors</div>
<div class="controls"><span></span><button data-dir="up">↑</button><span></span><button data-dir="left">←</button><button data-dir="down">↓</button><button data-dir="right">→</button></div>
<p><small>Use arrow keys or buttons. The dot reflects received sensor data, not a browser prediction.</small></p><div id="command">No command sent</div></div></div></section>
<div class="controls"><label>Minimum resolution (p) <input id="quality-min" type="number" min="144" max="1080" step="2" value="320"></label><label>Maximum resolution (p) <input id="quality-max" type="number" min="144" max="1080" step="2" value="1080"></label><button id="apply-quality">Apply quality</button><span id="quality-status">320p minimum / 1080p maximum</span></div><small>Apply restarts video negotiation briefly and stops automatic cuts. Source stays1080p; it is never upscaled. Faults affect all three data flows.</small>
<p id="paths"></p><p class="bad" id="errors"></p></main><script>
const token='__TOKEN__',canvas=document.getElementById('map'),ctx=canvas.getContext('2d');let pending=false;
async function move(direction){if(pending)return;pending=true;try{const r=await fetch('/command',{method:'POST',headers:{'Content-Type':'application/json','X-Demo-Token':token},body:JSON.stringify({direction})});const s=await r.json();document.getElementById('command').textContent=r.ok?'Command queued: '+direction:s.error;}catch(e){document.getElementById('command').textContent='Station unreachable';}finally{pending=false;}}
document.querySelectorAll('[data-dir]').forEach(b=>b.onclick=()=>move(b.dataset.dir));document.addEventListener('keydown',e=>{const d={ArrowLeft:'left',ArrowRight:'right',ArrowUp:'up',ArrowDown:'down'}[e.key];if(d){e.preventDefault();if(!e.repeat)move(d);}});
async function configure(rate){try{const r=await fetch('/auto',{method:'POST',headers:{'Content-Type':'application/json','X-Demo-Token':token},body:JSON.stringify({rate_hz:rate})});if(!r.ok)document.getElementById('errors').textContent=(await r.json()).error;}catch(e){document.getElementById('errors').textContent='Fault control unavailable';}}
document.getElementById('start').onclick=()=>configure(Number(document.getElementById('rate').value));document.getElementById('stop').onclick=()=>configure(0);
document.getElementById('apply-quality').onclick=async()=>{try{const r=await fetch('/quality',{method:'POST',headers:{'Content-Type':'application/json','X-Demo-Token':token},body:JSON.stringify({min_p:Number(document.getElementById('quality-min').value),max_p:Number(document.getElementById('quality-max').value)})});const s=await r.json();if(!r.ok)document.getElementById('errors').textContent=s.error;}catch(e){document.getElementById('errors').textContent='Quality control unavailable';}};
async function update(){try{const r=await fetch('/status',{cache:'no-store'}),s=await r.json();const live=s.video_silence_ms!==null&&s.video_silence_ms<1500&&s.sensor_silence_ms.x!==null&&s.sensor_silence_ms.x<1500&&s.sensor_silence_ms.y!==null&&s.sensor_silence_ms.y<1500;
const c=document.getElementById('connected');c.textContent=live?'LIVE':'WAITING / STALE';c.className=live?'good':'bad';document.getElementById('position').textContent='X '+(s.coordinates.x??'—')+' / Y '+(s.coordinates.y??'—');
document.getElementById('video').textContent='Decoded frames: '+s.frames+' · last frame '+(s.frame_id??'—')+' · '+(s.dimensions||[]).join(' × ');
document.getElementById('samples').textContent='Sensor updates: '+s.sensor_updates+' · last sequences X/Y '+s.sequences.x+'/'+s.sequences.y;
document.getElementById('quality-status').textContent=s.quality.status+' · '+s.quality.min_p+'p–'+s.quality.max_p+'p'+(s.quality.error?' · '+s.quality.error:'');document.getElementById('apply-quality').disabled=s.quality.status!=='ready';document.getElementById('start').disabled=s.quality.status!=='ready';
document.getElementById('paths').textContent='Command paths: '+Object.entries(s.path_states).map(([k,v])=>k.toUpperCase()+': '+v).join(' | ');
document.querySelectorAll('[data-track]').forEach(b=>{const k=b.dataset.track,p=s.paths[k],active=s.sender_primary===k&&s.sender_primary_silence_ms!==null&&s.sender_primary_silence_ms<2000;b.className='track '+(!p.enabled?'cut':active?'active':'');b.textContent=k.toUpperCase()+' · '+(!p.enabled?'CUT':active?'HIGH-QUALITY PRIMARY':'AVAILABLE');});document.getElementById('auto').textContent=s.auto.rate_hz?s.auto.rate_hz+' cuts/s · 100ms each':'Stopped';document.getElementById('faults').textContent='Primary changes: '+s.sender_primary_changes+' · latest preview frame source: '+(s.display_path??'—').toUpperCase()+' · preview source changes: '+s.display_switches+' · actual cuts: '+s.auto.cuts+' · dropped packets: '+Object.values(s.paths).reduce((n,p)=>n+p.dropped,0);
document.getElementById('errors').textContent=s.errors.join(' · ');if(s.last_receipt)document.getElementById('command').textContent='Receiver acceptance: '+s.last_receipt+' (not execution proof)';
ctx.clearRect(0,0,360,240);ctx.strokeStyle='#ddd';for(let x=0;x<360;x+=36){ctx.beginPath();ctx.moveTo(x,0);ctx.lineTo(x,240);ctx.stroke();}for(let y=0;y<240;y+=24){ctx.beginPath();ctx.moveTo(0,y);ctx.lineTo(360,y);ctx.stroke();}if(s.coordinates.x!==null&&s.coordinates.y!==null){ctx.fillStyle=live?'#222':'#aaa';ctx.beginPath();ctx.arc(12+s.coordinates.x*3.36,12+s.coordinates.y*2.16,8,0,Math.PI*2);ctx.fill();}
}catch(e){document.getElementById('connected').textContent='DISCONNECTED';}finally{setTimeout(update,50);}}update();</script></html>'''


def station(args):
    from multiflow_comm.v1 import (SensorReceiver, SensorReceiverSettings, MeasurementExpectation,
        CommandSender, CommandSenderSettings)
    from video_stream.v1 import VideoReceiver, ReceiverSettings, ReceiverRequirements
    stopped = stop_event()
    relays = PathRelays(args.base_port)
    lock = threading.Condition()
    state = dict(coordinates=dict(x=None, y=None), sequences=dict(x=0, y=0),
        sensor_updates=0, frames=0, frame_id=None, dimensions=None, errors=[], last_receipt=None,
        display_path=None, display_switches=0, preview_frames=0, max_preview_gap_ms=0,
        sender_primary=None, sender_primary_changes=0)
    state["quality"] = dict(min_p=320, max_p=1080, status="starting", error=None)
    quality_event = threading.Event()
    quality_request = [None]
    video_guard = threading.Lock()
    last_sensor = dict(x=None, y=None)
    last_primary = [None]
    newest = [None]; jpeg = [None, 0]; last_frame = [None]; last_preview = [None]
    render_generation = [0]
    def active_path():
        with lock:
            if last_preview[0] is None or time.monotonic()-last_preview[0] > .5: return None
            return state["display_path"]
    automatic = AutomaticCuts(relays, active_path)
    def fault_worker():
        while not stopped.wait(.01): automatic.step()
    def error(exc):
        with lock:
            state["errors"] = (state["errors"] + [str(exc)])[-5:]
    def frame(value):
        with lock:
            newest[0] = value
            last_frame[0] = time.monotonic()
            state.update(frames=state["frames"]+1, frame_id=value.frame_id, dimensions=[value.width,value.height])
            lock.notify_all()
    def observation(value):
        axis = value.sample.measurement_id
        try:
            coordinate = json.loads(value.sample.value_bytes)
        except (ValueError, TypeError) as exc:
            error(exc); return
        if axis == "video_primary":
            if coordinate not in ("a", "b", "c"):
                error(ValueError("Invalid sender primary")); return
            with lock:
                previous = state["sender_primary"]
                if previous is not None and previous != coordinate:
                    state["sender_primary_changes"] += 1
                state["sender_primary"] = coordinate
                last_primary[0] = time.monotonic()
            return
        if axis not in last_sensor or type(coordinate) not in (int,float) or not math.isfinite(coordinate) or not 0 <= coordinate <= 100:
            error(ValueError("Invalid coordinate")); return
        with lock:
            state["coordinates"][axis] = coordinate
            state["sequences"][axis] = value.sample.sample_sequence
            state["sensor_updates"] += 1
            last_sensor[axis] = time.monotonic()
    def make_video(minimum, maximum):
        low, high = quality_bounds(minimum, maximum)
        bitrate = round(300*(minimum/320)**2)
        return VideoReceiver(ReceiverSettings(paths("video", False, args.base_port),
            requirements=ReceiverRequirements(min_resolution=low,max_resolution=high,min_fps=25,max_fps=25,
                min_bitrate_kbps=max(300,bitrate),max_bitrate_kbps=6000)), on_frame=frame, on_error=error)
    video = make_video(320,1080)
    def quality_worker():
        nonlocal video
        while not stopped.is_set():
            if not quality_event.wait(.2): continue
            quality_event.clear()
            if stopped.is_set(): break
            with lock:
                request, quality_request[0] = quality_request[0], None
                old = (state["quality"]["min_p"], state["quality"]["max_p"])
            if request is None: continue
            automatic.configure(0)
            try:
                with video_guard:
                    video.stop()
                    with lock:
                        render_generation[0] += 1
                        newest[0] = last_frame[0] = last_preview[0] = None
                        jpeg[0] = None
                        state["display_path"] = None
                    video = make_video(*request)
                    video.start()
                with lock:
                    state["quality"] = dict(min_p=request[0], max_p=request[1], status="ready", error=None)
            except Exception as exc:
                # Explicit rollback, never silently report failed bounds as active.
                try:
                    with video_guard:
                        video.stop()
                        video = make_video(*old)
                        video.start()
                    with lock:
                        state["quality"] = dict(min_p=old[0],max_p=old[1],status="ready",error="Apply failed; old bounds restored: "+str(exc))
                except Exception as rollback:
                    with lock:
                        state["quality"]["status"] = "failed"
                        state["quality"]["error"] = str(rollback)
                    error(rollback)
    sensors = SensorReceiver(SensorReceiverSettings(paths("sensors", False, args.base_port)), on_observation=observation,on_error=error)
    for axis in ("x", "y"):
        sensors.register_measurement("robot",axis,MeasurementExpectation("demo.coordinate",1,"percent","json"))
    sensors.register_measurement("robot", "video_primary", MeasurementExpectation("demo.video_path", 1))
    commands = CommandSender(CommandSenderSettings(paths("commands", True, args.base_port)), on_error=error)
    token = secrets.token_hex(24)
    def renderer():
        while not stopped.is_set():
            with lock:
                lock.wait_for(lambda: newest[0] is not None or stopped.is_set(), .5)
                value, newest[0] = newest[0], None
                generation = render_generation[0]
            if value is None: continue
            try:
                image = value.to_image().convert("RGB")
                output = BytesIO(); image.save(output,format="JPEG",quality=85)
                with lock:
                    if generation != render_generation[0]: continue
                    now = time.monotonic()
                    if last_preview[0] is not None:
                        state["max_preview_gap_ms"] = max(state["max_preview_gap_ms"], (now-last_preview[0])*1000)
                    if state["display_path"] is not None and state["display_path"] != value.path_id:
                        state["display_switches"] += 1
                    state["display_path"] = value.path_id
                    state["preview_frames"] += 1
                    last_preview[0] = now
                    jpeg[0], jpeg[1] = output.getvalue(), jpeg[1]+1
                    lock.notify_all()
            except Exception as exc: error(exc)
            stopped.wait(.05)
    class Handler(BaseHTTPRequestHandler):
        def setup(self):
            self.request.settimeout(5)
            super().setup()
        def log_message(self, *_): pass
        def allowed_host(self):
            return self.headers.get("Host") in (f"127.0.0.1:{args.http_port}", f"localhost:{args.http_port}")
        def reply(self, status, value, content_type="application/json"):
            data = value if isinstance(value,bytes) else json.dumps(value).encode()
            self.send_response(status)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(data)))
            self.send_header("Cache-Control","no-store")
            self.send_header("X-Content-Type-Options","nosniff")
            self.send_header("X-Frame-Options","DENY")
            self.end_headers(); self.wfile.write(data)
        def do_GET(self):
            if not self.allowed_host(): return self.reply(403,dict(error="Invalid host"))
            if self.path == "/": return self.reply(200,HTML.replace("__TOKEN__",token).encode(),"text/html; charset=utf-8")
            if self.path == "/status":
                with lock:
                    value = json.loads(json.dumps(state))
                    now = time.monotonic()
                    value["video_silence_ms"] = None if last_frame[0] is None else (now-last_frame[0])*1000
                    value["sensor_silence_ms"] = {k:None if t is None else (now-t)*1000 for k,t in last_sensor.items()}
                    value["preview_silence_ms"] = None if last_preview[0] is None else (now-last_preview[0])*1000
                    value["sender_primary_silence_ms"] = None if last_primary[0] is None else (now-last_primary[0])*1000
                value["path_states"] = {p:s.value for p,s in commands.path_states.items()}
                value["paths"] = relays.snapshot()
                value["auto"] = automatic.snapshot()
                return self.reply(200,value)
            if self.path != "/stream": return self.reply(404,dict(error="Not found"))
            self.connection.settimeout(2)
            self.connection.setsockopt(socket.SOL_SOCKET,socket.SO_SNDBUF,32768)
            self.send_response(200); self.send_header("Content-Type","multipart/x-mixed-replace; boundary=frame")
            self.send_header("Cache-Control","no-store"); self.end_headers()
            last = -1
            try:
                while not stopped.is_set():
                    with lock:
                        lock.wait_for(lambda: jpeg[1] != last or stopped.is_set(), .5)
                        data, number = jpeg
                    if data is None or number == last: continue
                    self.wfile.write(b"--frame\r\nContent-Type: image/jpeg\r\nContent-Length: "+str(len(data)).encode()+b"\r\n\r\n"+data+b"\r\n")
                    self.wfile.flush(); last = number
            except (OSError,TimeoutError): pass
        def do_POST(self):
            if not self.allowed_host(): return self.reply(403,dict(error="Invalid host"))
            if self.path not in ("/command", "/path", "/auto", "/quality"): return self.reply(404,dict(error="Not found"))
            if self.headers.get("X-Demo-Token") != token: return self.reply(403,dict(error="Invalid token"))
            origin = self.headers.get("Origin")
            if origin and origin != "http://"+self.headers.get("Host",""): return self.reply(403,dict(error="Invalid origin"))
            try:
                length = int(self.headers.get("Content-Length","0"))
                if not 1 <= length <= 256: raise ValueError("Invalid body size")
                self.connection.settimeout(2)
                body = json.loads(self.rfile.read(length))
                if self.path == "/quality":
                    minimum, maximum = body["min_p"], body["max_p"]
                    quality_bounds(minimum, maximum)
                    with lock:
                        if state["quality"]["status"] != "ready":
                            return self.reply(409,dict(error="Video configuration busy"))
                        state["quality"]["status"] = "applying"
                        quality_request[0] = (minimum, maximum)
                        quality_event.set()
                    return self.reply(202,dict(status="APPLYING"))
                if self.path == "/auto":
                    with lock:
                        if body["rate_hz"] != 0 and state["quality"]["status"] != "ready":
                            return self.reply(409,dict(error="Wait for video configuration"))
                    automatic.configure(body["rate_hz"])
                    return self.reply(200,automatic.snapshot())
                if self.path == "/path":
                    automatic.configure(0)
                    relays.set_enabled(body["path"], body["enabled"])
                    return self.reply(200,dict(paths=relays.snapshot()))
                direction = body["direction"]
                if direction not in DIRECTIONS: raise ValueError("Unknown direction")
                ticket = commands.send_object(dict(direction=direction))
                def done(value):
                    with lock: state["last_receipt"] = value.status.value
                ticket.add_done_callback(done)
                self.reply(202,dict(status="QUEUED",direction=direction))
            except (KeyError,ValueError,TypeError) as exc: self.reply(400,dict(error=str(exc)))
            except Exception as exc: self.reply(503,dict(error=str(exc)))
    class Server(ThreadingHTTPServer):
        daemon_threads = True
    server = Server(("127.0.0.1",args.http_port),Handler)
    render_thread = threading.Thread(target=renderer,daemon=True)
    http_thread = threading.Thread(target=server.serve_forever,daemon=True)
    fault_thread = threading.Thread(target=fault_worker,daemon=True)
    quality_thread = threading.Thread(target=quality_worker,daemon=True)
    try:
        relays.start()
        sensors.start(); commands.start()
        render_thread.start(); http_thread.start()
        fault_thread.start()
        print(json.dumps(dict(role="station",url=f"http://127.0.0.1:{args.http_port}",status="STARTING")),flush=True)
        with video_guard: video.start()
        with lock: state["quality"]["status"] = "ready"
        quality_thread.start()
        stopped.wait(args.seconds)
    finally:
        stopped.set()
        with lock: lock.notify_all()
        if http_thread.is_alive(): server.shutdown()
        server.server_close()
        if fault_thread.is_alive(): fault_thread.join(3)
        automatic.configure(0)
        quality_event.set()
        if quality_thread.is_alive(): quality_thread.join(15)
        with video_guard: video.stop()
        for endpoint in (sensors,commands): endpoint.stop()
        relays.stop()
        if render_thread.is_alive(): render_thread.join(3)
        http_thread.join(3) if http_thread.is_alive() else None
    print(json.dumps(dict(role="station",status="STOPPED",frames=state["frames"],sensor_updates=state["sensor_updates"],errors=state["errors"])),flush=True)
    if state["errors"]: raise RuntimeError(state["errors"][0])


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("role",choices=("robot","station","both"))
    parser.add_argument("--source-replay",type=Path)
    parser.add_argument("--seconds",type=float,default=3600)
    parser.add_argument("--base-port",type=int,default=19000)
    parser.add_argument("--http-port",type=int,default=8765)
    args = parser.parse_args()
    if not math.isfinite(args.seconds) or args.seconds <= 0 or not 1024 <= args.base_port <= 65509 or not 1024 <= args.http_port <= 65535:
        parser.error("Invalid duration or port")
    if args.base_port <= args.http_port < args.base_port+27: parser.error("HTTP port overlaps transport ports")
    if args.role != "both": return robot(args) if args.role == "robot" else station(args)
    stopped = stop_event(); children = []
    try:
        for role in ("station","robot"):
            command = [sys.executable,"-I","-B",str(Path(__file__).resolve()),role,"--seconds",str(args.seconds),
                "--base-port",str(args.base_port),"--http-port",str(args.http_port)]
            if args.source_replay: command.extend(["--source-replay",str(args.source_replay)])
            children.append(subprocess.Popen(command))
        while not stopped.wait(.1):
            if any(p.poll() is not None for p in children): break
    finally:
        for child in children:
            if child.poll() is None: child.terminate()
        for child in children:
            try: child.wait(8)
            except subprocess.TimeoutExpired:
                child.kill(); child.wait()
    return 0 if all(p.returncode == 0 for p in children) else 1


if __name__ == "__main__":
    raise SystemExit(main())
