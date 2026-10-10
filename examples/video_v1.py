#!/usr/bin/env python3
"""Minimal sender/receiver using only the public video_stream.v1 API.

Defaults are trusted loopback UDP and synthetic video; no GUI or sudo needed.
Pass one --path per physical route for separate hosts. This receives decoded
frames but does not display them or establish performance superiority.
"""
import argparse
import json
import math
from pathlib import Path
import sys
import threading
import time

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from video_stream.v1 import (
    CameraSource, DelayPolicy, ReceiverRequirements, ReceiverSettings,
    SenderSettings, UdpTransport, TcpTransport, DeliveryMode, VideoReceiver, VideoSender,
)


def path_spec(value):
    parts=value.split(",")
    if len(parts)!=5 or any(not part.strip() for part in parts):
        raise argparse.ArgumentTypeError("Use ID,LOCAL_IP,LOCAL_PORT,PEER_IP,PEER_PORT")
    try:
        ports=int(parts[2]),int(parts[4])
        if any(not 1<=port<=65535 for port in ports):raise ValueError()
    except ValueError:
        raise argparse.ArgumentTypeError("Ports must be integers from 1 to 65535")
    return parts[0],(parts[1],ports[0]),(parts[3],ports[1])


def parser():
    result=argparse.ArgumentParser(description=__doc__)
    result.add_argument("role",choices=("sender","receiver"))
    result.add_argument("--path",action="append",type=path_spec,
                        help="ID,LOCAL_IP,LOCAL_PORT,PEER_IP,PEER_PORT; repeat for more paths")
    result.add_argument("--seconds",type=float,default=10,help="Runtime after negotiation")
    result.add_argument("--camera",action="store_true",help="Sender: opt into hardware camera instead of test pattern")
    result.add_argument("--fps",type=int,default=30,help="Fixed FPS, not dynamic adaptation")
    result.add_argument("--max-age-ms",type=float,default=150,help="Receiver delay target")
    result.add_argument("--strict",action="store_true",help="Receiver: discard over-age frames")
    result.add_argument("--transport",choices=("udp","tcp"),default="udp")
    result.add_argument("--delivery",choices=("latest","preserve_order"),default="latest",
                        help="Receiver's requested delivery policy; ordered requires TCP")
    return result


def run(args):
    if not math.isfinite(args.seconds) or args.seconds<=0:
        raise ValueError("seconds must be finite and positive")
    if args.fps<=0:raise ValueError("FPS must be positive")
    if args.role=="receiver" and args.camera:raise ValueError("--camera is sender-only")
    if args.role=="sender" and args.strict:raise ValueError("--strict is receiver-only")
    if args.role=="sender" and args.delivery!='latest':raise ValueError('--delivery is receiver-only; sender negotiates it')
    local,peer=(7000,7001) if args.role=="sender" else (7001,7000)
    specs=args.path or [("a",("127.0.0.1",local),("127.0.0.1",peer))]
    paths=[(UdpTransport(path_id,bind,remote) if args.transport=='udp' else
        TcpTransport(path_id,bind,remote,mode='connect' if args.role=='sender' else 'listen'))
        for path_id,bind,remote in specs]
    lock=threading.Lock();failed=threading.Event();errors=[]
    observed=dict(decoded_frames=0,last_frame_id=None,last_dimensions=None,last_session_id=None)
    def error(exc):
        with lock:errors.append(str(exc))
        failed.set()
    def frame(value):
        # Fast callback: no GUI, file I/O or frame queue. Applications may hand
        # the newest frame to their own bounded rendering/processing stage.
        with lock:
            session=getattr(value,'session_id',None)
            if (session==observed['last_session_id'] and observed["last_frame_id"] is not None
                    and value.frame_id<=observed["last_frame_id"]):
                errors.append("Public frame ID did not advance");failed.set();return
            observed.update(decoded_frames=observed["decoded_frames"]+1,
                            last_frame_id=value.frame_id,last_dimensions=[value.width,value.height],
                            last_session_id=session)
    if args.role=="sender":
        endpoint=VideoSender(SenderSettings(
            CameraSource(fps=args.fps,test_pattern=not args.camera),paths),on_error=error)
    else:
        requirements=ReceiverRequirements(min_resolution=(640,360),max_resolution=(1280,720),
            min_fps=args.fps,max_fps=args.fps,min_bitrate_kbps=600,max_bitrate_kbps=1800,
            max_frame_age_ms=args.max_age_ms)
        endpoint=VideoReceiver(ReceiverSettings(paths,requirements=requirements,
            delivery_mode=DeliveryMode(args.delivery),
            delay_policy=DelayPolicy.STRICT if args.strict else DelayPolicy.BEST_AVAILABLE),
            on_frame=frame,on_error=error)
    # start() negotiates with the other process. __exit__ releases codecs and
    # paths on normal completion, exceptions and Ctrl+C.
    with endpoint:
        until=time.monotonic()+args.seconds
        while time.monotonic()<until and not failed.wait(min(.05,max(0,until-time.monotonic()))):pass
        with lock:
            if errors:raise RuntimeError(errors[0])
            summary=dict(observed)
        if args.role=="receiver" and not summary["decoded_frames"]:
            raise RuntimeError("No decoded frames received; check sender and paths")
        if (args.role=='receiver' and args.delivery=='preserve_order' and endpoint.state!='FINISHED'):
            raise RuntimeError('Ordered sender did not finish before receiver deadline; use a longer receiver --seconds')
        codecs=endpoint.counters()["video"]["codecs"]
        finish=None
        if args.role=='sender' and endpoint.config.delivery_mode==DeliveryMode.PRESERVE_ORDER:
            finish=endpoint.finish()
    return dict(status="PASS",role=args.role,scope="Functional example, not a benchmark",
                paths=[p.path_id for p in paths],codecs=codecs,finish=finish,**summary)


if __name__=="__main__":
    args=parser().parse_args()
    try:print(json.dumps(run(args),indent=2))
    except KeyboardInterrupt:sys.exit(130)
    except Exception as exc:
        print(json.dumps(dict(status="FAILED",error=str(exc))),file=sys.stderr)
        sys.exit(1)
