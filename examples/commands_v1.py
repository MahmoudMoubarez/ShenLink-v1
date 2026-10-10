#!/usr/bin/env python3
"""Small command sender/receiver; trusted UDP/TCP, no GUI or sudo.

Start receiver first, then sender. RECEIVED is acceptance, not execution proof.
The example sends text only; use send_object/custom Codec for structured data.
"""
import argparse
from pathlib import Path
import sys
import threading

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from multiflow_comm.v1 import (
    CommandSender, CommandReceiver, CommandSenderSettings, CommandReceiverSettings,
    UdpTransport, TcpTransport, TicketStatus,
)


def path_spec(value):
    parts = value.split(",")
    try:
        if len(parts) != 5 or any(not p.strip() for p in parts):
            raise ValueError()
        local_port, peer_port = int(parts[2]), int(parts[4])
        if not 1 <= local_port <= 65535 or not 1 <= peer_port <= 65535:
            raise ValueError()
        return parts[0], (parts[1], local_port), (parts[3], peer_port)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("Use ID,LOCAL_IP,LOCAL_PORT,PEER_IP,PEER_PORT") from exc


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("role", choices=("sender", "receiver"))
    parser.add_argument("--transport", choices=("udp", "tcp"), default="udp")
    parser.add_argument("--path", action="append", type=path_spec)
    parser.add_argument("--count", type=int, default=10)
    parser.add_argument("--channel", default="commands")
    args = parser.parse_args(argv)
    if args.count <= 0:
        parser.error("--count must be positive")
    specs = args.path or [("a", ("127.0.0.1", 9201 if args.role == "sender" else 9202),
                            ("127.0.0.1", 9202 if args.role == "sender" else 9201))]
    paths = [UdpTransport(pid, local, peer) if args.transport == "udp" else
             TcpTransport(pid, local, peer, mode="connect" if args.role == "sender" else "listen")
             for pid, local, peer in specs]
    if args.role == "sender":
        with CommandSender(CommandSenderSettings(paths, channel_id=args.channel)) as sender:
            for i in range(args.count):
                ticket = sender.send_text(f"command-{i+1}")
                if ticket.wait(10) != TicketStatus.RECEIVED:
                    raise RuntimeError(f"Command {i+1}: {ticket.status.value}; outcome may be unknown")
                print(f"accepted {i+1}", flush=True)
    else:
        done = threading.Event()
        received = []
        def command(message):
            received.append(message.sequence)
            print(f"{message.sequence}: {message.text()}", flush=True)
            if len(received) >= args.count:
                done.set()
        with CommandReceiver(CommandReceiverSettings(paths, channel_id=args.channel),
                             on_command=command):
            if not done.wait(30):
                raise RuntimeError("Receiver observation timed out; no completion claim")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
