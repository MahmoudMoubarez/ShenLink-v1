"""Small standalone release checks; no original-project fixtures or tools."""
import socket
import time
import pytest
from multiflow_comm.v1 import (
    CommandSender, CommandReceiver, CommandSenderSettings, CommandReceiverSettings,
    SensorSender, SensorReceiver, SensorSenderSettings, SensorReceiverSettings,
    MeasurementExpectation, TelemetrySample, SampleStatus, UdpTransport, TcpTransport,
    TransportState,
)


def wait(predicate):
    until = time.monotonic() + 5
    while not predicate():
        assert time.monotonic() < until
        time.sleep(.005)


def test_same_adapter_contract_for_video_commands_sensors():
    import video_stream.v1 as video
    import multiflow_comm.v1 as flows
    for name in ("TransportAdapter", "TransportCapabilities", "UdpTransport", "TcpTransport"):
        assert getattr(video, name) is getattr(flows, name)


@pytest.mark.parametrize("transport", ["udp", "tcp"])
@pytest.mark.parametrize("flow", ["commands", "sensors"])
def test_standalone_native_bytes_endpoints(flow, transport):
    kind = socket.SOCK_DGRAM if transport == "udp" else socket.SOCK_STREAM
    reservations = [socket.socket(socket.AF_INET, kind) for _ in range(2)]
    try:
        for sock in reservations: sock.bind(("127.0.0.1", 0))
        a, b = [sock.getsockname() for sock in reservations]
    finally:
        for sock in reservations: sock.close()
    if transport == "udp":
        tx, rx = UdpTransport("a", a, b), UdpTransport("a", b, a)
    else:
        tx, rx = TcpTransport("a", a, b), TcpTransport("a", b, a, mode="listen")
    received = []
    if flow == "commands":
        sender = CommandSender(CommandSenderSettings([tx]))
        receiver = CommandReceiver(CommandReceiverSettings([rx]), on_command=received.append)
    else:
        sender = SensorSender(SensorSenderSettings([tx]))
        receiver = SensorReceiver(SensorReceiverSettings([rx]), on_observation=received.append)
        receiver.register_measurement("robot", "value", MeasurementExpectation("raw", 1, codec_id="raw"))
    try:
        receiver.start(); sender.start()
        wait(lambda: sender.path_states["a"] == TransportState.CONNECTED)
        for seq in range(1, 6):
            if flow == "commands":
                ticket = sender.send_bytes(str(seq).encode())
                wait(ticket.is_done)
                assert ticket.status.value == "RECEIVED"
            else:
                sender.publish(TelemetrySample("robot", "value", "boot", seq, SampleStatus.GOOD,
                    str(seq).encode(), "raw", "raw", 1))
            wait(lambda: len(received) == seq)
        if flow == "commands":
            assert [r.payload for r in received] == [str(i).encode() for i in range(1, 6)]
        else:
            assert [r.sample.sample_sequence for r in received] == list(range(1, 6))
    finally:
        sender.stop(); receiver.stop()
    assert tx.state == rx.state == TransportState.CLOSED
