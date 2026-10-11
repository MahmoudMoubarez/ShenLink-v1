"""Branding must not split class identity or change the existing transport API."""
import json
import os
import re
from pathlib import Path
import subprocess
import sys

import pytest


@pytest.mark.parametrize("legacy,canonical", [
    ("multiflow_comm.v1", "shenlink.v1"),
    ("video_stream.v1", "shenlink.v1.video"),
])
def test_every_public_export_is_the_original_object(legacy, canonical):
    import importlib
    old, new = importlib.import_module(legacy), importlib.import_module(canonical)
    assert new.__all__ == old.__all__
    for name in old.__all__:
        assert getattr(new, name) is getattr(old, name), name


def test_shared_adapter_identity_across_new_apis():
    import shenlink.v1 as flows
    import shenlink.v1.video as video
    for name in ("TransportAdapter", "TransportCallbacks", "TransportCapabilities",
                 "TransportState", "SendStatus", "UdpTransport", "TcpTransport"):
        assert getattr(flows, name) is getattr(video, name)


def test_distribution_name_version_and_package_discovery():
    import shenlink
    settings = (Path(__file__).resolve().parents[1]/"pyproject.toml").read_text()
    assert re.search(r'^name = "ShenLink"$', settings, re.MULTILINE)
    assert re.search(r'^version = "'+re.escape(shenlink.__version__)+r'"$', settings, re.MULTILINE)
    assert '"shenlink*"' in settings


def test_imports_do_not_open_sockets_start_threads_or_load_gstreamer():
    code = '''
import json, socket, threading, sys
def forbidden(*args, **kwargs):
    raise AssertionError("Import started a runtime")
socket.socket = forbidden
threading.Thread.start = forbidden
import shenlink
import shenlink.v1
import shenlink.v1.video
assert "gi" not in sys.modules
print(json.dumps({"version": shenlink.__version__, "pass": True}))
'''
    env = dict(os.environ, PYTHONPATH=str(Path(__file__).resolve().parents[1]/"src"))
    result = subprocess.run([sys.executable, "-c", code], env=env,
                            capture_output=True, text=True, timeout=15)
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout)["pass"]
