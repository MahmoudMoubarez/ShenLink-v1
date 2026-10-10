"""Wire format serialization and framing for multiflow_comm protocol version 1.

Format:
  [4 bytes: unsigned big-endian body_length]
  [2 bytes: unsigned big-endian header_length]
  [header_length bytes: JSON UTF-8 header]
  [payload_length bytes: binary payload]

Where body_length = 2 + header_length + payload_length.
"""

from __future__ import annotations

import json
import struct
from typing import Any, Optional

from .types import FrameKind, ProtocolError, SerializationError

PROTOCOL_VERSION = 1
MAX_PAYLOAD_SIZE = 64 * 1024  # 64 KiB
PREFIX_FORMAT = ">IH"
PREFIX_SIZE = struct.calcsize(PREFIX_FORMAT)  # 6 bytes
ALLOWED_KINDS = {k.value for k in FrameKind}
REQUIRED_HEADER_KEYS = {
    "version",
    "kind",
    "session_id",
    "channel_id",
    "message_id",
    "sequence",
    "latest_key",
    "mode",
    "priority",
    "receipt",
    "payload_length",
}


def encode_frame(
    kind: str,
    session_id: str,
    channel_id: str,
    message_id: str,
    sequence: int,
    mode: str,
    priority: int,
    receipt: str,
    payload: bytes = b"",
    latest_key: Optional[str] = None,
    version: int = PROTOCOL_VERSION,
) -> bytes:
    """Encode a protocol frame into binary wire format."""
    if kind not in ALLOWED_KINDS:
        raise SerializationError(f"Invalid frame kind '{kind}'. Allowed: {ALLOWED_KINDS}")

    if not isinstance(payload, (bytes, bytearray, memoryview)):
        raise SerializationError("Payload must be bytes-like")

    payload_bytes = bytes(payload)
    if len(payload_bytes) > MAX_PAYLOAD_SIZE:
        raise SerializationError(
            f"Payload size {len(payload_bytes)} exceeds maximum library limit of {MAX_PAYLOAD_SIZE} bytes"
        )

    header_dict = {
        "version": version,
        "kind": str(kind),
        "session_id": str(session_id),
        "channel_id": str(channel_id),
        "message_id": str(message_id),
        "sequence": int(sequence),
        "latest_key": latest_key,
        "mode": str(mode),
        "priority": int(priority),
        "receipt": str(receipt),
        "payload_length": len(payload_bytes),
    }

    try:
        header_json = json.dumps(header_dict, separators=(",", ":"), ensure_ascii=False)
        header_bytes = header_json.encode("utf-8")
    except Exception as e:
        raise SerializationError(f"Failed to serialize header to JSON: {e}") from e

    header_length = len(header_bytes)
    if header_length > 65535:
        raise SerializationError(f"Header length {header_length} exceeds 16-bit unsigned maximum")

    body_length = 2 + header_length + len(payload_bytes)
    if body_length > 0xFFFFFFFF:
        raise SerializationError("Body length exceeds 32-bit unsigned maximum")

    prefix = struct.pack(PREFIX_FORMAT, body_length, header_length)
    return prefix + header_bytes + payload_bytes


def decode_frame(frame_data: bytes) -> tuple[dict[str, Any], bytes]:
    """Decode a single complete binary frame.
    
    Returns:
        (header_dict, payload_bytes)
    
    Raises:
        ProtocolError on format, header, or length violations.
    """
    if len(frame_data) < PREFIX_SIZE:
        raise ProtocolError(f"Frame length {len(frame_data)} shorter than prefix size {PREFIX_SIZE}")

    body_length, header_length = struct.unpack(PREFIX_FORMAT, frame_data[:PREFIX_SIZE])

    total_expected = 4 + body_length
    if len(frame_data) != total_expected:
        raise ProtocolError(
            f"Frame length mismatch: expected total {total_expected} bytes from body_length {body_length}, got {len(frame_data)}"
        )

    if body_length < 2 + header_length:
        raise ProtocolError(
            f"Invalid body_length {body_length}: smaller than 2 + header_length {header_length}"
        )

    payload_length = body_length - 2 - header_length
    if payload_length > MAX_PAYLOAD_SIZE:
        raise ProtocolError(
            f"Payload length {payload_length} exceeds maximum library limit of {MAX_PAYLOAD_SIZE} bytes"
        )

    header_start = PREFIX_SIZE
    header_end = header_start + header_length
    header_bytes = frame_data[header_start:header_end]

    try:
        header_dict = json.loads(header_bytes.decode("utf-8"))
    except Exception as e:
        raise ProtocolError(f"Malformed JSON in header: {e}") from e

    if not isinstance(header_dict, dict):
        raise ProtocolError("Header must be a JSON object")

    missing = REQUIRED_HEADER_KEYS - set(header_dict.keys())
    if missing:
        raise ProtocolError(f"Header missing required keys: {sorted(missing)}")

    version = header_dict.get("version")
    if version != PROTOCOL_VERSION:
        raise ProtocolError(f"Unsupported protocol version: {version}. Expected {PROTOCOL_VERSION}")

    kind = header_dict.get("kind")
    if kind not in ALLOWED_KINDS:
        raise ProtocolError(f"Invalid frame kind '{kind}'")

    if header_dict.get("payload_length") != payload_length:
        raise ProtocolError(
            f"Header payload_length {header_dict.get('payload_length')} does not match calculated length {payload_length}"
        )

    payload = frame_data[header_end:]
    if len(payload) != payload_length:
        raise ProtocolError(f"Extracted payload length {len(payload)} does not match {payload_length}")

    return header_dict, payload


class StreamFramer:
    """Helper for reading framed multiflow_comm frames from continuous byte streams (e.g. TCP)."""

    def __init__(self, max_buffer_size: int = 256 * 1024) -> None:
        self._buffer = bytearray()
        self._max_buffer_size = max_buffer_size

    def feed(self, chunk: bytes) -> list[tuple[dict[str, Any], bytes]]:
        """Append incoming chunk and parse any complete frames."""
        if not chunk:
            return []

        if len(self._buffer) + len(chunk) > self._max_buffer_size:
            raise ProtocolError(f"Stream framer buffer exceeded limit {self._max_buffer_size} bytes")

        self._buffer.extend(chunk)
        frames: list[tuple[dict[str, Any], bytes]] = []

        while len(self._buffer) >= 4:
            body_length = struct.unpack(">I", self._buffer[:4])[0]
            if body_length > 2 + 65535 + MAX_PAYLOAD_SIZE:
                # Malformed body length - prevent infinite buffer growth
                raise ProtocolError(f"Frame body_length {body_length} exceeds sanity limit")

            total_frame_len = 4 + body_length
            if len(self._buffer) < total_frame_len:
                # Incomplete frame, wait for more data
                break

            frame_bytes = bytes(self._buffer[:total_frame_len])
            del self._buffer[:total_frame_len]

            header, payload = decode_frame(frame_bytes)
            frames.append((header, payload))

        return frames

    def reset(self) -> None:
        """Clear current buffer."""
        self._buffer.clear()
