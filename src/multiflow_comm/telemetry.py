"""Typed telemetry sample envelope and codec for multiflow_comm.

Implements Task 2 of the telemetry roadmap (docs/telemetry-contract.md).
Provides TelemetrySample, SampleStatus, envelope encoding/decoding with
versioned JSON metadata and strict base64 payloads, and schema validation.
"""

from __future__ import annotations

import base64
from dataclasses import dataclass
from enum import Enum
import json
import math
import threading
import time
from typing import Any, Callable, Optional, Union

from .messages import MessageReceiver, MessageSender, ReceivedMessage
from .qos import NetworkQoSConfig
from .scheduler import Scheduler
from .transport import MessageReceiverAdapter, MessageSenderAdapter
from .types import (
    ChannelBlockedEvent,
    CodecError,
    DeliveryMode,
    DeliveryTicket,
    PathPolicy,
    ReceiptPolicy,
    SessionResetEvent,
)
from .wire import MAX_PAYLOAD_SIZE

TELEMETRY_ENVELOPE_VERSION = 1

REQUIRED_ENVELOPE_KEYS = frozenset({
    "telemetry_version",
    "source_id",
    "measurement_id",
    "source_epoch",
    "sample_sequence",
    "status",
    "codec_id",
    "schema_id",
    "schema_version",
    "unit",
    "source_timestamp_ns",
    "source_clock_id",
    "value_bytes",
})


class SampleStatus(str, Enum):
    """Quality and validity status of a telemetry sample at acquisition.
    
    Ref: docs/telemetry-contract.md § Sample meaning.
    """
    GOOD = "GOOD"
    UNCERTAIN = "UNCERTAIN"
    BAD = "BAD"


class TelemetryCodecError(CodecError):
    """Raised when telemetry sample encoding, decoding, or envelope validation fails."""


class TelemetrySchemaMismatchError(TelemetryCodecError):
    """Raised when a decoded telemetry sample fails consumer schema or unit expectations."""


def _validate_non_empty_str(val: Any, field_name: str) -> None:
    if not isinstance(val, str) or val.strip() == "":
        raise TelemetryCodecError(f"{field_name} must be a non-empty string, got {val!r}")


def _validate_positive_int(val: Any, field_name: str) -> None:
    if isinstance(val, bool) or not isinstance(val, int) or val <= 0:
        raise TelemetryCodecError(f"{field_name} must be a positive integer, got {val!r}")


def _validate_positive_finite_float(val: Any, field_name: str) -> None:
    if isinstance(val, bool) or not isinstance(val, (int, float)):
        raise ValueError(f"{field_name} must be a real number, got {val!r}")
    if math.isnan(val) or math.isinf(val):
        raise ValueError(f"{field_name} must be a finite number, got {val!r}")
    if val <= 0:
        raise ValueError(f"{field_name} must be positive (> 0), got {val!r}")


@dataclass(frozen=True)
class TelemetrySample:
    """Immutable telemetry sample representing a measurement value at the source.

    Attributes:
        source_id: Application identifier of the sensor or originating host.
        measurement_id: Measurement identifier scoped to source_id (e.g. 'battery_voltage').
        source_epoch: Identifier distinguishing restarts of the source subsystem.
        sample_sequence: Positive integer incremented at the source per (source_id, measurement_id, source_epoch).
        status: Sample quality state (GOOD, UNCERTAIN, BAD).
        value_bytes: Raw serialized bytes of the sample value. Never automatically unpickled.
        codec_id: Codec identifier for value_bytes (e.g. 'json', 'raw', 'cbor').
        schema_id: Application data schema identifier.
        schema_version: Positive integer version of schema_id.
        unit: Physical unit (e.g. 'V', 'degC', 'm/s') or None.
        source_timestamp_ns: Monotonic or wall nanosecond timestamp at sensor read, or None.
        source_clock_id: Identifier of the source clock domain if source_timestamp_ns is set, else None.
    """
    source_id: str
    measurement_id: str
    source_epoch: str
    sample_sequence: int
    status: SampleStatus
    value_bytes: bytes
    codec_id: str
    schema_id: str
    schema_version: int
    unit: Optional[str] = None
    source_timestamp_ns: Optional[int] = None
    source_clock_id: Optional[str] = None

    def __post_init__(self) -> None:
        _validate_non_empty_str(self.source_id, "source_id")
        _validate_non_empty_str(self.measurement_id, "measurement_id")
        _validate_non_empty_str(self.source_epoch, "source_epoch")
        _validate_positive_int(self.sample_sequence, "sample_sequence")

        if not isinstance(self.status, SampleStatus):
            if isinstance(self.status, str):
                try:
                    object.__setattr__(self, "status", SampleStatus(self.status))
                except ValueError:
                    raise TelemetryCodecError(f"Invalid status: {self.status!r}")
            else:
                raise TelemetryCodecError(f"status must be a SampleStatus, got {type(self.status).__name__}")

        if not isinstance(self.value_bytes, (bytes, bytearray, memoryview)):
            raise TelemetryCodecError(f"value_bytes must be bytes-like, got {type(self.value_bytes).__name__}")
        if not isinstance(self.value_bytes, bytes):
            object.__setattr__(self, "value_bytes", bytes(self.value_bytes))

        _validate_non_empty_str(self.codec_id, "codec_id")
        _validate_non_empty_str(self.schema_id, "schema_id")
        _validate_positive_int(self.schema_version, "schema_version")

        if self.unit is not None:
            _validate_non_empty_str(self.unit, "unit")

        if self.source_timestamp_ns is None and self.source_clock_id is None:
            pass
        elif self.source_timestamp_ns is not None and self.source_clock_id is not None:
            if isinstance(self.source_timestamp_ns, bool) or not isinstance(self.source_timestamp_ns, int) or self.source_timestamp_ns < 0:
                raise TelemetryCodecError(f"source_timestamp_ns must be a non-negative integer, got {self.source_timestamp_ns!r}")
            _validate_non_empty_str(self.source_clock_id, "source_clock_id")
        else:
            raise TelemetryCodecError(
                "source_timestamp_ns and source_clock_id must either both be present or both be None"
            )


def encode_telemetry_sample(sample: TelemetrySample) -> bytes:
    """Encode a TelemetrySample into a versioned UTF-8 JSON envelope with base64 payload.

    Args:
        sample: The TelemetrySample instance to serialize.

    Returns:
        UTF-8 encoded binary envelope ready for transmission as a message payload.

    Raises:
        TelemetryCodecError: If sample is invalid or encoded size exceeds MAX_PAYLOAD_SIZE.
    """
    if not isinstance(sample, TelemetrySample):
        raise TelemetryCodecError(f"Expected TelemetrySample, got {type(sample).__name__}")

    try:
        b64_payload = base64.b64encode(sample.value_bytes).decode("ascii")
    except Exception as e:
        raise TelemetryCodecError(f"Failed to base64 encode value_bytes: {e}") from e

    envelope_dict = {
        "telemetry_version": TELEMETRY_ENVELOPE_VERSION,
        "source_id": sample.source_id,
        "measurement_id": sample.measurement_id,
        "source_epoch": sample.source_epoch,
        "sample_sequence": sample.sample_sequence,
        "status": sample.status.value,
        "codec_id": sample.codec_id,
        "schema_id": sample.schema_id,
        "schema_version": sample.schema_version,
        "unit": sample.unit,
        "source_timestamp_ns": sample.source_timestamp_ns,
        "source_clock_id": sample.source_clock_id,
        "value_bytes": b64_payload,
    }

    try:
        json_text = json.dumps(
            envelope_dict,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        )
        encoded_bytes = json_text.encode("utf-8")
    except Exception as e:
        raise TelemetryCodecError(f"Failed to serialize telemetry envelope to JSON: {e}") from e

    if len(encoded_bytes) > MAX_PAYLOAD_SIZE:
        raise TelemetryCodecError(
            f"Encoded telemetry sample ({len(encoded_bytes)} bytes) exceeds MAX_PAYLOAD_SIZE ({MAX_PAYLOAD_SIZE} bytes)"
        )

    return encoded_bytes


def _object_pairs_hook_reject_duplicates(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    """JSON object hook that rejects duplicate keys."""
    d: dict[str, Any] = {}
    for k, v in pairs:
        if k in d:
            raise TelemetryCodecError(f"Duplicate key in telemetry envelope: '{k}'")
        d[k] = v
    return d


def _parse_constant_reject(constant_str: str) -> Any:
    """Reject non-finite JSON constants (NaN, Infinity, -Infinity)."""
    raise TelemetryCodecError(f"Non-finite JSON constant not allowed in telemetry envelope: '{constant_str}'")


def decode_telemetry_sample(data: bytes) -> TelemetrySample:
    """Decode binary envelope bytes into a validated TelemetrySample.

    Args:
        data: The binary payload received over the message channel.

    Returns:
        The decoded and validated TelemetrySample instance.

    Raises:
        TelemetryCodecError: If data is malformed, has invalid fields, wrong types,
                             duplicate keys, or an unsupported envelope version.
    """
    if not isinstance(data, (bytes, bytearray, memoryview)):
        raise TelemetryCodecError(f"Expected bytes-like data, got {type(data).__name__}")

    raw_bytes = bytes(data)
    if len(raw_bytes) > MAX_PAYLOAD_SIZE:
        raise TelemetryCodecError(
            f"Telemetry payload ({len(raw_bytes)} bytes) exceeds MAX_PAYLOAD_SIZE ({MAX_PAYLOAD_SIZE} bytes)"
        )

    try:
        text = raw_bytes.decode("utf-8")
    except UnicodeDecodeError as e:
        raise TelemetryCodecError(f"Invalid UTF-8 in telemetry envelope: {e}") from e

    try:
        parsed = json.loads(
            text,
            object_pairs_hook=_object_pairs_hook_reject_duplicates,
            parse_constant=_parse_constant_reject,
        )
    except TelemetryCodecError:
        raise
    except (json.JSONDecodeError, RecursionError, ValueError) as e:
        raise TelemetryCodecError(f"Malformed JSON in telemetry envelope: {e}") from e

    if not isinstance(parsed, dict):
        raise TelemetryCodecError(f"Telemetry envelope must be a JSON object, got {type(parsed).__name__}")

    keys = set(parsed.keys())
    if keys != REQUIRED_ENVELOPE_KEYS:
        missing = REQUIRED_ENVELOPE_KEYS - keys
        extra = keys - REQUIRED_ENVELOPE_KEYS
        details = []
        if missing:
            details.append(f"missing required fields: {sorted(missing)}")
        if extra:
            details.append(f"unrecognized extra fields: {sorted(extra)}")
        raise TelemetryCodecError(f"Invalid telemetry envelope fields: {', '.join(details)}")

    # Validate envelope version
    ver = parsed["telemetry_version"]
    if isinstance(ver, bool) or not isinstance(ver, int):
        raise TelemetryCodecError(f"telemetry_version must be an integer, got {ver!r}")
    if ver != TELEMETRY_ENVELOPE_VERSION:
        raise TelemetryCodecError(f"Unsupported telemetry envelope version: {ver}")

    # Validate fields
    source_id = parsed["source_id"]
    _validate_non_empty_str(source_id, "source_id")

    measurement_id = parsed["measurement_id"]
    _validate_non_empty_str(measurement_id, "measurement_id")

    source_epoch = parsed["source_epoch"]
    _validate_non_empty_str(source_epoch, "source_epoch")

    seq = parsed["sample_sequence"]
    _validate_positive_int(seq, "sample_sequence")

    status_str = parsed["status"]
    if not isinstance(status_str, str):
        raise TelemetryCodecError(f"status must be a string, got {status_str!r}")
    try:
        status = SampleStatus(status_str)
    except ValueError:
        raise TelemetryCodecError(f"Invalid status: {status_str!r}")

    codec_id = parsed["codec_id"]
    _validate_non_empty_str(codec_id, "codec_id")

    schema_id = parsed["schema_id"]
    _validate_non_empty_str(schema_id, "schema_id")

    s_ver = parsed["schema_version"]
    _validate_positive_int(s_ver, "schema_version")

    unit = parsed["unit"]
    if unit is not None:
        _validate_non_empty_str(unit, "unit")

    ts = parsed["source_timestamp_ns"]
    clock = parsed["source_clock_id"]
    if ts is None and clock is None:
        pass
    elif ts is not None and clock is not None:
        if isinstance(ts, bool) or not isinstance(ts, int) or ts < 0:
            raise TelemetryCodecError(f"source_timestamp_ns must be a non-negative integer, got {ts!r}")
        _validate_non_empty_str(clock, "source_clock_id")
    else:
        raise TelemetryCodecError(
            "source_timestamp_ns and source_clock_id must either both be present or both be None"
        )

    b64_str = parsed["value_bytes"]
    if not isinstance(b64_str, str):
        raise TelemetryCodecError(f"value_bytes must be a base64 string, got {type(b64_str).__name__}")
    try:
        value_bytes = base64.b64decode(b64_str.encode("ascii"), validate=True)
    except Exception as e:
        raise TelemetryCodecError(f"Malformed base64 in value_bytes: {e}") from e

    return TelemetrySample(
        source_id=source_id,
        measurement_id=measurement_id,
        source_epoch=source_epoch,
        sample_sequence=seq,
        status=status,
        value_bytes=value_bytes,
        codec_id=codec_id,
        schema_id=schema_id,
        schema_version=s_ver,
        unit=unit,
        source_timestamp_ns=ts,
        source_clock_id=clock,
    )


def validate_telemetry_sample(
    sample: TelemetrySample,
    expected_schema_id: str,
    expected_schema_version: int,
    expected_unit: Optional[str] = None,
) -> None:
    """Validate a decoded sample against consumer schema and unit expectations.

    Args:
        sample: Decoded TelemetrySample.
        expected_schema_id: Required schema identifier.
        expected_schema_version: Required schema version integer.
        expected_unit: Expected physical unit string, or None if unmeasured/unitless.

    Raises:
        TelemetrySchemaMismatchError: If schema_id, schema_version, or unit do not match.
        TelemetryCodecError: If sample is not a TelemetrySample instance or expectation
            arguments are invalid.
    """
    if not isinstance(sample, TelemetrySample):
        raise TelemetryCodecError(f"Expected TelemetrySample, got {type(sample).__name__}")

    _validate_non_empty_str(expected_schema_id, "expected_schema_id")
    _validate_positive_int(expected_schema_version, "expected_schema_version")
    if expected_unit is not None:
        _validate_non_empty_str(expected_unit, "expected_unit")

    if sample.schema_id != expected_schema_id:
        raise TelemetrySchemaMismatchError(
            f"Telemetry schema_id mismatch: expected '{expected_schema_id}', got '{sample.schema_id}'"
        )

    if sample.schema_version != expected_schema_version:
        raise TelemetrySchemaMismatchError(
            f"Telemetry schema_version mismatch for '{sample.schema_id}': expected {expected_schema_version}, got {sample.schema_version}"
        )

    if sample.unit != expected_unit:
        raise TelemetrySchemaMismatchError(
            f"Telemetry unit mismatch for '{sample.schema_id}': expected '{expected_unit}', got '{sample.unit}'"
        )


def encode_latest_key(source_id: str, measurement_id: str) -> str:
    """Encode source_id and measurement_id unambiguously as a JSON array string."""
    _validate_non_empty_str(source_id, "source_id")
    _validate_non_empty_str(measurement_id, "measurement_id")
    return json.dumps([source_id, measurement_id], separators=(",", ":"))


class UnknownAgePolicy(str, Enum):
    """Policy for telemetry samples whose cross-device age cannot be reliably estimated."""
    ACCEPT_WITH_WARNING = "ACCEPT_WITH_WARNING"
    REJECT = "REJECT"


class SourceAgeStatus(str, Enum):
    """Classification of source-to-receiver sample age measurement status."""
    KNOWN = "KNOWN"
    UNKNOWN = "UNKNOWN"


@dataclass(frozen=True)
class MeasurementExpectation:
    """Registered expectations for an incoming telemetry measurement."""
    schema_id: str
    schema_version: int
    unit: Optional[str] = None
    codec_id: str = "json"

    def __post_init__(self) -> None:
        _validate_non_empty_str(self.schema_id, "schema_id")
        _validate_positive_int(self.schema_version, "schema_version")
        if self.unit is not None:
            _validate_non_empty_str(self.unit, "unit")
        _validate_non_empty_str(self.codec_id, "codec_id")


@dataclass(frozen=True)
class FreshnessPolicy:
    """Liveness and freshness bounds for a telemetry subscriber.

    Numeric defaults are explicitly omitted per docs/telemetry-contract.md.
    """
    max_silence_ms: float
    max_source_age_ms: Optional[float]
    unknown_age_policy: UnknownAgePolicy

    def __post_init__(self) -> None:
        _validate_positive_finite_float(self.max_silence_ms, "max_silence_ms")
        if self.max_source_age_ms is not None:
            _validate_positive_finite_float(self.max_source_age_ms, "max_source_age_ms")
        if not isinstance(self.unknown_age_policy, UnknownAgePolicy):
            try:
                object.__setattr__(self, "unknown_age_policy", UnknownAgePolicy(self.unknown_age_policy))
            except ValueError:
                raise ValueError(f"Invalid unknown_age_policy: {self.unknown_age_policy!r}")


@dataclass(frozen=True)
class TelemetryObservation:
    """Delivered telemetry observation with arrival, queue delay, and liveness metadata."""
    sample: TelemetrySample
    path_id: str
    arrival_monotonic_ns: int
    arrival_wall_time: float
    delivery_monotonic_ns: int
    delivery_wall_time: float
    source_age_status: SourceAgeStatus
    source_age_ms: Optional[float]
    is_live: bool
    not_live_reason: Optional[str] = None
    warning: Optional[str] = None
    transport_session_id: Optional[str] = None


@dataclass(frozen=True)
class TelemetryState:
    """Read-only measurement state snapshot with dynamically reevaluated liveness."""
    source_id: str
    measurement_id: str
    last_observation: Optional[TelemetryObservation]
    is_live: bool
    not_live_reason: Optional[str]
    silence_duration_ms: Optional[float]


@dataclass(frozen=True)
class TelemetrySubscriberMetrics:
    """Counters and metrics tracking subscriber message, rejection, and liveness events."""
    accepted_count: int = 0
    rejected_count: int = 0
    stale_count: int = 0
    unknown_age_count: int = 0
    observed_sequence_gap_count: int = 0
    malformed_count: int = 0
    schema_mismatch_count: int = 0
    codec_mismatch_count: int = 0
    unregistered_count: int = 0
    exhausted_epoch_count: int = 0


class TelemetryPublisher:
    """Publishing facade over MessageSender with LATEST_ONLY keyed semantics."""

    def __init__(
        self,
        channel_id: str,
        adapters: list[MessageSenderAdapter],
        priority: int = 700,
        path_policy: PathPolicy = PathPolicy.PRIMARY_THEN_BACKUP,
        max_queue_size: int = 256,
        scheduler: Optional[Scheduler] = None,
        require_network_priority: bool = False,
        qos_config: Optional[NetworkQoSConfig] = None,
        on_error: Optional[Callable[[Exception], None]] = None,
        probe_interval_s: float = 1.0,
    ) -> None:
        self.channel_id = channel_id
        self._sender = MessageSender(
            channel_id=channel_id,
            adapters=adapters,
            mode=DeliveryMode.LATEST_ONLY,
            receipt=ReceiptPolicy.NONE,
            priority=priority,
            path_policy=path_policy,
            max_queue_size=max_queue_size,
            scheduler=scheduler,
            require_network_priority=require_network_priority,
            qos_config=qos_config,
            on_error=on_error,
            probe_interval_s=probe_interval_s,
        )

    @property
    def is_blocked(self) -> bool:
        return self._sender.is_blocked

    @property
    def adapters(self) -> list[MessageSenderAdapter]:
        return self._sender.adapters

    def start(self) -> None:
        self._sender.start()

    def close(self) -> None:
        self._sender.close()

    def publish(self, sample: TelemetrySample) -> DeliveryTicket:
        """Publish a TelemetrySample over the underlying message sender.

        Encodes the sample envelope, derives an unambiguous latest_key from
        (source_id, measurement_id), and dispatches via send_bytes.
        Returns the DeliveryTicket tracking dispatch.
        """
        if not isinstance(sample, TelemetrySample):
            raise TelemetryCodecError(f"Expected TelemetrySample, got {type(sample).__name__}")

        payload = encode_telemetry_sample(sample)
        latest_key = encode_latest_key(sample.source_id, sample.measurement_id)
        return self._sender.send_bytes(payload, latest_key=latest_key)

    def __enter__(self) -> TelemetryPublisher:
        self.start()
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.close()


class _MeasurementTracker:
    def __init__(self) -> None:
        self.current_epoch: Optional[str] = None
        self.last_sequence: int = 0
        self.retired_epochs: set[str] = set()
        self.is_exhausted: bool = False
        self.exhausted_reason: Optional[str] = None
        self.last_observation: Optional[TelemetryObservation] = None
        self.last_arrival_mono_ns: Optional[int] = None
        self.arrival_source_age_bound_ms: Optional[float] = None


class TelemetrySubscriber:
    """Subscriber facade over MessageReceiver with LATEST_ONLY keyed freshness."""

    def __init__(
        self,
        channel_id: str,
        adapters: list[MessageReceiverAdapter],
        freshness_policy: FreshnessPolicy,
        max_retired_epochs_per_measurement: int,
        expectations: Optional[dict[tuple[str, str], MeasurementExpectation]] = None,
        age_estimator: Optional[Callable[[TelemetrySample, int], Optional[float]]] = None,
        max_queue_size: int = 256,
        on_observation: Optional[Callable[[TelemetryObservation], None]] = None,
        on_error: Optional[Callable[[Exception], None]] = None,
        time_source: Optional[Callable[[], int]] = None,
        reconnect_interval_s: float = 2.0,
        on_session_reset: Optional[Callable[[SessionResetEvent], None]] = None,
    ) -> None:
        if not isinstance(freshness_policy, FreshnessPolicy):
            raise ValueError(f"freshness_policy must be a FreshnessPolicy instance, got {freshness_policy!r}")
        _validate_positive_int(
            max_retired_epochs_per_measurement, "max_retired_epochs_per_measurement"
        )

        self.channel_id = channel_id
        self.freshness_policy = freshness_policy
        self.max_retired_epochs_per_measurement = max_retired_epochs_per_measurement
        self.age_estimator = age_estimator
        self.on_observation = on_observation
        self.on_error = on_error
        self._time_source = time_source or time.monotonic_ns

        self._expectations: dict[tuple[str, str], MeasurementExpectation] = {}
        if expectations:
            for key, exp in expectations.items():
                if not isinstance(key, tuple) or len(key) != 2:
                    raise ValueError(f"Expectations key must be (source_id, measurement_id) tuple, got {key!r}")
                if not isinstance(exp, MeasurementExpectation):
                    raise ValueError(f"Expected MeasurementExpectation, got {exp!r}")
                self._expectations[key] = exp

        self._lock = threading.Lock()
        self._trackers: dict[tuple[str, str], _MeasurementTracker] = {}

        # Metrics counters
        self._accepted_count = 0
        self._rejected_count = 0
        self._stale_count = 0
        self._unknown_age_count = 0
        self._observed_sequence_gap_count = 0
        self._malformed_count = 0
        self._schema_mismatch_count = 0
        self._codec_mismatch_count = 0
        self._unregistered_count = 0
        self._exhausted_epoch_count = 0

        self._receiver = MessageReceiver(
            channel_id=channel_id,
            adapters=adapters,
            mode=DeliveryMode.LATEST_ONLY,
            max_queue_size=max_queue_size,
            on_message=self._on_message,
            on_error=self._on_receiver_error,
            on_session_reset=on_session_reset,
            time_source=self._time_source,
            reconnect_interval_s=reconnect_interval_s,
        )

    @property
    def adapters(self) -> list[MessageReceiverAdapter]:
        return self._receiver.adapters

    @property
    def session_id(self) -> Optional[str]:
        return self._receiver.session_id

    def register_measurement(
        self,
        source_id: str,
        measurement_id: str,
        expectation: MeasurementExpectation,
    ) -> None:
        """Register expected schema, version, unit, and codec for a measurement."""
        _validate_non_empty_str(source_id, "source_id")
        _validate_non_empty_str(measurement_id, "measurement_id")
        if not isinstance(expectation, MeasurementExpectation):
            raise ValueError(f"Expected MeasurementExpectation, got {expectation!r}")
        with self._lock:
            self._expectations[(source_id, measurement_id)] = expectation

    def start(self) -> None:
        self._receiver.start()

    def close(self) -> None:
        self._receiver.close()

    def __enter__(self) -> TelemetrySubscriber:
        self.start()
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.close()

    def _call_age_estimator(
        self, sample: TelemetrySample, arrival_mono_ns: int
    ) -> tuple[Optional[float], Optional[Exception]]:
        """Call age_estimator(sample, arrival_monotonic_ns) outside internal lock.

        Returns:
            (bound_ms, None) on success, or (None, Exception) on error or invalid bound.
        """
        if self.age_estimator is None:
            return None, None
        try:
            raw = self.age_estimator(sample, arrival_mono_ns)
        except Exception as exc:
            return None, exc

        if raw is None:
            return None, None

        if isinstance(raw, bool) or not isinstance(raw, (int, float)):
            err = TelemetryCodecError(f"Age estimator returned non-number bound: {raw!r}")
            return None, err

        if math.isnan(raw) or math.isinf(raw):
            err = TelemetryCodecError(f"Age estimator returned non-finite bound: {raw!r}")
            return None, err

        if raw < 0:
            err = TelemetryCodecError(f"Age estimator returned negative bound: {raw!r}")
            return None, err

        return float(raw), None

    def _notify_error(self, exc: Exception) -> None:
        """Invoke on_error handler outside internal locks."""
        if self.on_error:
            try:
                self.on_error(exc)
            except Exception:
                pass

    def _on_receiver_error(self, exc: Exception) -> None:
        self._notify_error(exc)

    def _on_message(self, msg: ReceivedMessage) -> None:
        # Step 1: Injected clock & local timing captured outside lock
        delivery_mono_ns = self._time_source()
        delivery_wall = time.time()
        arrival_mono_ns = msg.received_monotonic_ns
        arrival_wall = msg.received_at
        queue_delay_ms = max(0.0, (delivery_mono_ns - arrival_mono_ns) / 1_000_000.0)

        # Step 2: Decode envelope outside lock
        try:
            sample = decode_telemetry_sample(msg.payload)
        except Exception as exc:
            with self._lock:
                self._malformed_count += 1
                self._rejected_count += 1
            self._notify_error(exc)
            return

        key = (sample.source_id, sample.measurement_id)

        # Step 3: Check registration
        with self._lock:
            exp = self._expectations.get(key)
        if exp is None:
            err = TelemetryCodecError(f"Unregistered telemetry measurement: {key}")
            with self._lock:
                self._unregistered_count += 1
                self._rejected_count += 1
            self._notify_error(err)
            return

        # Step 4: Validate schema/unit outside lock
        try:
            validate_telemetry_sample(sample, exp.schema_id, exp.schema_version, exp.unit)
        except Exception as exc:
            with self._lock:
                self._schema_mismatch_count += 1
                self._rejected_count += 1
            self._notify_error(exc)
            return

        if sample.codec_id != exp.codec_id:
            err = TelemetryCodecError(
                f"Telemetry codec_id mismatch for {key}: expected '{exp.codec_id}', got '{sample.codec_id}'"
            )
            with self._lock:
                self._codec_mismatch_count += 1
                self._rejected_count += 1
            self._notify_error(err)
            return

        # Step 5: Invoke age estimator outside lock
        arrival_age_bound_ms: Optional[float] = None
        estimator_error: Optional[Exception] = None
        if self.age_estimator is not None and sample.source_timestamp_ns is not None:
            arrival_age_bound_ms, estimator_error = self._call_age_estimator(
                sample, arrival_mono_ns
            )

        # Step 6: Determine observation attributes & freshness outside lock
        if estimator_error is not None:
            source_age_status = SourceAgeStatus.UNKNOWN
            source_age_ms = None
            is_live = False
            not_live_reason = f"Age estimator error: {estimator_error}"
            warning = None
        elif arrival_age_bound_ms is None:
            source_age_status = SourceAgeStatus.UNKNOWN
            source_age_ms = None
            if self.freshness_policy.unknown_age_policy == UnknownAgePolicy.REJECT:
                is_live = False
                not_live_reason = "Source age is UNKNOWN (rejected by policy)"
                warning = None
            else:
                warning = "Source age is UNKNOWN (accepted with warning by policy)"
                if sample.status == SampleStatus.BAD:
                    is_live = False
                    not_live_reason = "Sample status is BAD"
                elif queue_delay_ms > self.freshness_policy.max_silence_ms:
                    is_live = False
                    not_live_reason = (
                        f"Silence duration ({queue_delay_ms:.1f}ms) exceeds "
                        f"max_silence_ms ({self.freshness_policy.max_silence_ms:.1f}ms)"
                    )
                else:
                    is_live = True
                    not_live_reason = None
        else:
            source_age_status = SourceAgeStatus.KNOWN
            warning = None
            total_source_age_ms = arrival_age_bound_ms + queue_delay_ms
            source_age_ms = total_source_age_ms
            if sample.status == SampleStatus.BAD:
                is_live = False
                not_live_reason = "Sample status is BAD"
            elif (
                self.freshness_policy.max_source_age_ms is not None
                and total_source_age_ms > self.freshness_policy.max_source_age_ms
            ):
                is_live = False
                not_live_reason = (
                    f"Source age bound ({total_source_age_ms:.1f}ms) exceeds "
                    f"max_source_age_ms ({self.freshness_policy.max_source_age_ms:.1f}ms)"
                )
            elif queue_delay_ms > self.freshness_policy.max_silence_ms:
                is_live = False
                not_live_reason = (
                    f"Silence duration ({queue_delay_ms:.1f}ms) exceeds "
                    f"max_silence_ms ({self.freshness_policy.max_silence_ms:.1f}ms)"
                )
            else:
                is_live = True
                not_live_reason = None

        obs = TelemetryObservation(
            sample=sample,
            path_id=msg.path_id,
            arrival_monotonic_ns=arrival_mono_ns,
            arrival_wall_time=arrival_wall,
            delivery_monotonic_ns=delivery_mono_ns,
            delivery_wall_time=delivery_wall,
            transport_session_id=msg.session_id,
            source_age_status=source_age_status,
            source_age_ms=source_age_ms,
            is_live=is_live,
            not_live_reason=not_live_reason,
            warning=warning,
        )

        # Step 7: Atomic epoch/sequence/tracker updates inside lock ONLY
        exhaustion_error_to_fire: Optional[Exception] = None
        should_deliver = False

        with self._lock:
            tracker = self._trackers.get(key)
            if tracker is None:
                tracker = _MeasurementTracker()
                self._trackers[key] = tracker

            # If measurement is already exhausted, fail closed: reject sample
            if tracker.is_exhausted:
                self._rejected_count += 1
                return

            # Delayed sample from an already retired epoch
            if sample.source_epoch in tracker.retired_epochs:
                self._stale_count += 1
                self._rejected_count += 1
                return

            if tracker.current_epoch is None:
                tracker.current_epoch = sample.source_epoch
                tracker.last_sequence = sample.sample_sequence
            elif sample.source_epoch != tracker.current_epoch:
                # Epoch restart/change: check retired epochs bound
                if len(tracker.retired_epochs) >= self.max_retired_epochs_per_measurement:
                    tracker.is_exhausted = True
                    tracker.exhausted_reason = (
                        f"Retired epoch limit ({self.max_retired_epochs_per_measurement}) "
                        f"exceeded for {key}; failing closed"
                    )
                    self._exhausted_epoch_count += 1
                    self._rejected_count += 1
                    exhaustion_error_to_fire = TelemetryCodecError(tracker.exhausted_reason)
                else:
                    tracker.retired_epochs.add(tracker.current_epoch)
                    tracker.current_epoch = sample.source_epoch
                    tracker.last_sequence = sample.sample_sequence
            else:
                # Same epoch
                if sample.sample_sequence <= tracker.last_sequence:
                    self._stale_count += 1
                    self._rejected_count += 1
                    return
                elif sample.sample_sequence > tracker.last_sequence + 1:
                    self._observed_sequence_gap_count += 1
                tracker.last_sequence = sample.sample_sequence

            if not tracker.is_exhausted:
                tracker.last_observation = obs
                tracker.last_arrival_mono_ns = arrival_mono_ns
                tracker.arrival_source_age_bound_ms = arrival_age_bound_ms
                self._accepted_count += 1
                if source_age_status == SourceAgeStatus.UNKNOWN:
                    self._unknown_age_count += 1
                should_deliver = True

        # Step 8: Fire user callbacks OUTSIDE lock
        if exhaustion_error_to_fire is not None:
            self._notify_error(exhaustion_error_to_fire)
            return

        if estimator_error is not None:
            self._notify_error(estimator_error)

        if should_deliver and self.on_observation:
            try:
                self.on_observation(obs)
            except Exception as exc:
                self._notify_error(exc)

    def get_state(self, source_id: str, measurement_id: str) -> TelemetryState:
        """Query read-only measurement state with dynamically reevaluated liveness."""
        key = (source_id, measurement_id)
        with self._lock:
            tracker = self._trackers.get(key)
            if tracker is None or tracker.last_observation is None or tracker.last_arrival_mono_ns is None:
                return TelemetryState(
                    source_id=source_id,
                    measurement_id=measurement_id,
                    last_observation=None,
                    is_live=False,
                    not_live_reason="No samples received yet",
                    silence_duration_ms=None,
                )

            obs = tracker.last_observation
            last_arrival_mono_ns = tracker.last_arrival_mono_ns
            arrival_age_bound = tracker.arrival_source_age_bound_ms
            is_exhausted = tracker.is_exhausted
            exhausted_reason = tracker.exhausted_reason

        now_mono_ns = self._time_source()
        silence_ms = max(0.0, (now_mono_ns - last_arrival_mono_ns) / 1_000_000.0)

        # If exhausted, fail closed: NOT LIVE
        if is_exhausted:
            return TelemetryState(
                source_id=source_id,
                measurement_id=measurement_id,
                last_observation=obs,
                is_live=False,
                not_live_reason=exhausted_reason,
                silence_duration_ms=silence_ms,
            )

        if obs.sample.status == SampleStatus.BAD:
            is_live = False
            reason = "Sample status is BAD"
        elif not obs.is_live:
            is_live = False
            reason = obs.not_live_reason
        elif silence_ms > self.freshness_policy.max_silence_ms:
            is_live = False
            reason = (
                f"Silence duration ({silence_ms:.1f}ms) exceeds "
                f"max_silence_ms ({self.freshness_policy.max_silence_ms:.1f}ms)"
            )
        elif obs.source_age_status == SourceAgeStatus.KNOWN and arrival_age_bound is not None:
            current_source_age_ms = arrival_age_bound + silence_ms
            if (
                self.freshness_policy.max_source_age_ms is not None
                and current_source_age_ms > self.freshness_policy.max_source_age_ms
            ):
                is_live = False
                reason = (
                    f"Current source age ({current_source_age_ms:.1f}ms) exceeds "
                    f"max_source_age_ms ({self.freshness_policy.max_source_age_ms:.1f}ms)"
                )
            else:
                is_live = obs.is_live
                reason = obs.not_live_reason
        else:
            if self.freshness_policy.unknown_age_policy == UnknownAgePolicy.REJECT:
                is_live = False
                reason = "Source age is UNKNOWN (rejected by policy)"
            else:
                is_live = obs.is_live
                reason = obs.not_live_reason

        return TelemetryState(
            source_id=source_id,
            measurement_id=measurement_id,
            last_observation=obs,
            is_live=is_live,
            not_live_reason=reason,
            silence_duration_ms=silence_ms,
        )

    def get_metrics(self) -> TelemetrySubscriberMetrics:
        """Return a snapshot of subscriber counters and metrics."""
        with self._lock:
            return TelemetrySubscriberMetrics(
                accepted_count=self._accepted_count,
                rejected_count=self._rejected_count,
                stale_count=self._stale_count,
                unknown_age_count=self._unknown_age_count,
                observed_sequence_gap_count=self._observed_sequence_gap_count,
                malformed_count=self._malformed_count,
                schema_mismatch_count=self._schema_mismatch_count,
                codec_mismatch_count=self._codec_mismatch_count,
                unregistered_count=self._unregistered_count,
                exhausted_epoch_count=self._exhausted_epoch_count,
            )


@dataclass(frozen=True)
class EventObservation:
    """Delivered event observation with arrival, channel sequence, and session metadata.
    
    Attributes:
        sample: Validated TelemetrySample payload.
        channel_sequence: Transport sequence number within the session (1..N).
        path_id: Transport path ID over which the frame was received.
        arrival_monotonic_ns: Monotonic timestamp in nanoseconds when the frame was accepted.
        arrival_wall_time: Wall clock time estimated for frame arrival.
        delivery_monotonic_ns: Monotonic timestamp in nanoseconds when dispatched to callback.
        delivery_wall_time: Wall clock time when dispatched to callback.
        session_id: Channel session ID distinguishing session epochs.
    """
    sample: TelemetrySample
    channel_sequence: int
    path_id: str
    arrival_monotonic_ns: int
    arrival_wall_time: float
    delivery_monotonic_ns: int
    delivery_wall_time: float
    session_id: str


@dataclass(frozen=True)
class EventSubscriberMetrics:
    """Snapshot of event subscriber metrics."""
    delivered_count: int = 0
    malformed_count: int = 0
    schema_mismatch_count: int = 0
    unregistered_count: int = 0


# Type alias for symmetry and clarity
TelemetryEvent = TelemetrySample


class EventPublisher:
    """Publisher facade for RELIABLE_EVENT profile over MessageSender in ORDERED_ALL mode."""

    def __init__(
        self,
        channel_id: str,
        adapters: list[MessageSenderAdapter],
        priority: int = 800,
        path_policy: PathPolicy = PathPolicy.REDUNDANT,
        max_queue_size: int = 256,
        ack_timeout: float = 0.5,
        max_ordered_retries: Optional[int] = 5,
        probe_interval_s: float = 1.0,
        scheduler: Optional[Scheduler] = None,
        require_network_priority: bool = False,
        qos_config: Optional[NetworkQoSConfig] = None,
        on_channel_blocked: Optional[Callable[[ChannelBlockedEvent], None]] = None,
        on_error: Optional[Callable[[Exception], None]] = None,
    ) -> None:
        _validate_non_empty_str(channel_id, "channel_id")
        if not adapters:
            raise ValueError("At least one adapter must be provided")
        _validate_positive_int(max_queue_size, "max_queue_size")
        _validate_positive_finite_float(ack_timeout, "ack_timeout")
        _validate_positive_finite_float(probe_interval_s, "probe_interval_s")
        if isinstance(priority, bool) or not isinstance(priority, int) or not (0 <= priority <= 1000):
            raise ValueError("priority must be an integer between 0 and 1000")
        if not isinstance(path_policy, PathPolicy):
            try:
                path_policy = PathPolicy(path_policy)
            except ValueError:
                raise ValueError(f"Invalid path_policy: {path_policy!r}")
        if max_ordered_retries is not None:
            if isinstance(max_ordered_retries, bool) or not isinstance(max_ordered_retries, int) or max_ordered_retries < 0:
                raise ValueError("max_ordered_retries must be a non-negative int or None")

        self.channel_id = channel_id
        self._sender = MessageSender(
            channel_id=channel_id,
            adapters=adapters,
            mode=DeliveryMode.ORDERED_ALL,
            receipt=ReceiptPolicy.RECEIVED,
            priority=priority,
            path_policy=path_policy,
            require_network_priority=require_network_priority,
            qos_config=qos_config,
            max_queue_size=max_queue_size,
            ack_timeout=ack_timeout,
            max_ordered_retries=max_ordered_retries,
            probe_interval_s=probe_interval_s,
            scheduler=scheduler,
            on_channel_blocked=on_channel_blocked,
            on_error=on_error,
        )

    @property
    def is_blocked(self) -> bool:
        return self._sender.is_blocked

    @property
    def session_id(self) -> str:
        return self._sender.session_id

    @property
    def adapters(self) -> list[MessageSenderAdapter]:
        return self._sender.adapters

    def start(self) -> None:
        self._sender.start()

    def close(self) -> None:
        self._sender.close()

    def publish(self, event: TelemetrySample) -> DeliveryTicket:
        """Publish a TelemetrySample (TelemetryEvent) over ORDERED_ALL channel.

        Returns a DeliveryTicket tracking delivery receipt.
        Raises TelemetryCodecError if event is not a valid TelemetrySample.
        Raises QueueFullError if sender queue is full.
        Raises ChannelBlockedError if channel is blocked.
        """
        if not isinstance(event, TelemetrySample):
            raise TelemetryCodecError(f"Expected TelemetrySample, got {type(event).__name__}")
        payload = encode_telemetry_sample(event)
        return self._sender.send_bytes(payload)

    def restart_session(self) -> None:
        self._sender.restart_session()

    def __enter__(self) -> EventPublisher:
        self.start()
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.close()


class EventSubscriber:
    """Subscriber facade for RELIABLE_EVENT profile over MessageReceiver in ORDERED_ALL mode."""

    def __init__(
        self,
        channel_id: str,
        adapters: list[MessageReceiverAdapter],
        max_queue_size: int = 256,
        expectations: Optional[dict[tuple[str, str], MeasurementExpectation]] = None,
        on_event: Optional[Callable[[EventObservation], None]] = None,
        on_error: Optional[Callable[[Exception], None]] = None,
        on_session_reset: Optional[Callable[[SessionResetEvent], None]] = None,
        reconnect_interval_s: float = 2.0,
        time_source: Optional[Callable[[], int]] = None,
    ) -> None:
        _validate_non_empty_str(channel_id, "channel_id")
        if not adapters:
            raise ValueError("At least one adapter must be provided")
        _validate_positive_int(max_queue_size, "max_queue_size")
        _validate_positive_finite_float(reconnect_interval_s, "reconnect_interval_s")

        self.channel_id = channel_id
        self.on_event = on_event
        self.on_error = on_error
        self.on_session_reset = on_session_reset
        self._time_source = time_source or time.monotonic_ns
        self._lock = threading.Lock()

        self._expectations: dict[tuple[str, str], MeasurementExpectation] = {}
        if expectations:
            for key, exp in expectations.items():
                if not isinstance(key, tuple) or len(key) != 2:
                    raise ValueError(f"Expectations key must be (source_id, measurement_id) tuple, got {key!r}")
                if not isinstance(exp, MeasurementExpectation):
                    raise ValueError(f"Expected MeasurementExpectation, got {exp!r}")
                self._expectations[key] = exp

        # Metrics
        self._delivered_count = 0
        self._malformed_count = 0
        self._schema_mismatch_count = 0
        self._unregistered_count = 0

        self._receiver = MessageReceiver(
            channel_id=channel_id,
            adapters=adapters,
            mode=DeliveryMode.ORDERED_ALL,
            max_queue_size=max_queue_size,
            reconnect_interval_s=reconnect_interval_s,
            on_message=self._on_message,
            on_session_reset=self._on_receiver_session_reset,
            on_error=self._on_receiver_error,
            time_source=self._time_source,
        )

    @property
    def adapters(self) -> list[MessageReceiverAdapter]:
        return self._receiver.adapters

    @property
    def session_id(self) -> Optional[str]:
        return self._receiver.session_id

    def start(self) -> None:
        self._receiver.start()

    def close(self) -> None:
        self._receiver.close()

    def register_event(
        self,
        source_id: str,
        measurement_id: str,
        expectation: MeasurementExpectation,
    ) -> None:
        _validate_non_empty_str(source_id, "source_id")
        _validate_non_empty_str(measurement_id, "measurement_id")
        if not isinstance(expectation, MeasurementExpectation):
            raise ValueError(f"Expected MeasurementExpectation, got {expectation!r}")
        with self._lock:
            self._expectations[(source_id, measurement_id)] = expectation

    def get_metrics(self) -> EventSubscriberMetrics:
        with self._lock:
            return EventSubscriberMetrics(
                delivered_count=self._delivered_count,
                malformed_count=self._malformed_count,
                schema_mismatch_count=self._schema_mismatch_count,
                unregistered_count=self._unregistered_count,
            )

    def _notify_error(self, exc: Exception) -> None:
        if self.on_error:
            try:
                self.on_error(exc)
            except Exception:
                pass

    def _on_receiver_session_reset(self, event: SessionResetEvent) -> None:
        if self.on_session_reset:
            try:
                self.on_session_reset(event)
            except Exception as exc:
                self._notify_error(exc)

    def _on_receiver_error(self, exc: Exception) -> None:
        self._notify_error(exc)

    def _on_message(self, message: ReceivedMessage) -> None:
        delivery_mono_ns = self._time_source()
        delivery_wall = time.time()
        arrival_mono_ns = message.received_monotonic_ns
        elapsed_s = max(0.0, (delivery_mono_ns - arrival_mono_ns) / 1_000_000_000.0)
        arrival_wall = delivery_wall - elapsed_s

        # 1. Decode sample envelope
        try:
            sample = decode_telemetry_sample(message.payload)
        except Exception as exc:
            with self._lock:
                self._malformed_count += 1
            self._notify_error(exc)
            return

        # 2. Check expectations if registered
        key = (sample.source_id, sample.measurement_id)
        with self._lock:
            exp = self._expectations.get(key)
            has_expectations = bool(self._expectations)

        if exp is not None:
            try:
                validate_telemetry_sample(sample, exp.schema_id, exp.schema_version, exp.unit)
            except Exception as exc:
                with self._lock:
                    self._schema_mismatch_count += 1
                self._notify_error(exc)
                return

            if sample.codec_id != exp.codec_id:
                err = TelemetryCodecError(
                    f"Telemetry codec_id mismatch for {key}: expected '{exp.codec_id}', got '{sample.codec_id}'"
                )
                with self._lock:
                    self._schema_mismatch_count += 1
                self._notify_error(err)
                return
        elif has_expectations:
            with self._lock:
                self._unregistered_count += 1
            self._notify_error(
                TelemetrySchemaMismatchError(f"Event {key} not registered in expectations")
            )
            return

        observation = EventObservation(
            sample=sample,
            channel_sequence=message.sequence,
            path_id=message.path_id,
            arrival_monotonic_ns=arrival_mono_ns,
            arrival_wall_time=arrival_wall,
            delivery_monotonic_ns=delivery_mono_ns,
            delivery_wall_time=delivery_wall,
            session_id=message.session_id,
        )

        with self._lock:
            self._delivered_count += 1

        if self.on_event:
            try:
                self.on_event(observation)
            except Exception as exc:
                self._notify_error(exc)

    def __enter__(self) -> EventSubscriber:
        self.start()
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.close()


TelemetryEventPublisher = EventPublisher
TelemetryEventSubscriber = EventSubscriber
