"""Sensor v1 facade; existing telemetry engine, shared opaque byte adapters.

No new delivery algorithm, serialization, queue, or scheduler. Latest readings
are coalesced independently per (source_id, measurement_id); receipts are NONE.
"""
from dataclasses import dataclass, field
from types import MappingProxyType

from .byte_transport import TransportState, validate_adapter
from .command_endpoints import _Endpoint, _positive
from .telemetry import (
    TelemetryPublisher, TelemetrySubscriber, FreshnessPolicy,
    MeasurementExpectation, UnknownAgePolicy,
)
from .types import PathPolicy


def _validate(settings):
    if not isinstance(settings.channel_id, str) or not settings.channel_id.strip():
        raise ValueError("Nonempty channel_id required")
    if type(settings.max_queue_size) is not int or settings.max_queue_size <= 0:
        raise ValueError("Positive integer max_queue_size required")
    paths = tuple(settings.paths)
    if not paths:
        raise ValueError("At least one transport path required")
    for path in paths:
        validate_adapter(path)
        if path.state != TransportState.DISCONNECTED:
            raise ValueError("Sensor paths must be fresh and exclusively owned")
    if len({p.path_id for p in paths}) != len(paths):
        raise ValueError("Duplicate path IDs")
    object.__setattr__(settings, "paths", paths)


@dataclass(frozen=True)
class SensorSenderSettings:
    paths: tuple
    channel_id: str = "sensors"
    max_queue_size: int = 256
    probe_interval_s: float = 0.2

    def __post_init__(self):
        _validate(self)
        _positive(self.probe_interval_s, "probe_interval_s")


@dataclass(frozen=True)
class SensorReceiverSettings:
    paths: tuple
    channel_id: str = "sensors"
    max_queue_size: int = 256
    freshness_policy: FreshnessPolicy = field(default_factory=lambda: FreshnessPolicy(
        max_silence_ms=1000, max_source_age_ms=None,
        unknown_age_policy=UnknownAgePolicy.ACCEPT_WITH_WARNING))
    max_retired_epochs_per_measurement: int = 32
    reconnect_interval_s: float = 2.0
    expectations: object = field(default_factory=dict)

    def __post_init__(self):
        _validate(self)
        _positive(self.reconnect_interval_s, "reconnect_interval_s")
        if not isinstance(self.freshness_policy, FreshnessPolicy):
            raise TypeError("FreshnessPolicy required")
        if type(self.max_retired_epochs_per_measurement) is not int or self.max_retired_epochs_per_measurement <= 0:
            raise ValueError("Positive retired epoch bound required")
        expectations = dict(self.expectations)
        for key, value in expectations.items():
            if not isinstance(key, tuple) or len(key) != 2 or any(
                not isinstance(part, str) or not part.strip() for part in key
            ) or not isinstance(value, MeasurementExpectation):
                raise ValueError("Expectations require (source_id, measurement_id): MeasurementExpectation")
        object.__setattr__(self, "expectations", MappingProxyType(expectations))


class _SensorEndpoint(_Endpoint):
    @property
    def session_id(self):
        return self._core._sender.session_id if isinstance(self._core, TelemetryPublisher) else self._core.session_id

    def start(self):
        if self._closed:
            raise RuntimeError("Sensor endpoint is closed; create a fresh endpoint")
        return super().start()


class SensorSender(_SensorEndpoint):
    """Redundant latest-only readings. Tickets track dispatch, not remote receipt."""
    def __init__(self, settings: SensorSenderSettings, *, on_error=None, on_state=None):
        if not isinstance(settings, SensorSenderSettings):
            raise TypeError("SensorSenderSettings required")
        self._initialize(settings, sender=True, on_error=on_error, on_state=on_state)
        self._core = TelemetryPublisher(settings.channel_id, self._paths,
            path_policy=PathPolicy.REDUNDANT, max_queue_size=settings.max_queue_size,
            probe_interval_s=settings.probe_interval_s, on_error=on_error)

    def publish(self, sample):
        if not self._started or self._closed:
            raise RuntimeError("Start the sensor sender before publishing")
        return self._core.publish(sample)


class SensorReceiver(_SensorEndpoint):
    def __init__(self, settings: SensorReceiverSettings, *, on_observation,
                 age_estimator=None, on_error=None, on_state=None, on_session=None):
        if not isinstance(settings, SensorReceiverSettings):
            raise TypeError("SensorReceiverSettings required")
        for callback in (on_observation,):
            if not callable(callback):
                raise TypeError("on_observation must be callable")
        for callback in (age_estimator, on_session):
            if callback is not None and not callable(callback):
                raise TypeError("Callbacks must be callable or None")
        self._initialize(settings, sender=False, on_error=on_error, on_state=on_state)
        self._core = TelemetrySubscriber(settings.channel_id, self._paths,
            freshness_policy=settings.freshness_policy,
            max_retired_epochs_per_measurement=settings.max_retired_epochs_per_measurement,
            expectations=dict(settings.expectations), age_estimator=age_estimator,
            max_queue_size=settings.max_queue_size, on_observation=on_observation,
            on_error=on_error, reconnect_interval_s=settings.reconnect_interval_s,
            on_session_reset=on_session)

    def register_measurement(self, source_id, measurement_id, expectation):
        if self._closed:
            raise RuntimeError("Sensor receiver is closed")
        self._core.register_measurement(source_id, measurement_id, expectation)

    def get_state(self, source_id, measurement_id):
        return self._core.get_state(source_id, measurement_id)

    def get_metrics(self):
        return self._core.get_metrics()
