"""Validated configuration models; engine lifecycle wiring follows separately."""
from dataclasses import dataclass, field
from .transport import validate_adapter
from .shared.adaptive import AdaptiveConfig
from .shared.delivery import DelayPolicy
from .shared.negotiation import ReceiverRequirements
from .shared.ordered import DeliveryMode
from .shared.fps import AdaptationPriority


def validated_paths(paths):
    paths=tuple(paths)
    if not paths:
        raise ValueError("At least one transport path is required")
    for adapter in paths:validate_adapter(adapter)
    if len({adapter.path_id for adapter in paths})!=len(paths):
        raise ValueError("Transport path IDs must be unique")
    return paths


@dataclass(frozen=True)
class SenderSettings:
    """A capture source and developer-supplied paths. Does not open resources."""
    source: object
    paths: tuple
    min_fps: int = 1
    max_fps: int | None = None

    def __post_init__(self):
        if (not callable(getattr(self.source,"build_elements",None))
                or any(type(getattr(self.source,name,None)) is not int or getattr(self.source,name)<=0
                       for name in ("width","height","fps"))):
            raise TypeError("Source must provide dimensions, FPS and build_elements")
        object.__setattr__(self,"paths",validated_paths(self.paths))
        ceiling=self.source.fps if self.max_fps is None else self.max_fps
        if (type(self.min_fps) is not int or type(ceiling) is not int
                or not 1<=self.min_fps<=ceiling<=self.source.fps):
            raise ValueError('Sender FPS range must fit capture capabilities')
        object.__setattr__(self,'max_fps',ceiling)


@dataclass(frozen=True)
class ReceiverSettings:
    """Receiver-owned requirements; defaults retain the verified video policy.

    Dynamic FPS is opt-in. Preserve-order uses one fixed negotiated profile rather than
    the latest-frame adaptive baseline/enhancement selection policy.
    """
    paths: tuple
    requirements: ReceiverRequirements = field(default_factory=lambda: AdaptiveConfig().requirements)
    quality_steps: int = 3
    delay_policy: DelayPolicy = DelayPolicy.BEST_AVAILABLE
    delivery_mode: DeliveryMode = DeliveryMode.LATEST
    dynamic_fps: bool = False
    fps_steps: int = 3
    adaptation_priority: AdaptationPriority = AdaptationPriority.FRAME_RATE_FIRST

    def __post_init__(self):
        object.__setattr__(self,"paths",validated_paths(self.paths))
        object.__setattr__(self,"delivery_mode",DeliveryMode(self.delivery_mode))
        if self.delivery_mode==DeliveryMode.PRESERVE_ORDER:
            if self.delay_policy!=DelayPolicy.BEST_AVAILABLE:raise ValueError("Ordered delivery cannot discard over-age frames")
            if not all(p.capabilities.reliable and p.capabilities.ordered for p in self.paths):
                raise ValueError("Preserve-order requires reliable, ordered adapters")
        if not isinstance(self.requirements,ReceiverRequirements):
            raise TypeError("Receiver requirements must be ReceiverRequirements")
        config=AdaptiveConfig(requirements=self.requirements,quality_steps=self.quality_steps,
                              delay_policy=self.delay_policy,delivery_mode=self.delivery_mode,
                              dynamic_fps=self.dynamic_fps,fps_steps=self.fps_steps,
                              adaptation_priority=self.adaptation_priority)
        # Reject incompatible timer/grace envelopes before an engine allocates
        # codecs. Engine negotiation still has to check sender source abilities.
        if self.delivery_mode==DeliveryMode.LATEST:config.dual(self.paths[0].path_id)
        object.__setattr__(self,"delay_policy",config.delay_policy)
        object.__setattr__(self,'adaptation_priority',config.adaptation_priority)

    def adaptive_config(self):
        return AdaptiveConfig(requirements=self.requirements,quality_steps=self.quality_steps,
                              delay_policy=self.delay_policy,delivery_mode=self.delivery_mode,
                              dynamic_fps=self.dynamic_fps,fps_steps=self.fps_steps,
                              adaptation_priority=self.adaptation_priority)
