"""Two explicit adaptation priorities; no arbitrary cross-unit weights."""
from dataclasses import replace
from enum import Enum
from .ladder import quality_ladder


class AdaptationPriority(str, Enum):
    FRAME_RATE_FIRST = 'frame_rate_first'
    QUALITY_FIRST = 'quality_first'


def adaptation_ladder(requirements, quality_steps=3, fps_steps=3,
                      priority=AdaptationPriority.FRAME_RATE_FIRST):
    priority=AdaptationPriority(priority)
    if type(fps_steps) is not int or not 1<=fps_steps<=8:
        raise ValueError('Choose between one and eight frame-rate levels')
    if requirements.max_fps>120:raise ValueError('Dynamic FPS supports up to 120 FPS')
    quality=quality_ladder(requirements,quality_steps,fps=requirements.min_fps)
    if requirements.min_fps!=requirements.max_fps and fps_steps<2:
        raise ValueError('A variable FPS range requires at least two steps')
    rates=tuple(dict.fromkeys(round(requirements.min_fps+
        (requirements.max_fps-requirements.min_fps)*i/max(1,fps_steps-1)) for i in range(fps_steps)))
    if priority==AdaptationPriority.FRAME_RATE_FIRST:
        profiles=[replace(quality[0],fps=f) for f in rates]
        profiles += [replace(q,fps=rates[-1]) for q in quality[1:]]
    else:
        profiles=list(quality)
        profiles += [replace(quality[-1],fps=f) for f in rates[1:]]
    return tuple(profiles)
