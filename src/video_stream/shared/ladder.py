"""Generate bounded H264/I420 profiles; explicit capability lists also supported."""
from dataclasses import replace
import math
from .session import BaselineProfile
from .negotiation import profile_from_wire
from dataclasses import asdict


def quality_ladder(requirements,steps=3,*,fps=None,supported_profiles=None):
    if type(steps) is not int or not 1<=steps<=8:raise ValueError('Choose between one and eight quality levels')
    fps=requirements.max_fps if fps is None else fps
    if type(fps) is not int or not requirements.min_fps<=fps<=requirements.max_fps:
        raise ValueError('FPS outside developer bounds')
    if requirements.codec!='H264':raise ValueError('This encoder supports H264 only')
    if supported_profiles is not None:
        profiles=[replace(profile_from_wire(asdict(p)),max_frame_age_ms=requirements.max_frame_age_ms)
                  for p in supported_profiles]
        valid=sorted(set(p for p in profiles if p.fps==fps and requirements.accepts(p)),
                     key=lambda p:(p.width*p.height,p.bitrate_kbps))
        if not valid:raise ValueError('No supported profile inside developer bounds')
        if len(valid)>steps:
            valid=[valid[round(i*(len(valid)-1)/(steps-1))] for i in range(steps)] if steps>1 else valid[:1]
        return tuple(valid)
    max_w,max_h=(v//2*2 for v in requirements.max_resolution)
    if not max_w or not max_h:raise ValueError('No even H264 dimensions inside bounds')
    ratio=max_w/max_h
    low_h=math.ceil(max(requirements.min_resolution[1],requirements.min_resolution[0]/ratio)/2)*2
    low_w=max(math.ceil(low_h*ratio/2)*2,math.ceil(requirements.min_resolution[0]/2)*2)
    if low_h>max_h or low_w>max_w:raise ValueError('No aspect-preserving profile inside developer bounds; supply explicit profiles')
    result=[]
    for i in range(steps):
        fraction=i/(steps-1) if steps>1 else 0
        h=round((low_h+(max_h-low_h)*fraction)/2)*2
        w=max(low_w,min(max_w,round(h*ratio/2)*2))
        bitrate=round(requirements.min_bitrate_kbps+
                      (requirements.max_bitrate_kbps-requirements.min_bitrate_kbps)*fraction)
        p=BaselineProfile(w,h,fps,bitrate,requirements.max_frame_age_ms)
        if not requirements.accepts(p):raise ValueError('Generated profile outside bounds')
        if p not in result:result.append(p)
    return tuple(result)
