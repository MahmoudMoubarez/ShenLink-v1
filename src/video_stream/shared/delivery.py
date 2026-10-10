"""Delay policy is independent of the developer's mandatory media bounds."""
from enum import Enum
import math


class DelayPolicy(str,Enum):
    BEST_AVAILABLE='best_available'
    STRICT='strict'


def accepts_age(age,budget,policy):
    policy=DelayPolicy(policy)
    return (type(age) in (int,float) and math.isfinite(age) and age>=0
            and (policy==DelayPolicy.BEST_AVAILABLE or age<=budget))
