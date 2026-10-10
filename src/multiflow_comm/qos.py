"""Network Quality of Service (QoS) and DiffServ (DSCP) configuration.

Ref:
- RFC 2474 (Definition of the Differentiated Services Field in the IPv4 and IPv6 Headers)
- RFC 8837 (Differentiated Services Code Point (DSCP) Packet Markings for WebRTC)

Constraints:
- Numeric priority (0..1000) is NOT numerically converted to DSCP.
- Explicit mapping from priority tiers (0..3) to DSCP (0..63) configured by developer/network admin.
- Default is NO DSCP marking.
- Marking attempts report one of: NOT_REQUESTED, MARK_APPLIED_LOCALLY, NETWORK_PRIORITY_UNAVAILABLE.
- Does NOT claim end-to-end priority guarantees ("priority guaranteed" is forbidden).
"""

from __future__ import annotations

from dataclasses import dataclass, field
import socket
from typing import Optional

from .scheduler import get_priority_tier
from .types import NetworkPriorityError, NetworkQoSStatus


@dataclass
class NetworkQoSConfig:
    """Explicit mapping from priority tiers to DSCP codepoints.
    
    Tiers:
      3: Critical (750..1000) -> e.g. DSCP 46 (Expedited Forwarding / EF)
      2: High     (500..749)  -> e.g. DSCP 34 (Assured Forwarding / AF41)
      1: Normal   (250..499)  -> e.g. DSCP 18 (AF21)
      0: Low      (0..249)    -> e.g. DSCP 10 (AF11) or 0 (Best Effort)
    """
    tier_dscp_mapping: dict[int, int] = field(default_factory=dict)
    default_dscp: Optional[int] = None

    def __post_init__(self) -> None:
        for tier, dscp in self.tier_dscp_mapping.items():
            if not (0 <= tier <= 3):
                raise ValueError(f"Invalid tier {tier}. Must be in 0..3")
            if not (0 <= dscp <= 63):
                raise ValueError(f"Invalid DSCP {dscp}. Must be in 0..63 (6 bits)")
        if self.default_dscp is not None:
            if not (0 <= self.default_dscp <= 63):
                raise ValueError(f"Invalid default DSCP {self.default_dscp}. Must be in 0..63")

    def get_dscp(self, priority: int) -> Optional[int]:
        """Lookup DSCP for a numeric priority (0..1000)."""
        tier = get_priority_tier(priority)
        return self.tier_dscp_mapping.get(tier, self.default_dscp)


def dscp_to_tos_byte(dscp: int) -> int:
    """Convert 6-bit DSCP (0..63) to IP TOS byte (DSCP in top 6 bits, ECN 0)."""
    if not (0 <= dscp <= 63):
        raise ValueError(f"DSCP must be 0..63, got {dscp}")
    return (dscp & 0x3F) << 2


def apply_socket_dscp(sock: socket.socket, dscp: int) -> bool:
    """Attempt to apply DSCP / IP_TOS marking to a socket outer IP packet.
    
    Returns True if successfully set locally by the OS, False if OS/policy rejected it.
    Does NOT raise exceptions.
    """
    tos = dscp_to_tos_byte(dscp)
    try:
        # IPv4
        sock.setsockopt(socket.IPPROTO_IP, socket.IP_TOS, tos)
        return True
    except (OSError, socket.error):
        # On some platforms (e.g. Windows without QoS Group Policy), setsockopt IP_TOS may fail
        pass

    try:
        # IPv6 fallback if IPv6 socket
        if hasattr(socket, "IPV6_TCLASS"):
            sock.setsockopt(socket.IPPROTO_IPV6, socket.IPV6_TCLASS, tos)
            return True
    except (OSError, socket.error):
        pass

    return False
