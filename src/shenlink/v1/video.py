"""ShenLink v1 video API; identical classes to the compatible video_stream.v1 API.

Importing this module does not initialize GStreamer or open transport sockets.
"""

from video_stream import v1 as _implementation

__all__ = list(_implementation.__all__)
globals().update({name: getattr(_implementation, name) for name in __all__})
del _implementation
