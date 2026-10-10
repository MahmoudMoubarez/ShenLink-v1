"""Explicit session lifecycle and sequence ordering guard.

Protects against out-of-order, delayed, or stale commands across session restarts
and multi-path cross-signaling. Quality commands can NEVER modify the active session.
"""

import threading


class SequenceGuard:
    """Thread-safe validator for session scoping and monotonic sequence ordering."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.session_id: str | None = None
        self.last_sequence: dict[str, int] = {}

    def start_session(self, session_id: str) -> None:
        """Explicitly initialize or switch to a new active session.
        
        Clears sequence history for all paths. Only explicit session creation
        can activate a session.
        """
        if not session_id:
            raise ValueError("session_id must be a non-empty string")
        with self._lock:
            self.session_id = session_id
            self.last_sequence.clear()

    def end_session(self, session_id: str) -> None:
        """Close an active session if matching."""
        with self._lock:
            if self.session_id == session_id:
                self.session_id = None
                self.last_sequence.clear()

    def validate(self, session_id: str, target_path: str, sequence: int) -> bool:
        """Validate command against active session and sequence ordering without advancing sequence."""
        with self._lock:
            if not self.session_id or session_id != self.session_id:
                return False
            last = self.last_sequence.get(target_path, -1)
            return sequence > last

    def commit(self, session_id: str, target_path: str, sequence: int) -> bool:
        """Validate and advance command sequence for the target path."""
        with self._lock:
            if not self.session_id or session_id != self.session_id:
                return False
            last = self.last_sequence.get(target_path, -1)
            if sequence <= last:
                return False
            self.last_sequence[target_path] = sequence
            return True

    def accept(self, session_id: str, target_path: str, sequence: int) -> bool:
        """Single-step validate and commit for backward compatibility."""
        return self.commit(session_id, target_path, sequence)

