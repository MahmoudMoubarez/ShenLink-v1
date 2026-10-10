"""Local non-preemptive execution priority scheduler with starvation prevention.

Splits 0..1000 into 4 priority tiers:
  Tier 3: 750..1000 (Critical)
  Tier 2: 500..749  (High, default for messages = 700)
  Tier 1: 250..499  (Normal, default for video = 300)
  Tier 0: 0..249    (Low)

Rules:
- Non-preemptive: currently running work cannot be interrupted.
- Starvation prevention: after 8 consecutive tasks executed from the highest
  available tier, 1 task from a lower waiting tier is executed, then cycle resets.
- Within tier: highest priority value wins; FIFO on ties.
- Mandatory constraint: ORDERED_ALL channels strictly preserve channel sequence
  order regardless of individual message priorities.
- Bounded queue: default 256 tasks per channel. QueueFullError for ORDERED_ALL,
  key coalescing for LATEST_ONLY.
- Callbacks executed outside internal locks.
- Clean shutdown without thread leaks.
"""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
import threading
import time
from typing import Any, Callable, Optional

from .types import (
    DeliveryMode,
    DeliveryTicket,
    FlowProcessingCapability,
    QueueFullError,
)

MAX_TOP_TIER_STREAK = 8
DEFAULT_MAX_QUEUE_PER_CHANNEL = 256


def get_priority_tier(priority: int) -> int:
    """Map integer priority (0..1000) to one of four tiers (0..3)."""
    p = max(0, min(1000, int(priority)))
    if p >= 750:
        return 3
    if p >= 500:
        return 2
    if p >= 250:
        return 1
    return 0


@dataclass
class ScheduledTask:
    """A unit of work queued in the scheduler."""
    task_id: str
    channel_id: str
    priority: int
    mode: DeliveryMode
    sequence: int
    insertion_index: int
    fn: Callable[[], Any]
    latest_key: Optional[str] = None
    ticket: Optional[DeliveryTicket] = None
    created_at: float = field(default_factory=time.time)


@dataclass
class FlowInfo:
    """Metadata for a registered communication flow."""
    flow_id: str
    priority: int
    capability: FlowProcessingCapability
    metadata: dict[str, Any] = field(default_factory=dict)


class Scheduler:
    """Multi-channel priority scheduler with starvation prevention."""

    def __init__(
        self,
        resource_group: str = "default",
        max_queue_per_channel: int = DEFAULT_MAX_QUEUE_PER_CHANNEL,
    ) -> None:
        self.resource_group = resource_group
        self.max_queue_per_channel = max_queue_per_channel

        self._lock = threading.RLock()
        self._cv = threading.Condition(self._lock)
        self._insertion_counter = 0

        # Channel queues: channel_id -> list of ScheduledTask
        self._channel_tasks: dict[str, list[ScheduledTask]] = {}
        self._channel_modes: dict[str, DeliveryMode] = {}
        self._channel_in_flight: set[str] = set()

        # Registered flows metadata
        self._registered_flows: dict[str, FlowInfo] = {}

        # Starvation prevention state
        self._top_tier_streak = 0

        # Worker thread
        self._running = False
        self._worker_thread: Optional[threading.Thread] = None
        self._executed_tasks_count = 0
        self._last_executed_tiers: list[int] = []

    def set_channel_in_flight(self, channel_id: str, in_flight: bool) -> None:
        """Mark channel as having an in-flight dispatch. Subsequent tasks on this channel wait."""
        with self._lock:
            if in_flight:
                self._channel_in_flight.add(channel_id)
            else:
                self._channel_in_flight.discard(channel_id)
            self._cv.notify_all()

    def is_channel_in_flight(self, channel_id: str) -> bool:
        """Check if channel has an in-flight unacknowledged task."""
        with self._lock:
            return channel_id in self._channel_in_flight


    def register_flow(
        self,
        flow_id: str,
        priority: int,
        capability: FlowProcessingCapability = FlowProcessingCapability.MANAGED_QUEUE,
        metadata: Optional[dict[str, Any]] = None,
    ) -> FlowInfo:
        """Register a flow (e.g. video or message channel) with metadata and capability."""
        with self._lock:
            info = FlowInfo(
                flow_id=flow_id,
                priority=priority,
                capability=capability,
                metadata=metadata or {},
            )
            self._registered_flows[flow_id] = info
            return info

    def get_flow(self, flow_id: str) -> Optional[FlowInfo]:
        """Retrieve registered flow info."""
        with self._lock:
            return self._registered_flows.get(flow_id)

    def register_channel(self, channel_id: str, mode: DeliveryMode) -> None:
        """Register a message channel and its delivery mode."""
        with self._lock:
            if channel_id not in self._channel_tasks:
                self._channel_tasks[channel_id] = []
            self._channel_modes[channel_id] = mode

    def submit(
        self,
        task_id: str,
        channel_id: str,
        priority: int,
        mode: DeliveryMode,
        sequence: int,
        fn: Callable[[], Any],
        latest_key: Optional[str] = None,
        ticket: Optional[DeliveryTicket] = None,
    ) -> ScheduledTask:
        """Submit a task to the scheduler respecting channel queue bounds and mode rules.
        
        Raises:
            QueueFullError if channel queue exceeds capacity.
        """
        with self._lock:
            if channel_id not in self._channel_tasks:
                self._channel_tasks[channel_id] = []
            self._channel_modes[channel_id] = mode

            tasks = self._channel_tasks[channel_id]
            self._insertion_counter += 1
            insertion_index = self._insertion_counter

            if mode == DeliveryMode.LATEST_ONLY:
                # Key coalescing: replace existing task for the same latest_key
                key = latest_key or ""
                replaced_index = -1
                for idx, existing in enumerate(tasks):
                    if (existing.latest_key or "") == key:
                        replaced_index = idx
                        break

                if replaced_index >= 0:
                    old_task = tasks[replaced_index]
                    if old_task.ticket:
                        old_task.ticket.set_superseded()
                    new_task = ScheduledTask(
                        task_id=task_id,
                        channel_id=channel_id,
                        priority=priority,
                        mode=mode,
                        sequence=sequence,
                        insertion_index=insertion_index,
                        fn=fn,
                        latest_key=latest_key,
                        ticket=ticket,
                    )
                    tasks[replaced_index] = new_task
                    self._cv.notify_all()
                    return new_task

            # Check queue capacity
            if len(tasks) >= self.max_queue_per_channel:
                raise QueueFullError(
                    f"Channel '{channel_id}' queue capacity reached ({self.max_queue_per_channel})"
                )

            task = ScheduledTask(
                task_id=task_id,
                channel_id=channel_id,
                priority=priority,
                mode=mode,
                sequence=sequence,
                insertion_index=insertion_index,
                fn=fn,
                latest_key=latest_key,
                ticket=ticket,
            )
            tasks.append(task)
            self._cv.notify_all()
            return task

    def start(self) -> None:
        """Start scheduler worker thread."""
        with self._lock:
            if self._running:
                return
            self._running = True
            self._worker_thread = threading.Thread(
                target=self._worker_loop,
                name=f"MultiflowScheduler-{self.resource_group}",
                daemon=True,
            )
            self._worker_thread.start()

    def stop(self, timeout: float = 2.0) -> None:
        """Stop worker thread and wait for completion."""
        worker = None
        with self._lock:
            if not self._running:
                return
            self._running = False
            self._cv.notify_all()
            worker = self._worker_thread
            self._worker_thread = None

        if worker and worker.is_alive():
            worker.join(timeout=timeout)

    def close(self) -> None:
        """Alias for stop() for idempotent lifecycle teardown."""
        self.stop()

    def _get_eligible_candidates(self) -> list[tuple[ScheduledTask, str, int]]:
        """Find candidate tasks eligible for immediate execution.
        
        Returns:
            list of (task, channel_id, index_in_channel_queue)
        """
        candidates: list[tuple[ScheduledTask, str, int]] = []
        for channel_id, tasks in self._channel_tasks.items():
            if not tasks:
                continue
            if channel_id in self._channel_in_flight:
                # Channel has an in-flight dispatch (window size 1); subsequent tasks wait
                continue
            mode = self._channel_modes.get(channel_id, DeliveryMode.ORDERED_ALL)
            if mode == DeliveryMode.ORDERED_ALL:
                # ORDERED_ALL: ONLY head of queue is eligible to preserve sequential ordering!
                candidates.append((tasks[0], channel_id, 0))
            else:
                # LATEST_ONLY: all pending tasks (different keys) are eligible
                for idx, t in enumerate(tasks):
                    candidates.append((t, channel_id, idx))
        return candidates


    def _select_next_task(self) -> Optional[ScheduledTask]:
        """Select next task according to priority tiers, starvation rule, and channel order."""
        candidates = self._get_eligible_candidates()
        if not candidates:
            return None

        # Group candidates by tier
        tier_map: dict[int, list[tuple[ScheduledTask, str, int]]] = {0: [], 1: [], 2: [], 3: []}
        for item in candidates:
            task = item[0]
            tier = get_priority_tier(task.priority)
            tier_map[tier].append(item)

        non_empty_tiers = [t for t in sorted(tier_map.keys(), reverse=True) if tier_map[t]]
        if not non_empty_tiers:
            return None

        highest_tier = non_empty_tiers[0]
        lower_waiting_tiers = [t for t in non_empty_tiers if t < highest_tier]

        # Starvation check: after 8 consecutive tasks from highest available tier,
        # pick 1 from lower waiting tier if any lower tasks exist
        if self._top_tier_streak >= MAX_TOP_TIER_STREAK and lower_waiting_tiers:
            chosen_tier = lower_waiting_tiers[0]
            self._top_tier_streak = 0  # reset cycle
        else:
            chosen_tier = highest_tier
            self._top_tier_streak += 1

        tier_candidates = tier_map[chosen_tier]

        # Inside tier: highest priority number wins; ties broken by insertion_index (FIFO)
        best_item = max(
            tier_candidates,
            key=lambda item: (item[0].priority, -item[0].insertion_index),
        )

        chosen_task, channel_id, task_idx = best_item
        # Remove task from channel queue
        channel_queue = self._channel_tasks[channel_id]
        if task_idx < len(channel_queue) and channel_queue[task_idx] is chosen_task:
            channel_queue.pop(task_idx)
        else:
            channel_queue.remove(chosen_task)

        self._last_executed_tiers.append(chosen_tier)
        return chosen_task

    def _worker_loop(self) -> None:
        """Main non-preemptive scheduler dispatch loop."""
        while True:
            task_to_run: Optional[ScheduledTask] = None
            with self._cv:
                while self._running:
                    task_to_run = self._select_next_task()
                    if task_to_run is not None:
                        break
                    self._cv.wait(timeout=0.1)

                if not self._running and task_to_run is None:
                    break

            if task_to_run is not None:
                # Crucial requirement: Callbacks/task execution OUTSIDE internal locks
                try:
                    task_to_run.fn()
                except Exception:
                    # Task function errors are handled at the caller/ticket level
                    pass
                with self._lock:
                    self._executed_tasks_count += 1
