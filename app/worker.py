"""
Per-assistant async worker pool with priority queue, API rate-limiting semaphore,
dynamic worker scaling, and a broadcast lock.

Priority levels (lower value = processed first):
  BROADCAST = 1  – admin-initiated broadcast messages
  LOG       = 2  – forwarding user messages to the log chat
  USER      = 3  – outgoing auto-replies to individual users
"""

import asyncio
import enum
import logging
import time
from collections.abc import Coroutine
from typing import Any

logger = logging.getLogger(__name__)


class Priority(int, enum.Enum):
    """Task priority for the worker queue.  Lower value = processed first."""

    BROADCAST = 1
    LOG = 2
    USER = 3


class WorkerPool:
    """
    Isolated async worker pool for a single assistant bot.

    Features
    --------
    - ``asyncio.PriorityQueue`` → tasks are drained in BROADCAST > LOG > USER order.
    - Shared ``api_sem`` semaphore that callers acquire around each Telegram API
      call to cap total concurrent network operations.
    - ``broadcast_lock`` that must be held for the duration of a broadcast to
      prevent two broadcasts running simultaneously on the same assistant.
    - Dynamic worker scaling: a lightweight scaler coroutine adds workers when
      queue depth exceeds ``SCALE_UP_THRESHOLD`` and lets idle surplus workers
      exit after ``SCALE_DOWN_IDLE`` seconds.
    """

    MIN_WORKERS: int = 2
    MAX_WORKERS: int = 10
    SCALE_UP_THRESHOLD: int = 5     # queue depth that triggers a new worker
    SCALE_DOWN_IDLE: float = 30.0   # idle seconds before a surplus worker exits
    API_CONCURRENCY: int = 8        # max simultaneous Telegram API calls

    def __init__(self, assistant_id: str) -> None:
        self.assistant_id = assistant_id
        self._queue: asyncio.PriorityQueue[tuple[int, int, Coroutine[Any, Any, Any]]] = (
            asyncio.PriorityQueue()
        )
        # Acquired by callers around each Telegram API call (not around whole tasks).
        self.api_sem = asyncio.Semaphore(self.API_CONCURRENCY)
        # Must be held for the full duration of a broadcast.
        self.broadcast_lock = asyncio.Lock()
        self._workers: list[asyncio.Task[None]] = []
        self._scaler_task: asyncio.Task[None] | None = None
        self._running = False
        self._seq = 0

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def enqueue_nowait(
        self,
        coro: Coroutine[Any, Any, Any],
        priority: Priority = Priority.USER,
    ) -> None:
        """Schedule *coro* immediately (non-blocking).

        Safe to call from both sync and async contexts.  The *seq* counter
        provides FIFO ordering within the same priority level.
        """
        self._seq += 1
        self._queue.put_nowait((priority.value, self._seq, coro))

    def queue_depth(self) -> int:
        """Return the number of tasks currently waiting in the queue."""
        return self._queue.qsize()

    def active_workers(self) -> int:
        """Return the number of worker tasks that have not yet finished."""
        return sum(1 for w in self._workers if not w.done())

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def start(self) -> None:
        self._running = True
        for _ in range(self.MIN_WORKERS):
            self._add_worker()
        self._scaler_task = asyncio.create_task(
            self._scaler(), name=f"scaler-{self.assistant_id}"
        )
        logger.info(
            "WorkerPool[%s] started — workers=%d, api_concurrency=%d",
            self.assistant_id,
            self.MIN_WORKERS,
            self.API_CONCURRENCY,
        )

    async def stop(self) -> None:
        self._running = False
        if self._scaler_task and not self._scaler_task.done():
            self._scaler_task.cancel()
            try:
                await self._scaler_task
            except asyncio.CancelledError:
                pass
        for w in list(self._workers):
            w.cancel()
        if self._workers:
            await asyncio.gather(*self._workers, return_exceptions=True)
        self._workers.clear()
        logger.info("WorkerPool[%s] stopped", self.assistant_id)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _add_worker(self) -> asyncio.Task[None]:
        wid = len(self._workers)
        task: asyncio.Task[None] = asyncio.create_task(
            self._worker(wid), name=f"worker-{self.assistant_id}-{wid}"
        )
        self._workers.append(task)
        return task

    async def _worker(self, worker_id: int) -> None:
        idle_since: float | None = None
        while self._running:
            try:
                item = await asyncio.wait_for(self._queue.get(), timeout=5.0)
            except asyncio.TimeoutError:
                # No work arrived within the window.
                if idle_since is None:
                    idle_since = time.monotonic()
                if (
                    self.active_workers() > self.MIN_WORKERS
                    and time.monotonic() - idle_since >= self.SCALE_DOWN_IDLE
                ):
                    logger.debug(
                        "WorkerPool[%s] worker-%d scaling down after %.0fs idle",
                        self.assistant_id,
                        worker_id,
                        self.SCALE_DOWN_IDLE,
                    )
                    break
                continue
            except asyncio.CancelledError:
                break

            idle_since = None
            _, _, coro = item
            try:
                await coro
            except Exception:
                logger.exception(
                    "WorkerPool[%s] worker-%d: unhandled exception in task",
                    self.assistant_id,
                    worker_id,
                )
            finally:
                self._queue.task_done()

    async def _scaler(self) -> None:
        """Periodically add a worker when the queue depth exceeds the threshold."""
        while self._running:
            await asyncio.sleep(2)
            # Remove references to completed workers.
            self._workers = [w for w in self._workers if not w.done()]
            depth = self._queue.qsize()
            active = len(self._workers)
            if depth >= self.SCALE_UP_THRESHOLD and active < self.MAX_WORKERS:
                self._add_worker()
                logger.debug(
                    "WorkerPool[%s] scaled up → %d workers (queue depth=%d)",
                    self.assistant_id,
                    active + 1,
                    depth,
                )
