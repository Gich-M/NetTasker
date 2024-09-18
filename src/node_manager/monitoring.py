"""
    This module contains classes for monitoring and
        scaling workers in a distributed system.
"""

from typing import Any, Dict
import logging
from src.utils.helpers import get_available_port, \
    check_node_health, calculate_node_score
from .run_worker import Worker
from .task_distributor import HealthChecker


class Scaler:
    """Manages the scaling of workers based on system load."""

    def __init__(self,
                 config: Any,
                 workers: Dict[str,
                               Worker],
                 logger: logging.Logger):
        """
        Initialize the Scaler.

        Args:
            config: Configuration object.
            workers: Dictionary of active workers.
            logger: Logger object for logging events.
        """
        self.config = config
        self.workers = workers
        self.logger = logger

    async def scale_workers(self) -> None:
        """
        Determine if scaling up or down is necessary based on
            current worker load.
        """
        active_workers = sum(
            1 for worker in self.workers.values() if worker.status == 'active')
        total_workers = len(self.workers)

        self.logger.info(
            f"Current worker status: {active_workers} \
                active out of {total_workers} total")

        if active_workers / total_workers > \
                self.config.scaling.scale_up_threshold:
            await self.scale_up()
        elif active_workers / total_workers < \
                self.config.scaling.scale_down_threshold:
            await self.scale_down()

    async def scale_up(self) -> None:
        """
        Add a new worker if the maximum worker limit hasn't been reached.
        """
        if len(self.workers) < self.config.scaling.max_workers:
            new_worker_id = f"worker_{len(self.workers) + 1}"
            used_ports = [worker.port for worker in self.workers.values()]
            new_port = get_available_port(
                self.config.processor.port_range_start,
                self.config.processor.port_range_end,
                used_ports
            )
            if new_port:
                await self.start_worker_process(new_worker_id, new_port)
                self.logger.info(
                    f"Scaled up: Added new worker {new_worker_id} \
                        on port {new_port}")
            else:
                self.logger.warning("Unable to scale up: No available ports")
        else:
            self.logger.info(
                "Max worker limit reached, unable to scale up further")

    async def start_worker_process(self, worker_id: str, port: int) -> None:
        """
        Start a new worker process.

        Args:
            worker_id: Unique identifier for the new worker.
            port: Port number for the new worker.
        """
        new_worker = Worker(worker_id, port, config=self.config)
        self.workers[worker_id] = new_worker
        await new_worker.start_worker_process()

    async def scale_down(self) -> None:
        """
        Remove the lowest performing worker if the minimum worker
            limit hasn't been reached.
        """
        if len(self.workers) > self.config.scaling.min_workers:
            worker_to_remove = min(
                (w for w in self.workers.values() if w.status == 'active'),
                key=lambda w: w.performance_score,
                default=None
            )
            if worker_to_remove:
                await worker_to_remove.stop_worker_process(
                    worker_to_remove.process)
                del self.workers[worker_to_remove.worker_id]
                self.logger.info(
                    f"Scaled down: Removed worker \
                        {worker_to_remove.worker_id}")
            else:
                self.logger.warning(
                    "No active workers available for scale down")
        else:
            self.logger.info(
                "Min worker limit reached, unable to scale down further")


class PerformanceMonitor:
    """Monitors the performance of workers and handles health checks."""

    def __init__(self,
                 config: Any,
                 workers: Dict[str,
                               Worker],
                 logger: logging.Logger):
        """
        Initialize the PerformanceMonitor.

        Args:
            config: Configuration object.
            workers: Dictionary of active workers.
            logger: Logger object for logging events.
        """
        self.config = config
        self.workers = workers
        self.logger = logger
        self.health_checker = HealthChecker(
            self.config, self.workers, self.logger)

    async def monitor_performance(self) -> None:
        """
        Monitor the performance of all active workers.
        """
        self.logger.debug("Monitoring performance of all workers")
        for worker_id, worker in self.workers.items():
            if worker.status == 'active':
                try:
                    is_healthy = await check_node_health(
                        self.config.processor.host,
                        worker.port
                    )
                    if is_healthy:
                        self.logger.info(
                            f"Worker {worker_id} is healthy")
                    else:
                        self.logger.error(
                            f"Worker {worker_id} health check failed")
                        await self.health_checker.handle_worker_failure(
                            worker_id)
                except Exception as e:
                    self.logger.error(
                        f"Unexpected error while monitoring worker \
                            {worker_id}: {str(e)}",
                        exc_info=True)

    async def update_worker_metrics(
            self,
            worker_id: str,
            metrics: Dict[str, Any]) -> None:
        """
        Update the performance metrics for a specific worker.

        Args:
            worker_id: Unique identifier of the worker.
            metrics: Dictionary containing performance metrics.
        """
        if worker_id in self.workers:
            worker = self.workers[worker_id]
            worker.performance_score = calculate_node_score(metrics)
            self.logger.debug(
                f"Updated metrics for worker {worker_id}: {metrics}")
        else:
            self.logger.warning(
                f"Received metrics for unknown worker {worker_id}")
