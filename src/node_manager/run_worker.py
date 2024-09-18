"""Module for managing worker processes and task queues."""

import asyncio
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional
import os
import logging
from src.utils.helpers import check_node_health
from src.utils.config import Config


@dataclass
class Worker:
    """
    A class representing a worker process. It manages the worker's
        initialization, health checks, termination, and task execution.

    Attributes:
        worker_id: The unique identifier for the worker.
        port: The port number on which the worker process will run.
        status: The current status of the worker (default: 'inactive').
        performance_score: The performance score of the worker (default: 1.0).
        node_manager_info: A tuple containing node manager host and port.
        config: The configuration object for the worker.
        logger: The logger object for the worker.
        tasks: A list of tasks assigned to the worker.
    """
    worker_id: str
    port: int
    status: str = 'inactive'
    performance_score: float = 1.0
    node_manager_info: tuple = ('127.0.0.1', 8081)
    config: Optional[Config] = None
    logger: logging.Logger = logging.getLogger(__name__)
    tasks: List[Dict[str, Any]] = field(default_factory=list)

    async def start_worker_process(self) -> None:
        """
        Starts the worker process.

        This function initializes and starts a worker process by executing
        the worker script. It checks for the existence of the
        worker script, logs relevant information, and
        handles potential errors during the process.

        Return:
        None
        """
        self.logger.info(
            "Starting worker process: %s on port %s",
            self.worker_id,
            self.port)
        worker_script = os.path.abspath(
            os.path.join(
                os.path.dirname(__file__),
                '..',
                'workers',
                'worker'))

        if not os.path.exists(worker_script):
            self.logger.error("Worker script not found: %s", worker_script)
            self.status = 'inactive'
            return

        self.logger.debug("Worker script permissions: %s",
                          oct(os.stat(worker_script).st_mode)[-3:])

        command = [
            worker_script,
            self.config.processor.host,
            str(self.config.processor.port),
            self.worker_id
        ]

        self.logger.debug("Worker start command: %s", ' '.join(command))

        try:
            process = await asyncio.create_subprocess_exec(
                *command,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )

            self.logger.info(
                "Worker %s started with PID %s", self.worker_id, process.pid)

            try:
                stdout, stderr = await asyncio.wait_for(
                        process.communicate(), timeout=10.0)
            except asyncio.TimeoutError:
                self.logger.warning(
                    "Timeout while waiting for worker %s output",
                    self.worker_id)
                stdout, stderr = b'', b''

            if stdout:
                self.logger.debug(
                    "Worker %s stdout: %s",
                    self.worker_id,
                    stdout.decode().strip())
            if stderr:
                self.logger.error(
                    "Worker %s stderr: %s",
                    self.worker_id,
                    stderr.decode().strip())
            else:
                self.logger.warning(
                    "No stderr output from worker %s", self.worker_id)

            if process.returncode is not None:
                self.logger.error(
                    "Worker %s terminated unexpectedly with return code %s",
                    self.worker_id, process.returncode)
                self.status = 'inactive'
                return

            await asyncio.sleep(5)
            is_healthy = await check_node_health(
                    self.config.processor.host, self.port)
            if is_healthy:
                self.logger.info(
                    "Worker %s initialized successfully", self.worker_id)
                self.status = 'active'
            else:
                self.logger.warning(
                    "Worker %s failed initial health check", self.worker_id)
                if process.returncode is None:
                    await self.stop_worker_process(process)

        except Exception as e:
            self.logger.error(
                "Failed to start worker %s: %s",
                self.worker_id, str(e),
                exc_info=True)
            self.status = 'inactive'

    async def stop_worker_process(self, process):
        """
        Stop the worker process.

        This function attempts to gracefully terminate the worker process.
        If the process doesn't terminate within 5 seconds, it forcefully
        kills the process.

        Args:
        process: The process object representing the worker process.

        Return:
        None
        """
        if process:
            process.terminate()
            try:
                await asyncio.wait_for(process.wait(), timeout=5.0)
            except asyncio.TimeoutError:
                self.logger.warning(
                    "Worker %s did not terminate within 5 seconds, \
                        forcing termination.", self.worker_id)
                process.kill()
                await process.wait()
            finally:
                self.status = 'inactive'
        else:
            self.logger.warning(
                "Attempted to stop worker %s, but no process was running.",
                self.worker_id)


@dataclass(order=True)
class PrioritizedItem:
    """
    A class representing a prioritized item in the task queue.

    Attributes:
    priority: An integer representing the priority of the item.
    item: A dictionary containing the actual task data.
    """
    priority: int
    item: Dict[str, Any] = field(compare=False)


class TaskQueue(asyncio.PriorityQueue):
    """
    A priority queue for managing tasks.

    This class extends asyncio.PriorityQueue to provide a task queue
    with priority ordering.
    """

    async def put(self, item: PrioritizedItem) -> None:
        """
        Adds a prioritized item to the queue.

        Args:
        item: A PrioritizedItem object to be added to the queue.

        Return:
        None
        """
        await super().put(item)

    async def get(self) -> PrioritizedItem:
        """
        Retrieves and removes the highest priority item from the queue.

        Return:
        PrioritizedItem: The highest priority item in the queue.
        """
        return await super().get()

    def empty(self) -> bool:
        """
        Checks if the queue is empty.

        Return:
        bool: True if the queue is empty, False otherwise.
        """
        return self.qsize() == 0
