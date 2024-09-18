"""Module for task distribution and health checking."""
from typing import Dict, List, Any
import aiohttp
from src.utils.helpers import check_node_health
from src.ipc.http_utils import send_http_task
from .run_worker import Worker, PrioritizedItem, TaskQueue


class TaskDistributor:
    """
    A class responsible for distributing tasks to available workers.

    This class manages the distribution of tasks from a queue to
        active workers, handles task assignment, and
        manages task reassignment in case of failures.

    Attributes:
        config: Configuration object containing system settings.
        workers (Dict[str, Worker]): A dictionary of available workers.
        task_queue (TaskQueue): A priority queue of tasks to be distributed.
        logger: Logger object for logging events and errors.
    """

    def __init__(self,
                 config: Any,
                 workers: Dict[str, Worker],
                 task_queue: TaskQueue,
                 logger: Any):
        self.config = config
        self.workers = workers
        self.task_queue = task_queue
        self.logger = logger

    async def distribute_tasks(self) -> None:
        """
        Distributes tasks from the task queue to available workers.

        This method continuously assigns tasks to the most suitable
            active worker until the task queue is empty
            or no active workers are available.
        """
        while not self.task_queue.empty():
            priority_item = await self.task_queue.get()
            task = priority_item.item

            available_workers = [
                w for w in self.workers.values() if w.status == 'active']
            if not available_workers:
                self.logger.warning(
                    "No active workers available. Requeueing task.")
                await self.task_queue.put(priority_item)
                break

            worker = max(available_workers, key=lambda w: w.performance_score)
            worker.tasks.append(task)
            self.logger.info(
                f"Assigned task {task['task_id']} to worker \
                    {worker.worker_id}")

            try:
                worker_url = f"http://{self.config.processor.host}:"\
                    f"{worker.port}/process"
                async with aiohttp.ClientSession() as session:
                    result = await send_http_task(
                        self.logger, session, worker_url, task, self.config)
                    if not result['success']:
                        worker.tasks.remove(task)
                        await self.task_queue.put(priority_item)
            except Exception as e:
                self.logger.error(
                    f"Failed to send task to worker {worker.worker_id}: \
                        {str(e)}")
                worker.tasks.remove(task)
                await self.task_queue.put(priority_item)

    async def reassign_tasks(self,
                             tasks_to_reassign: List[Dict[str, Any]]) -> None:
        """
        Reassigns a list of tasks back to the task queue.

        Args:
            tasks_to_reassign: A list of tasks to be reassigned.
        """
        for task in tasks_to_reassign:
            await self.task_queue.put(PrioritizedItem(priority=1, item=task))
        self.logger.info(f"Reassigned {len(tasks_to_reassign)} tasks")


class HealthChecker:
    """
    A class responsible for checking and managing the health of workers.

    This class performs health checks on workers and handles worker failures
    by reassigning tasks from failed workers.

    Attributes:
        config: Configuration object containing system settings.
        workers: A dictionary of workers to be health-checked.
        logger: Logger object for logging events and errors.
        task_distributor (TaskDistributor): An instance of TaskDistributor
            for task reassignment.
    """

    def __init__(self, config: Any, workers: Dict[str, Worker], logger: Any):
        self.config = config
        self.workers = workers
        self.logger = logger
        self.task_distributor = TaskDistributor(
            self.config, self.workers, None, self.logger)

    async def check_workers_health(self) -> None:
        """
        Performs health checks on all workers and handles any failures
            detected.
        """
        self.logger.info("Starting health check for all workers")
        for worker_id, worker in self.workers.items():
            self.logger.debug(f"Checking health of worker {worker_id}")
            try:
                is_healthy = await check_node_health(
                    self.config.processor.host, worker.port)
                self.logger.debug(
                    f"Health check result for worker {worker_id}: \
                        {is_healthy}")
                if is_healthy:
                    worker.status = 'active'
                    self.logger.info(f"Worker {worker_id} is active")
                else:
                    self.logger.warning(
                        f"Worker {worker_id} health check failed")
                    await self.handle_worker_failure(worker_id)
            except Exception as e:
                self.logger.error(
                    f"Error during health check for worker {worker_id}: \
                        {str(e)}",
                    exc_info=True)
        self.logger.info("Completed health check for all workers")

    async def handle_worker_failure(self, worker_id: str) -> None:
        """
        Handles the failure of a worker by reassigning its tasks.

        Args:
            worker_id (str): The ID of the failed worker.
        """
        if worker_id in self.workers:
            worker = self.workers[worker_id]
            worker.status = 'inactive'
            tasks_to_reassign = worker.tasks
            worker.tasks = []
            await self.task_distributor.reassign_tasks(tasks_to_reassign)
            self.logger.warning(
                f"Worker {worker_id} is inactive, reassigned \
                    {len(tasks_to_reassign)} tasks")
