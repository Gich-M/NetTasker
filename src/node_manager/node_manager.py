"""
Module for managing distributed worker nodes and task processing.
"""

import asyncio
import logging
from typing import Dict, Any, Optional
from aiohttp import web
from src.ipc.http_utils import create_http_server, \
    start_http_server, stop_http_server, receive_http_result
from src.task_processor import TaskProcessor
from src.utils.config import Config
from src.utils.logging_setup import logging_setup
from src.utils.helpers import setup_alert_logger, log_system_state
from src.node_manager.run_worker import Worker, TaskQueue
from src.node_manager.http_server import HttpServer
from src.node_manager.task_distributor import TaskDistributor, HealthChecker
from src.node_manager.monitoring import PerformanceMonitor, Scaler


class NodeManager:
    """
    Manages distributed worker nodes and coordinates task processing.
    """

    def __init__(self, config: Config) -> None:
        """
            Initializes the NodeManager with the provided configuration.

            Args:
                config (Config): The configuration object for the NodeManager.

            Attributes:
                logger: The logger object for logging events.
                config: The configuration object.
                workers: A dictionary of workers.
                task_queue: The task queue for managing tasks.
                lock: An asyncio lock for synchronization.
                running: Flag indicating if the NodeManager is running.
                results: A dictionary for storing results.
                task_processor: The task processor object.
                alert_logger: The logger object for alert messages.
                task_distributor: The task distributor object.
                performance_monitor: The performance monitor object.
                health_checker: The health checker object.
                scaler: The scaler object.
                http_server: The HTTP server object.
                host: The host address for the NodeManager.
                port: The port number for the NodeManager.
                runner: The web application runner.
        """
        self.logger = logging.getLogger(__name__)
        self.logger.info("Initializing NodeManager")

        self.config: Config = config
        self.workers: Dict[str, Worker] = {}
        self.task_queue: TaskQueue = TaskQueue()
        self.lock: asyncio.Lock = asyncio.Lock()
        self.running: bool = True
        self.results: Dict[str, Any] = {}

        self.task_processor: TaskProcessor = TaskProcessor(self.config)

        logging_setup(
            self.config.log.log_level,
            self.config.log.log_file,
            self.config.log.alert_log_file,
            self.config.log.error_log_file
        )

        self.logger = logging.getLogger(__name__)
        self.alert_logger = setup_alert_logger(self.config.log.alert_log_file)

        self.task_distributor: TaskDistributor = TaskDistributor(
            self.config, self.workers, self.task_queue, self.logger)
        self.performance_monitor: PerformanceMonitor = PerformanceMonitor(
            self.config, self.workers, self.logger)
        self.health_checker: HealthChecker = HealthChecker(
            self.config, self.workers, self.logger)
        self.scaler: Scaler = Scaler(self.config, self.workers, self.logger)

        self.http_server: HttpServer = HttpServer(
            config=self.config,
            task_queue=self.task_queue,
            workers=self.workers,
            results=self.results,
            logger=self.logger
        )

        self.host: str = self.config.processor.host
        self.port: int = self.config.processor.port
        self.runner: Optional[web.AppRunner] = None

    async def initialize_http_server(self) -> None:
        """
            Initialize the HTTP server.

            Sets up the web application and starts the HTTP server.
        """
        self.host, self.port, self.runner = await create_http_server(
            self.http_server.app,
            self.config.processor.host,
            self.config.processor.port
        )

        self.logger.info("NodeManager initialized successfully")

    async def run(self) -> None:
        """
            Runs the NodeManager, initializing components and handling
                main loop operations.

            Starts the NodeManager, sets up the HTTP server,
            checks Nginx status, initializes workers, and enters a main loop
            to monitor worker health, distribute tasks, process results,
            monitor performance, scale workers, log system state,
            and handle exceptions. It ends when the running flag
                is set to False.

            Returns:
                None
        """
        self.logger.info("NodeManager started")
        await self.initialize_http_server()
        await start_http_server(self.runner)
        internal_url = f"http://{self.host}:{self.port}"
        self.logger.info("NodeManager is listening on %s", internal_url)

        if self.config.nginx.enabled:
            external_url = (f"http://{self.config.nginx.address}:"
                            f"{self.config.nginx.port}")
            self.logger.info("Nginx is listening on %s", external_url)
            self.logger.info(
                "NodeManager is running behind Nginx at %s",
                external_url)
        else:
            self.logger.info(
                "Nginx is not enabled. NodeManager is directly accessible.")

        await self.initialize_workers()

        while self.running:
            try:
                self.logger.debug("Starting main loop iteration")
                await self.health_checker.check_workers_health()
                await self.task_distributor.distribute_tasks()
                await self.fetch_and_process_results()
                await self.performance_monitor.monitor_performance()
                await self.scaler.scale_workers()
                log_system_state(self.logger, self.workers, self.task_queue)
                await asyncio.sleep(self.config.task.health_check_interval)
            except asyncio.CancelledError:
                self.logger.info("NodeManager received cancellation signal")
                break
            except Exception as e:
                self.logger.error(
                    "Error in main loop: %s", str(e), exc_info=True)
                await asyncio.sleep(5)

        self.logger.info("NodeManager main loop ended")
        await stop_http_server(self.runner)

    def stop(self) -> None:
        """
            Stop the NodeManager.

            Sets the running flag to False, causing the main loop to terminate.
            Any tasks currently being processed are completed
                before the loop ends.
        """
        self.logger.info("Stopping NodeManager")
        self.running = False

    async def initialize_workers(self) -> None:
        """
            Initialize and start worker processes based on the configuration.

            Iterates through the specified number of workers in the
                configuration, assigns a unique ID and port number to each
                worker, and attempts to start the worker process. If a worker
                cannot be started due to insufficient ports, a warning message
                is logged.

            Parameters:
            - self: The instance of the NodeManager class.

            Returns:
            - None
        """
        for i in range(self.config.processor.num_workers):
            worker_id = f"worker_{i+1}"
            worker_port = self.config.processor.port_range_start + i
            if worker_port <= self.config.processor.port_range_end:
                try:
                    self.logger.debug(
                        "Attempting to start worker %s on port %d",
                        worker_id,
                        worker_port)
                    worker = Worker(worker_id, worker_port, config=self.config)
                    self.workers[worker_id] = worker
                    await worker.start_worker_process()
                    self.logger.info(
                        "Initialized worker: %s on port %d",
                        worker_id,
                        worker_port)
                except Exception as e:
                    self.logger.error(
                        "Failed to initialize worker %s: %s",
                        worker_id,
                        str(e),
                        exc_info=True)
            else:
                self.logger.warning(
                    "Not enough ports available to initialize worker: %s",
                    worker_id)
            await asyncio.sleep(1)

    async def fetch_and_process_results(self) -> None:
        """
            Fetches and processes results from all active worker processes.

            Iterates through active workers, fetches results from their URLs,
                and logs the processing status.
            If no active workers are found, it logs a debug message
                and returns without processing.
        """
        if not self.workers:
            self.logger.debug("No active workers to fetch and process results")
            return
        self.logger.debug("Fetching and processing results from all workers")
        for worker_id, worker in self.workers.items():
            if worker.status == 'active':
                try:
                    url = (f"http://{self.config.processor.host}:"
                           f"{worker.port}/get_results")
                    await receive_http_result(url)
                    self.logger.info(
                        "Processed results from worker %s", worker_id)
                except Exception as e:
                    self.logger.error(
                        "Error fetching results from worker %s: %s",
                        worker_id, str(e))

    async def cleanup(self) -> None:
        """
            Performs cleanup operations for the NodeManager.

            Stops worker processes and stops the HTTP server.
        """
        self.logger.info("Starting NodeManager cleanup")
        for _, worker in list(self.workers.items()):
            await worker.stop_worker_process(worker.process)
        await stop_http_server(self.runner)
        self.logger.info("NodeManager cleanup completed")


async def main() -> None:
    """
    Main entry point for the NodeManager application.

    Initializes the logging, creates a configuration object,
        initializes the NodeManager, and runs the main loop. It also handles
        exceptions during the execution and performs cleanup operations.
    """
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    config = Config()

    node_manager = NodeManager(config)

    try:
        await node_manager.run()
    except KeyboardInterrupt:
        logger.info("Shutting down...")
    except Exception as e:
        logger.error("An error occurred: %s", str(e), exc_info=True)
    finally:
        try:
            await node_manager.cleanup()
        except AttributeError as ae:
            logger.error(
                "Cleanup error: %s. Ensure all attributes are \
                    properly initialized.", str(ae))
        except Exception as e:
            logger.error("Error during cleanup: %s", str(e), exc_info=True)

if __name__ == "__main__":
    asyncio.run(main())
