"""HTTP server module for handling task management and worker communication."""

from datetime import datetime
from typing import Dict, Any
import uuid
from aiohttp import web
from src.ipc.http_utils import check_nginx_health
from .run_worker import PrioritizedItem


class HttpServer:
    """HTTP server for managing tasks and worker communication."""

    def __init__(
            self,
            config: Any,
            task_queue: Any,
            workers: Dict[str, Any],
            results: Dict[str, Any],
            logger: Any):
        """
        Initialize the HTTP server.

        Args:
            config: The configuration object.
            task_queue: The queue for tasks.
            workers: The dictionary of workers.
            results: The dictionary of results.
            logger: The logger object.
        """
        self.config = config
        self.task_queue = task_queue
        self.workers = workers
        self.results = results
        self.logger = logger
        self.app = web.Application()
        self.setup_routes()

    def setup_routes(self) -> None:
        """Set up the routes for the HTTP server."""
        self.app.router.add_post('/receive_task', self.receive_task)
        self.app.router.add_post('/process_task', self.handle_task)
        self.app.router.add_get('/health', self.health_check)
        self.app.router.add_get('/nginx_health', self.nginx_health_check)
        self.app.router.add_post('/submit_result', self.submit_result)
        self.app.router.add_get('/results', self.get_results)

    async def receive_task(self, request: web.Request) -> web.Response:
        """
        Receive a task from a client and queue it.

        Args:
            request: The incoming request object.

        Return:
            JSON response indicating success or failure.
        """
        try:
            data = await request.json()
            task = data.get('task')
            task_type = data.get('task_type', 'ip_data')
            task_id = str(uuid.uuid4())
            client_ip = self.get_client_ip(request)
            priority_item = PrioritizedItem(
                priority=1,
                item={
                    "task": task,
                    "task_type": task_type,
                    "task_id": task_id,
                    "client_ip": client_ip
                })
            await self.task_queue.put(priority_item)
            self.logger.info(
                f"Task received from {client_ip} and queued: \
                    {task_id} (Type: {task_type})")
            return web.json_response(
                {"message": "Task queued successfully", "task_id": task_id})
        except Exception as e:
            self.logger.error(f"Error receiving task: {str(e)}", exc_info=True)
            return web.json_response({"error": str(e)}, status=500)

    async def handle_task(self, request: web.Request) -> web.Response:
        """
        Handle a task request from a worker.

        Args:
            request: The incoming request object.

        Return:
            JSON response with a task or a message indicating
                no tasks available.
        """
        try:
            data = await request.json()
            worker_id = data.get('worker_id')
            self.logger.info(f"Received task request from worker: {worker_id}")

            if not self.task_queue.empty():
                priority_item = await self.task_queue.get()
                task = priority_item.item
                self.logger.info(
                    f"Assigning task {task['task_id']} to worker {worker_id}")
                return web.json_response({"task": task})
            self.logger.info(f"No tasks available for worker {worker_id}")
            return web.json_response({"message": "No tasks available"})
        except Exception as e:
            self.logger.error(
                f"Error handling task request: {str(e)}",
                exc_info=True)
            return web.json_response({"error": str(e)}, status=500)

    async def submit_result(self, request: web.Request) -> web.Response:
        """
        Submit a task result from a worker.

        Args:
            request: The incoming request object.

        Return:
            JSON response indicating success or failure.
        """
        try:
            data = await request.json()
            worker_id = data.get('worker_id')
            task_id = data.get('task_id')
            result = data.get('result')

            if not all([worker_id, task_id, result]):
                return web.json_response(
                    {"error": "Missing required fields"}, status=400)

            self.results[task_id] = {
                'worker_id': worker_id,
                'result': result,
                'timestamp': datetime.now().isoformat()
            }

            return web.json_response({"status": "result received and stored"})
        except Exception as e:
            self.logger.error(
                f"Error submitting result: {str(e)}",
                exc_info=True)
            return web.json_response({"error": str(e)}, status=500)

    async def get_results(self, _: web.Request) -> web.Response:
        """
        Get all stored results.

        Args:
            request: The incoming request object.

        Return:
            JSON response containing all stored results.
        """
        return web.json_response(self.results)

    async def health_check(self, request: web.Request) -> web.Response:
        """
        Perform a health check of the server.

        Args:
            request: The incoming request object.

        Return:
            JSON response containing health status information.
        """
        return web.json_response({
            "status": "healthy",
            "workers": len(self.workers),
            "active_workers": sum(
                1 for worker in self.workers.values()
                if worker.status == 'active'
            ),
            "tasks_queued": self.task_queue.qsize(),
            "behind_proxy": True,
            "client_ip": self.get_client_ip(request)
        })

    async def nginx_health_check(self, _: web.Request) -> web.Response:
        """
        Perform a health check of the Nginx server.

        Args:
            request: The incoming request object.

        Return:
            JSON response indicating Nginx health status.
        """
        try:
            is_healthy = await check_nginx_health(
                f"http://{self.config.nginx.address}:{self.config.nginx.port}")
            if is_healthy:
                return web.json_response({"status": "healthy"})
            return web.json_response({"status": "unhealthy"}, status=500)
        except Exception as e:
            self.logger.error(
                f"Error checking Nginx health: {str(e)}",
                exc_info=True)
            return web.json_response({"error": str(e)}, status=500)

    def get_client_ip(self, request: web.Request) -> str:
        """
        Get the client IP address from the request.

        Args:
            request: The incoming request object.

        Return:
            The client IP address.
        """
        x_forwarded_for = request.headers.get('X-Forwarded-For')
        if x_forwarded_for:
            return x_forwarded_for.split(',')[0].strip()
        return request.remote
