"""
    HTTP utility functions for creating, starting,
        and stopping servers, and handling tasks.
"""

import asyncio
import logging
from typing import Dict, Any, Tuple

import aiohttp
from aiohttp import web

logger = logging.getLogger(__name__)


async def create_http_server(
    app: web.Application,
    host: str,
    port: int
) -> Tuple[str, int, web.AppRunner]:
    """
    Creates an HTTP server.

    Args:
        app: The aiohttp application.
        host: The host to bind the server to.
        port: The port to bind the server to.

    Return:
        A tuple containing the host, port, and AppRunner instance.
    """
    logger.info("Creating HTTP server on %s:%s", host, port)
    runner = web.AppRunner(app)
    app['host'] = host
    app['port'] = port
    return host, port, runner


async def start_http_server(runner: web.AppRunner) -> None:
    """
    Starts the HTTP server.

    Args:
        runner: The AppRunner instance to start.
    """
    logger.info("Starting HTTP server")
    await runner.setup()
    site = web.TCPSite(runner, runner.app['host'], runner.app['port'])
    await site.start()
    logger.info(
        "HTTP server started on %s:%s",
        runner.app['host'],
        runner.app['port'])


async def stop_http_server(runner: web.AppRunner) -> None:
    """
    Stops the HTTP server.

    Args:
        runner: The AppRunner instance to stop.
    """
    logger.info("Stopping HTTP server")
    await runner.cleanup()
    logger.info("HTTP server stopped")


async def health_check(url: str, timeout: float = 5.0) -> bool:
    """
    Performs a health check on the given URL.

    Args:
        url: The URL to check.
        timeout: The timeout for the request. Defaults to 5.0 seconds.

    Return:
        True if the health check was successful, False otherwise.
    """
    logger.debug("Performing health check on %s", url)
    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(
                f"{url}/health",
                timeout=timeout
            ) as response:
                is_healthy = response.status == 200
                logger.info(
                    "Health check result for %s: %s",
                    url,
                    'Healthy' if is_healthy else 'Unhealthy'
                )
                return is_healthy
        except aiohttp.ClientError:
            logger.warning("Health check failed for %s", url)
            return False


async def check_nginx_health(url: str, timeout: float = 5.0) -> bool:
    """
    Checks the health of Nginx at the given URL.

    Args:
        url: The URL to check.
        timeout: The timeout for the request. Defaults to 5.0 seconds.

    Return:
        True if Nginx is healthy, False otherwise.
    """
    logger.debug("Checking Nginx health on %s", url)
    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(
                f"{url}/nginx_health",
                timeout=timeout
            ) as response:
                is_healthy = response.status == 200
                logger.info(
                    "Nginx health check result for %s: %s",
                    url,
                    'Healthy' if is_healthy else 'Unhealthy'
                )
                return is_healthy
        except aiohttp.ClientError:
            logger.warning("Nginx health check failed for %s", url)
            return False


async def send_http_task(
    task_logger: logging.Logger,
    session: aiohttp.ClientSession,
    url: str,
    task: Dict[str, Any],
    config: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Sends an HTTP task to the specified URL.

    Args:
        task_logger: Logger instance for logging messages.
        session: The aiohttp ClientSession to use for the request.
        url: The base URL to send the task to.
        task: The task data to send.
        config: Configuration dictionary containing:
            - timeout: Timeout for the request in seconds. Defaults to 5.0.
            - method: HTTP method to use. Defaults to 'POST'.
            - expect_json: Whether to expect a JSON response. Defaults to True.

    Return:
        A dictionary containing the success status and
            either the response data or error message.
    """
    task_id = task.get('task_id', 'unknown')
    timeout = config.get('timeout', 5.0)
    method = config.get('method', 'POST')
    expect_json = config.get('expect_json', True)

    try:
        async with session.request(
            method,
            f"{url}/receive_task",
            json=task,
            timeout=timeout
        ) as response:
            if response.status == 200:
                result = (await response.json() if expect_json
                          else await response.text())
                task_logger.info(f"Task {task_id} sent successfully")
                return {"success": True, "data": result}

            error_msg = f"HTTP request failed with status {response.status}"
            task_logger.error(f"Failed to send task {task_id}. {error_msg}")
            return {"success": False, "error": error_msg}
    except asyncio.TimeoutError:
        error_msg = "Request timed out"
        task_logger.error(f"Timeout sending task {task_id}")
        return {"success": False, "error": error_msg}
    except Exception as e:
        error_msg = f"Error sending HTTP task: {str(e)}"
        task_logger.error(
            f"Error sending task {task_id}: {str(e)}",
            exc_info=True)
        return {"success": False, "error": error_msg}


async def receive_http_result(
    url: str,
    timeout: float = 5.0
) -> Dict[str, Any]:
    """
    Receives HTTP results from the specified URL.

    Args:
        url: The URL to fetch results from.
        timeout: Timeout for the request in seconds. Defaults to 5.0.

    Return:
        The JSON response containing the results.

    Raises:
        aiohttp.ClientError: If there's an error in receiving the HTTP result.
    """
    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(
                f"{url}/results",
                timeout=timeout
            ) as response:
                if response.status == 200:
                    return await response.json()
                raise aiohttp.ClientResponseError(
                    response.request_info,
                    response.history,
                    status=response.status,
                    message=f"HTTP request failed with status\
                        {response.status}")
        except asyncio.TimeoutError as e:
            raise aiohttp.ClientError("Request timed out") from e
        except Exception as e:
            raise aiohttp.ClientError(
                f"Error receiving HTTP result: {str(e)}"
            ) from e
