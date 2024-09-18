"""
This module contains helper functions for the system.
"""
import asyncio
import logging
from typing import Dict, Any, List, Optional

import aiohttp

logger = logging.getLogger(__name__)


async def check_node_health(
        host: str,
        port: int,
        timeout: float = 5.0) -> bool:
    """
    Checks the health of a node by sending a GET request to its health endpoint.

    Args:
        host: The hostname of the node.
        port: The port number of the node.
        timeout: The timeout for the request in seconds. Defaults to 5.0.

    Return:
        bool: True if the node is healthy (Return a 200 status),
            False otherwise.
    """
    try:
        async with aiohttp.ClientSession() as session:
            url = f"http://{host}:{port}/health"
            async with session.get(url, timeout=timeout) as response:
                return response.status == 200
    except (aiohttp.ClientError, asyncio.TimeoutError) as e:
        logger.error(
            "Failed to check node health for %s:%s: %s",
            host,
            port,
            str(e))
        return False


async def send_alert(message: str, alert_url: str) -> None:
    """
    Sends an alert message to a specified URL.

    Args:
        message: The alert message to send.
        alert_url: The URL to send the alert to.
    """
    try:
        async with aiohttp.ClientSession() as session:
            await session.post(alert_url, json={'message': message})
    except aiohttp.ClientError as e:
        logger.error("Failed to send alert: %s. Error: %s", message, str(e))


def calculate_node_score(performance_metrics: Dict[str, Any]) -> float:
    """
    Calculates a node's performance score based on given metrics.

    Args:
        performance_metrics: A dictionary containing performance metrics.

    Return:
        float: The calculated performance score.
    """
    speed = performance_metrics.get('speed', 0)
    reliability = performance_metrics.get('reliability', 0)
    uptime = performance_metrics.get('uptime', 0)

    return (speed * 0.4) + (reliability * 0.4) + (uptime * 0.2)


def get_available_port(
        start_port: int,
        end_port: int,
        used_ports: List[int]) -> Optional[int]:
    """
    Finds an available port within a specified range.

    Args:
        start_port: The start of the port range.
        end_port: The end of the port range.
        used_ports: A list of ports that are already in use.

    Return:
        Optional: The first available port, or None if no ports are available.
    """
    available_ports = set(range(start_port, end_port + 1)) - set(used_ports)
    return min(available_ports) if available_ports else None


def setup_alert_logger(log_file: str) -> logging.Logger:
    """
    Sets up a logger for alerts.

    Args:
        log_file: The path to the log file.

    Return:
        logging.Logger: The configured logger for alerts.
    """
    alert_logger = logging.getLogger('alerts')
    alert_handler = logging.FileHandler(log_file)
    alert_handler.setFormatter(logging.Formatter(
        '%(asctime)s - %(levelname)s - %(message)s'))
    alert_logger.addHandler(alert_handler)
    alert_logger.setLevel(logging.WARNING)
    return alert_logger


def log_system_state(syslog: logging.Logger,
                     workers: Dict[str, Any],
                     task_queue: asyncio.PriorityQueue) -> None:
    """
    Logs the current state of the system and its workers.

    Args:
        logger: The logger to use for logging.
        workers: A dictionary of workers and their states.
        task_queue: The task queue.
    """
    state: Dict[str, Any] = {
        "total_workers": len(workers),
        "active_workers": sum(
            1 for worker in workers.values()
            if worker.status == 'active'),
        "queued_tasks": task_queue.qsize(),
    }
    syslog.info(f"System State: {state}")

    for worker_id, worker in workers.items():
        worker_state = {
            "worker_id": worker_id,
            "port": worker.port,
            "status": worker.status,
            "tasks": len(worker.tasks),
            "performance_score": worker.performance_score
        }
        syslog.info(f"Worker State: {worker_state}")
