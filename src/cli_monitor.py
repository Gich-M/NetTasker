"""
CLI Monitor for NodeManager

This module provides a command-line interface to monitor the results
from the NodeManager.
"""

from typing import Dict, Any
import logging
import asyncio
import aiohttp


logger = logging.getLogger(__name__)


async def display_results(node_manager_url: str) -> None:
    """
    Fetches and displays results from the NodeManager.

    Args:
        node_manager_url (str): The URL of the NodeManager.

    Returns:
        None
    """
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(f"{node_manager_url}/results") as response:
                if response.status == 200:
                    results: Dict[str, Any] = await response.json()
                    if results:
                        for task_id, result_data in results.items():
                            logger.info("Task %s:", task_id)
                            logger.info(
                                "  Worker: %s", result_data['worker_id'])
                            logger.info("  Result: %s", result_data['result'])
                            logger.info(
                                "  Timestamp: %s", result_data['timestamp'])
                            logger.info("")
                    else:
                        logger.info("No results available")
                else:
                    logger.error("Failed to get results: %s", response.status)
    except aiohttp.ClientError as e:
        logger.error("Error connecting to NodeManager: %s", str(e))
    except asyncio.TimeoutError:
        logger.error("Request to NodeManager timed out")
    except ValueError as e:
        logger.error("Error parsing response from NodeManager: %s", str(e))
    except Exception as e:
        logger.error("Unexpected error fetching results: %s", str(e))
        logger.exception("Stack trace:", exc_info=True)

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    MANAGER_URL = "http://localhost:8080"
    asyncio.run(display_results(MANAGER_URL))
