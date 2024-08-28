import asyncio
import aiohttp
from typing import Dict, Any


async def check_node_health(node_url: str, timeout: float = 5.0) -> bool:
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get("%s / health", node_url, timeout=timeout) as response:
                return response.status == 200
    except (aiohttp.ClientError, asyncio.TimeoutError):
        return False
    
def calculate_node_score(performance_metrics: Dict[str, Any]) -> float:
    speed = performance_metrics.get('speed', 0)
    reliability = performance_metrics.get('reliability', 0)
    uptime = performance_metrics.get('uptime', 0)

    return (speed * 0.4) + (reliability * 0.4) + (uptime * 0.2)

async def send_alert(message: str, alert_url: str) -> None:
    try:
        async with aiohttp.ClientSession() as session:
            await session.post(alert_url, json={'message': message})
    except aiohttp.ClientError:
        print("Failed to send alert: %s", message)