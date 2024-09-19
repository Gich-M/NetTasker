"""CLI monitor for task processing and result retrieval."""

import argparse
import asyncio
import json
import aiohttp


class TaskCLI:
    """Command-line interface for task submission and result retrieval."""

    def __init__(self, host, port):
        """
        Initializes the TaskCLI with the provided host and port.

        Args:
            host: The host of the task processing node manager.
            port: The port of the task processing node manager.

        Attributes:
            base_url: The base URL for making requests to the task processing
            node manager.
        """
        self.base_url = f"http://{host}:{port}"

    async def submit_task(self, task_type, task):
        """
        Submits a task to the task processing node manager.

        Args:
            task_type: The type of task to submit.
            task: The task data to submit.

        Return:
            dict: The response from the task processing node manager.
        """
        async with aiohttp.ClientSession() as session:
            url = f"{self.base_url}/receive_task"
            payload = {
                "task_type": task_type,
                "task": task
            }
            async with session.post(url, json=payload) as response:
                return await response.json()

    async def get_results(self):
        """
        Retrieves all results from the task processing node manager.

        Returns:
            dict: The response from the task processing node manager.
        """
        async with aiohttp.ClientSession() as session:
            url = f"{self.base_url}/results"
            async with session.get(url) as response:
                return await response.json()


async def main():
    """
    Asynchronous function to handle task submission and
        result retrieval through the command-line interface.
     - Parses command-line arguments for host, port,
        task submission, and result retrieval.
     - Submits a task or retrieves all results based on the provided command.
     - Prints the JSON-formatted result or results with indentation
        for better readability.
    """
    parser = argparse.ArgumentParser(description="Task Processing CLI")
    parser.add_argument(
        "--host",
        default="localhost",
        help="Node manager host")
    parser.add_argument(
        "--port",
        type=int,
        default=8080,
        help="Node manager port")
    subparsers = parser.add_subparsers(dest="command", required=True)

    submit_parser = subparsers.add_parser("submit", help="Submit a task")
    submit_parser.add_argument(
        "task_type",
        choices=[
            "ip_data",
            "port_scan",
            "os_fingerprint",
            "traceroute",
            "dns_enum",
            "whois",
            "subnet",
            "reverse_dns",
            "banner_grab"],
        help="Type of task to submit")
    submit_parser.add_argument("task", help="Task data (e.g., IP address)")

    subparsers.add_parser("results", help="Get all results")

    args = parser.parse_args()
    cli = TaskCLI(args.host, args.port)

    if args.command == "submit":
        result = await cli.submit_task(args.task_type, args.task)
        print(json.dumps(result, indent=2))
    elif args.command == "results":
        results = await cli.get_results()
        print(json.dumps(results, indent=2))

if __name__ == "__main__":
    asyncio.run(main())
