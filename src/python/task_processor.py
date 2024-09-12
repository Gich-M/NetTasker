"""Module for processing tasks related to IP address data."""
import ipaddress
import logging
import asyncio
from typing import List, Dict, Any, Tuple
from collections import Counter, defaultdict

from .logging_setup import logging_setup
from .config import Config

logger = logging.getLogger(__name__)


class TaskProcessor:
    """
    A class for processing tasks related to IP address data.

    Attributes:
        config (Config): The configuration object for the TaskProcessor.
        ip_counter (Counter): A Counter object to keep track of IP occurrences.
        ip_data (defaultdict): A defaultdict to store IP data.
        unique_ips (set): A set to store unique IP addresses.
        top_n (int): The number of top IPs to retrieve.
    """

    def __init__(self, config: Config):
        """
        Initializes the TaskProcessor with the given configuration.

        Args:
            config (Config): The configuration object for the TaskProcessor.
        """
        logger.debug("Initializing TaskProcessor")
        self.config = config
        self.ip_counter = Counter()
        self.ip_data = defaultdict(list)
        self.unique_ips = set()
        self.top_n = 5

        logger.debug("Setting up logging")
        try:
            logging_setup(
                self.config.log.log_level,
                self.config.log.log_file,
                self.config.log.error_log_file)
        except Exception as e:
            logger.error("Error in logging_setup: %s", str(e))
            raise

        self.logger = logging.getLogger(__name__)
        logger.debug("TaskProcessor initialized successfully")

    def _read_file(self) -> List[Tuple[str, int, int, int, int]]:
        """
        Reads IP addresses from a file and returns them as a list of tuples.

        Returns:
            List[Tuple[str, int, int, int, int]]: A list of tuples containing
                IP addresses and their corresponding octets.
        """
        try:
            with open(self.config.file.file_path, "r", encoding='utf-8') as f:
                data = []
                for line in f:
                    line = line.strip()
                    if line:
                        try:
                            ip = ipaddress.ip_address(line)
                            if isinstance(ip, ipaddress.IPv4Address):
                                octets = [int(octet)
                                          for octet in line.split('.')]
                                data.append((line, *octets))
                            else:
                                self.logger.warning(
                                    "Skipping non-IPv4 address: %s", line)
                        except ValueError as e:
                            self.logger.warning(
                                "Skipping invalid IP address: %s. Error: %s", line, e)
                return data
        except FileNotFoundError:
            self.logger.error("File not found: %s", self.config.file.file_path)
            raise
        except PermissionError:
            self.logger.error(
                "Read permission denied: %s",
                self.config.file.file_path)
            raise
        except Exception as e:
            self.logger.error(
                "Error reading file %s: %s",
                self.config.file.file_path,
                str(e))
            raise

    def process_ip_data(self, data: List[Any]) -> Dict[str, Any]:
        """
        Processes the IP data and returns the result.

        Args:
            data (List[Any]): The IP data to process.

        Returns:
            Dict[str, Any]: The result of the processing.
        """
        ip_id = str(data[0])
        self.ip_counter[ip_id] += 1
        self.unique_ips.add(ip_id)
        self.ip_data[ip_id].append(tuple(data[1:]))
        return {
            'ip_id': ip_id,
            'count': self.ip_counter[ip_id],
            'data': data,
            'data_summary': self.get_ip_data_summary(ip_id)
        }

    async def process_tasks(
            self, tasks: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Processes the tasks and returns the results.

        Args:
            tasks (List[Dict[str, Any]]): The tasks to process.

        Returns:
            List[Dict[str, Any]]: The results of the processing.
        """
        results = []
        for task in tasks:
            try:
                result = self.process_ip_data(task['data'])
                result['task_id'] = task['task_id']
                results.append(result)
            except Exception as e:
                self.logger.error(
                    "Error processing task %s: %s", task['task_id'], str(e))
        return results

    async def distribute_tasks(
            self,
            tasks: List[Dict[str, Any]],
            workers: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Distributes tasks among workers.

        Args:
            tasks (List[Dict[str, Any]]): The tasks to distribute.
            workers (List[Dict[str, Any]]): The workers to distribute tasks to.

        Returns:
            List[Dict[str, Any]]: The distributed tasks.
        """
        distributed_tasks = []
        for i, task in enumerate(tasks):
            worker = workers[i % len(workers)]
            distributed_tasks.append({
                'worker_id': worker['worker_id'],
                'host_id': worker['host_id'],
                'task': task
            })
        return distributed_tasks

    async def process_file(self):
        """
        Processes the file and returns the tasks.

        Returns:
        List[Dict[str, Any]]: The tasks to process.
        """
        ip_data = await asyncio.to_thread(self._read_file)
        tasks = [{'task_id': f'task_{i}', 'data': data}
                 for i, data in enumerate(ip_data)]
        return tasks

    def get_ip_stats(self) -> Dict[str, Any]:
        """
        Gets the IP statistics.

        Returns:
            Dict[str, Any]: The IP statistics.
        """
        total_ips = sum(self.ip_counter.values())
        unique_ips = len(self.unique_ips)
        return {
            "total_occurrences": total_ips,
            "unique_ips": unique_ips,
            "average_occurrences": total_ips /
            unique_ips if unique_ips > 0 else 0}

    def get_top_n_ips(self) -> List[Tuple[str, int]]:
        """
        Gets the top N (most redundant) IPs.

        Returns:
            List[Tuple[str, int]]: The top N IPs.
        """
        return self.ip_counter.most_common(self.top_n)

    def get_ip_data_summary(self, ip_id: str) -> Dict[str, Any]:
        """
        Gets the IP data summary.

        Args:
            ip_id (str): The IP address to get the data summary for.

        Returns:
            Dict[str, Any]: The IP data summary.
        """
        if ip_id not in self.ip_data:
            return {}

        data = self.ip_data[ip_id]
        return {
            "count": len(data),
            "avg_values": [sum(col) / len(data) for col in zip(*data)],
            "max_values": [max(col) for col in zip(*data)],
            "min_values": [min(col) for col in zip(*data)]
        }

    def get_overall_statistics(self) -> Dict[str, Any]:
        """
        Gets the overall statistics.

        Returns:
            Dict[str, Any]: The overall statistics.
        """
        return {
            "ip_statistics": self.get_ip_stats(),
            "top_n_ips": self.get_top_n_ips()
        }
