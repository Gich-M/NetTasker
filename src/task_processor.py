"""Module for processing tasks related to IP address data."""

import asyncio
import ipaddress
import json
import logging
from collections import Counter, defaultdict
from typing import Any, Dict, List, Tuple

from src.utils.config import Config
from src.utils.logging_setup import logging_setup

logger = logging.getLogger(__name__)


class TaskProcessor:
    """
    A class for processing tasks related to IP address data.

    Attributes:
        config: The configuration object for the TaskProcessor.
        ip_counter: A Counter object to keep track of IP occurrences.
        ip_data: A defaultdict to store IP data.
        unique_ips: A set to store unique IP addresses.
        top_n: The number of top IPs to retrieve.
        lock: A lock for thread-safe operations.
        logger: Logger for this class.
    """

    def __init__(self, config: Config):
        """
        Initializes the TaskProcessor with the given configuration.

        Args:
            config: The configuration object for the TaskProcessor.
        """
        logger.debug("Initializing TaskProcessor")
        self.config = config
        self.ip_counter = Counter()
        self.ip_data = defaultdict(list)
        self.unique_ips = set()
        self.top_n = 5
        self.lock = asyncio.Lock()

        logger.debug("Setting up logging")
        try:
            logging_setup(
                self.config.log.log_level,
                self.config.log.log_file,
                self.config.log.alert_log_file,
                self.config.log.error_log_file
            )
        except Exception as e:
            logger.error("Error in logging_setup: %s", str(e))
            raise

        self.logger = logging.getLogger(__name__)
        logger.debug("TaskProcessor initialized successfully")

    async def process_tasks(self, tasks: List[Dict[str, Any]]) -> str:
        """
        Processes the tasks and returns the result.

        Args:
            tasks: The tasks to process.

        Return:
            str: The result of the processing.
        """
        results = []
        for task in tasks:
            try:
                task_type = task.get('task_type', 'ip_data')
                ip = task['task']

                result = await self._process_task_by_type(task_type, ip)
                result['task_id'] = task['task_id']
                results.append(result)
            except Exception as e:
                self.logger.error(
                    "Error processing task %s: %s",
                    task['task_id'],
                    str(e))
                results.append({
                    'task_id': task['task_id'],
                    'error': str(e)
                })

        return json.dumps(results)

    async def _process_task_by_type(
            self,
            task_type: str,
            ip: str) -> Dict[str, Any]:
        """
        Process a task based on its type.

        Args:
            task_type: The type of task to process.
            ip: The IP address to process.

        Return:
            Dict: The result of the processing.

        Raises:
            ValueError: If the task type is unknown.
        """
        task_processors = {
            'ip_data': self.process_ip_data,
            'port_scan': self.perform_port_scan,
            'os_fingerprint': self.perform_os_fingerprint,
            'traceroute': self.perform_traceroute,
            'dns_enum': self.perform_dns_enum,
            'whois': self.perform_whois_lookup,
            'subnet': self.calculate_subnet,
            'reverse_dns': self.perform_reverse_dns,
            'banner_grab': self.perform_banner_grab
        }

        processor = task_processors.get(task_type)
        if not processor:
            raise ValueError(f"Unknown task type: {task_type}")

        return await processor(ip)

    async def process_ip_data(self, ip: str) -> Dict[str, Any]:
        """
        Processes the IP data and returns the result.

        Args:
            ip: The IP address to process.

        Return:
            Dict: The result of the processing.
        """
        async with self.lock:
            self.ip_counter[ip] += 1
            self.unique_ips.add(ip)
            self.ip_data[ip].append(tuple(map(int, ip.split('.'))))
        return {
            'ip_id': ip,
            'count': self.ip_counter[ip],
            'data_summary': self.get_ip_data_summary(ip)
        }

    def _read_file(self) -> List[Tuple[str, int, int, int, int]]:
        """
        Reads IP addresses from a file and returns them as a list of tuples.

        Return:
            List: A list of tuples containing
                IP addresses and their corresponding octets.

        Raises:
            FileNotFoundError: If the specified file is not found.
            PermissionError: If there's no read permission for the file.
            Exception: For any other error that occurs during file reading.
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
                                "Skipping invalid IP address: %s. Error: %s",
                                line, e)
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

    def get_ip_stats(self) -> Dict[str, Any]:
        """
        Gets the IP statistics.

        Return:
            Dict: The IP statistics.
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

        Return:
            List: The top N IPs.
        """
        return self.ip_counter.most_common(self.top_n)

    def get_ip_data_summary(self, ip_id: str) -> Dict[str, Any]:
        """
        Gets the IP data summary.

        Args:
            ip_id: The IP address to get the data summary for.

        Return:
            Dict: The IP data summary.
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

        Return:
            Dict: The overall statistics.
        """
        return {
            "ip_statistics": self.get_ip_stats(),
            "top_n_ips": self.get_top_n_ips()
        }

    async def perform_port_scan(self, ip: str) -> Dict[str, Any]:
        """
        Performs a port scan on the given IP address.

        Args:
            ip: The IP address to scan.

        Return:
            Dict: The result of the port scan.
        """
        cmd = f"nmap -p- -T4 {ip}"
        result = await self._run_command(cmd)
        return {"ip": ip, "port_scan": result}

    async def perform_os_fingerprint(self, ip: str) -> Dict[str, Any]:
        """
        Performs OS fingerprinting on the given IP address.

        Args:
            ip: The IP address to fingerprint.

        Return:
            Dict: The result of the OS fingerprinting.
        """
        cmd = f"nmap -O {ip}"
        result = await self._run_command(cmd)
        return {"ip": ip, "os_fingerprint": result}

    async def perform_traceroute(self, ip: str) -> Dict[str, Any]:
        """
        Performs a traceroute to the given IP address.

        Args:
            ip: The IP address to traceroute to.

        Return:
            Dict: The result of the traceroute.
        """
        cmd = f"traceroute {ip}"
        result = await self._run_command(cmd)
        return {"ip": ip, "traceroute": result}

    async def perform_dns_enum(self, ip: str) -> Dict[str, Any]:
        """
        Performs DNS enumeration on the given IP address.

        Args:
            ip: The IP address to enumerate.

        Return:
            Dict[str, Any]: The result of the DNS enumeration.
        """
        cmd = f"dnsenum {ip}"
        result = await self._run_command(cmd)
        return {"ip": ip, "dns_enum": result}

    async def perform_whois_lookup(self, ip: str) -> Dict[str, Any]:
        """
        Performs a WHOIS lookup on the given IP address.

        Args:
            ip: The IP address to look up.

        Return:
            Dict: The result of the WHOIS lookup.
        """
        cmd = f"whois {ip}"
        result = await self._run_command(cmd)
        return {"ip": ip, "whois": result}

    async def calculate_subnet(self, ip: str) -> Dict[str, Any]:
        """
        Calculates subnet information for the given IP address.

        Args:
            ip: The IP address to calculate subnet for.

        Return:
            Dict: The subnet information.
        """
        network = ipaddress.IPv4Network(ip, strict=False)
        return {
            "ip": ip,
            "network": str(network.network_address),
            "broadcast": str(network.broadcast_address),
            "netmask": str(network.netmask),
            "num_addresses": network.num_addresses
        }

    async def perform_reverse_dns(self, ip: str) -> Dict[str, Any]:
        """
        Performs a reverse DNS lookup on the given IP address.

        Args:
            ip: The IP address to look up.

        Return:
            Dict: The result of the reverse DNS lookup.
        """
        cmd = f"host {ip}"
        result = await self._run_command(cmd)
        return {"ip": ip, "reverse_dns": result}

    async def perform_banner_grab(self, ip: str) -> Dict[str, Any]:
        """
        Performs a banner grab on the given IP address.

        Args:
            ip: The IP address to perform banner grab on.

        Return:
            Dict: The result of the banner grab.
        """
        cmd = f"nc -v -n -z -w 1 {ip} 1-1000"
        result = await self._run_command(cmd)
        return {"ip": ip, "banner_grab": result}

    async def _run_command(self, cmd: str) -> str:
        """
        Runs a shell command asynchronously.

        Args:
            cmd: The command to run.

        Return:
            str: The output of the command.
        """
        process = await asyncio.create_subprocess_shell(
            cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        stdout, stderr = await process.communicate()
        if stderr:
            self.logger.warning(
                "Command '%s' produced stderr: %s",
                cmd,
                stderr.decode())
        return stdout.decode()
