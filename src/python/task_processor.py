import logging
from typing import List, Dict, Set, Any, Tuple
from collections import Counter, defaultdict
from ipaddress import ip_address, IPv4Address
from src.python.config import NODE_CONFIG


class TaskProcessor:
    def __init__(self, config):
        self.config = NODE_CONFIG
        self.logger = logging.getLogger(__name__)
        self.ip_counter = Counter()
        self.ip_data = defaultdict(list)
        self.unique_ips = set()
        self.top_n = 5

    def _read_file(self) -> Set[Tuple[str, ...]]:
        file_path = self.config['file_path']
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                return {tuple(line.strip(';')) for line in f}
        except FileNotFoundError:
            self.logger.error("File not found: %s", file_path)
            raise
        except PermissionError:
            self.logger.error("Read permission denied: %s", file_path)
            raise
        except Exception as e:
            self.logger.error("Error reading file %s : %s", file_path, str(e))
            raise

    def get_ip_count(self, ip: str) -> int:
        return self.ip_counter[ip]

    def get_ip_version(self, ip: str) -> int:
        try:
            return "IPv4" if isinstance(
                ip_address(ip), IPv4Address) else "IPv6"
        except ValueError:
            self.logger.warning("Invalid IP address: %s", ip)
            return None

    def is_private_ip(self, ip: str) -> int:
        try:
            return ip_address(ip).is_private
        except ValueError:
            return False

    def get_top_n_ips(self) -> List[tuple]:
        return self.ip_counter.most_common(self.top_n)

    def get_ip_statics(self) -> Dict[str, Any]:
        totals_ips = sum(len(data) for data in self.ip_data.values())
        unique_ips = len(self.unique_ips)
        return {
            "total_occurences": totals_ips,
            "unique_ips": unique_ips,
            "average_occurences": totals_ips /
            unique_ips if unique_ips > 0 else 0}

    def get_ip_data_summary(self, ip: str) -> Dict[str, Any]:
        if ip not in self.ip_data:
            return {}

        data = self.ip_data[ip]
        return {
            "count": len(data),
            "avg_values": [sum(col) / len(data) for col in zip(*data)],
            "max_values": [max(col) for col in zip(*data)],
            "min_values": [min(col) for col in zip(*data)]
        }

    async def process_tasks(self, tasks: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        results = []
        for task in tasks:
            ip = task.get('ip')
            if ip:
                result = {
                    'task_id': task['id'],
                    'ip': ip,
                    'count': self.get_ip_count(ip),
                    'version': self.get_ip_version(ip),
                    'is_private': self.is_private_ip(ip),
                    'data_summary': self.get_ip_data_summary(ip)
                }
                results.append(result)
            else:
                self.logger.warning("Task %s has no IP address", task['id'])

        return results

    async def get_overall_statistics(self) -> Dict[str, Any]:
        return {
            "ip_statistics": self.get_ip_statics(),
            "top_n_ips": self.get_top_n_ips()
        }
    
if __name__ == "__main__":
    processor = TaskProcessor(NODE_CONFIG)