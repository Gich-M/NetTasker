import asyncio
import json
import logging
from .config import NODE_CONFIG
from .logging_setup import logging_setup
from .task_processor import TaskProcessor
from .utils import check_node_health

class NodeManager():
    def __init__(self):
        self.config = NODE_CONFIG
        self.nodes = {}
        self.task_queue = asyncio.PriorityQueue()
        self.task_assignments = {}
        self.lock = asyncio.Lock()
        self.running = True
        self.results = {}
        self.task_processor = TaskProcessor(self.config)

        logging_setup(
            self.config['log_level'],
            self.config['log_file'],
            self.config['error_log_file']
        )
        self.logger = logging.getLogger(__name__)
        self.logger.info("NodeManager initialized")

    async def run(self):
        self.logger.info("NodeManager started")
        while self.running:
            await self.check_nodes_health()
            await self.distribute_tasks()
            await self.monitor_performance()
            await asyncio.sleep(self.config['health_check_interval'])

    def stop(self):
        self.running = False
        self.logger.info("NodeManager stopping")

    async def add_node(self, node_id, ip_address):
        async with self.lock:
            if len(self.nodes) < self.config['max_nodes']:
                port = self.config['default_port']
                self.nodes[node_id] = {
                    'ip': ip_address,
                    'port': port,
                    'status': 'active',
                    'tasks': [],
                    'performance_score': 1
                }
                self.logger.info(
                    "Node added: %s at %s:%s", node_id, ip_address, port)
                return True
            self.logger.warning(
                "Failed to add node %s: maximum node limit reached", node_id)
            return False

    async def remove_node(self, node_id):
        async with self.lock:
            if node_id in self.nodes:
                removed_node = self.nodes.pop(node_id)
                await self.reassign_tasks(removed_node['tasks'])
                self.logger.info("Node removed: %s", node_id)
                return True
            self.logger.warning(
                "Failed to remove node %s: node not found", node_id)
            return False

    async def get_active_nodes(self):
        async with self.lock:
            active_nodes = {
                k: v for k,
                v in self.nodes.items() if v['status'] == 'active'}
            return active_nodes

    async def check_nodes_health(self):
        for node_id, node_info in self.nodes.items():
            try:
                if await self.check_node_health(
                        node_info['ip'], node_info['port']):
                    self.nodes[node_id]['status'] = 'active'
                    self.logger.debug("Node %s is active", node_id)
                else:
                    await self.handle_node_failure(node_id)
            except Exception as e:
                self.logger.error(
                    "Error checking health of node %s: %s", node_id, {str(e)})
                await self.handle_node_failure(node_id)

    async def handle_node_failure(self, node_id):
        async with self.lock:
            self.nodes[node_id]['status'] = 'inactive'
            tasks_to_reassign = self.nodes[node_id]['tasks']
            self.nodes[node_id]['tasks'] = []
            await self.reassign_tasks(tasks_to_reassign)
            self.logger.warning(
                "Node %s is inactive, reassigned %s tasks",
                node_id, len(tasks_to_reassign))

    async def add_task(self, task, priority=1):
        await self.task_queue.put((priority, task))

    async def distribute_tasks(self):
        active_nodes = await self.get_active_nodes()
        while not self.task_queue.empty() and active_nodes:
            _, task = await self.task_queue.get()
            node_id = await self.select_best_nodes(active_nodes)
            async with self.lock:
                self.nodes[node_id]['tasks'].append(task)
                self.task_assignments[task] = node_id
            self.logger.info("Task %s assigned to node %s", task, node_id)
            await self.send_task_to_node(node_id, task)

    async def select_best_nodes(self, active_nodes):
        best_nodes = min(
            active_nodes,
            key=lambda x: len(
                self.nodes[x]['tasks']) /
            self.nodes[x]['performance_score'])
        return best_nodes

    async def send_task_to_node(self, node_id, task):
        node_info = self.nodes[node_id]
        try:
            _, writer = await asyncio.open_connection(
                node_info['ip'], node_info['port'])
            message = json.dumps({
                "type": "TASK",
                "task_id": task.task_id,
                "ip_batch": [str(ip) for ip in task.ip_batch]
            })
            writer.write(message.encode())
            await writer.drain()
            writer.close()
            await writer.wait_closed()
            self.logger.info("Task %s sent to node %s", task, node_id)
        except Exception as e:
            self.logger.error(
                "Failed to send task to node %s: %s", node_id, {str(e)})
            await self.reassign_tasks(task.task_id)

    async def receive_result(self, node_id, task_id, result):
        async with self.lock:
            if task_id in self.task_assignments and \
                    self.task_assignments[task_id] == node_id:
                self.logger.info(
                    "Result received for task %s \
                        from node %s", task_id, node_id)
                self.nodes[node_id]['tasks'].remove(task_id)
                del self.task_assignments[task_id]
                self.update_node_performance(node_id, result)
                processed_results = await self.task_processor.process_tasks([result])
                for processed_result in processed_results:
                    self.logger.info("Processed result: %s", processed_result)
            else:
                self.logger.warning(
                    "Received result for unassigned task %s \
                        from node %s", task_id, node_id)

    async def reassign_tasks(self, tasks_to_reassign):
        for task in tasks_to_reassign:
            await self.add_task(task)
        self.logger.info("Reassigned %d tasks", len(tasks_to_reassign))

    async def monitor_performance(self):
        for node_id, node_info in self.nodes.items():
            if node_info['status'] == 'active':
                await self.check_node_load(node_id)

    async def check_node_load(self, node_id):
        node_info = self.nodes[node_id]
        current_load = len(node_info['tasks'])
        max_ts_per_node = self.config['max_tasks_per_node']
        if current_load > max_ts_per_node:
            tasks_to_reassign = node_info['tasks'][max_ts_per_node:]
            node_info['tasks'] = node_info['tasks'][:max_ts_per_node]
            await self.reassign_tasks(tasks_to_reassign)
            self.logger.info(
                "Node %s has reached its maximum load, \
                reassigned %s tasks", node_id, len(tasks_to_reassign))

    async def update_node_metrics(self, node_id, metrics):
        self.nodes[node_id].update(metrics)
        await self.check_node_load(node_id)
        self.logger.debug(
            "Updated metrics for node %s: %s", node_id, metrics)

    def update_node_performance(self, node_id, result):
        execution_time = result.get('execution_time', 1)
        self.nodes[node_id]['performance_score'] = 1 / \
            max(execution_time, 0.001)
        self.logger.debug(
            "Updated performance score for node %s: %.2f",
            node_id, self.nodes[node_id]['performance_score'])
