import asyncio
import json
import logging
from typing import Any, Dict, List
from .config import Config
from .logging_setup import logging_setup
from .task_processor import TaskProcessor
from .utils import check_node_health, send_alert, calculate_node_score

class NodeManager:
    def __init__(self, config: Config):
        self.logger = logging.getLogger(__name__)
        self.logger.info("Initializing NodeManager")
        
        self.config = config
        self.logger.debug("Config loaded: %s", self.config.__dict__)
        
        self.nodes = {}
        self.task_queue = asyncio.PriorityQueue()
        self.task_assignments = {}
        self.lock = asyncio.Lock()
        self.running = True
        self.results = {}
        
        self.logger.info("Initializing TaskProcessor")
        self.task_processor = TaskProcessor(self.config)
        
        self.logger.info("Setting up logging")
        logging_setup(
            self.config.log.log_level,
            self.config.log.log_file,
            self.config.log.error_log_file
        )
        
        # Set up a separate logger for alerts
        self.alert_logger = logging.getLogger('alerts')
        alert_handler = logging.FileHandler('logs/alerts.log')
        alert_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
        self.alert_logger.addHandler(alert_handler)
        self.alert_logger.setLevel(logging.WARNING)
        
        # Nginx configuration
        self.nginx_address = self.config.get('nginx', 'address')
        self.nginx_port = self.config.get('nginx', 'port')
        
        self.logger.info("NodeManager initialized successfully")

    async def run(self):
        self.logger.info("NodeManager started")
        try:
            while self.running:
                self.logger.debug("Starting main loop iteration")
                await self.check_nodes_health()
                await self.distribute_tasks()
                await self.monitor_performance()
                await asyncio.sleep(self.config.task.health_check_interval)
        except Exception as e:
            self.logger.error("Error in main loop: %s", str(e), exc_info=True)
        finally:
            self.logger.info("NodeManager main loop ended")

    def stop(self):
        self.logger.info("Stopping NodeManager")
        self.running = False

    async def add_node(self, node_id: str, ip_address: str):
        self.logger.info("Attempting to add node: %s at %s", node_id, ip_address)
        async with self.lock:
            if len(self.nodes) < self.config.get('nodes', 'max_nodes'):
                port = self.config.get('nodes', 'default_port')
                self.nodes[node_id] = {
                    'ip': ip_address,
                    'port': port,
                    'status': 'active',
                    'tasks': [],
                    'performance_score': 1
                }
                self.logger.info("Node added successfully: %s at %s:%s", node_id, ip_address, port)
                return True
            self.logger.warning("Failed to add node %s: maximum node limit reached", node_id)
            return False

    async def remove_node(self, node_id: str):
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
                k: v for k, v in self.nodes.items() if v['status'] == 'active'}
            return active_nodes

    async def check_nodes_health(self):
        for node_id, node_info in self.nodes.items():
            try:
                node_url = f"http://{node_info['ip']}:{node_info['port']}"
                if await check_node_health(node_url):
                    node_info['status'] = 'active'
                    self.logger.debug("Node %s is active", node_id)
                else:
                    await self.handle_node_failure(node_id)
            except Exception as e:
                self.logger.error(
                    "Error checking health of node %s: %s", node_id, str(e))
                await self.handle_node_failure(node_id)

    async def handle_node_failure(self, node_id: str):
        async with self.lock:
            node_info = self.nodes[node_id]
            node_info['status'] = 'inactive'
            tasks_to_reassign = node_info['tasks']
            node_info['tasks'] = []
            await self.reassign_tasks(tasks_to_reassign)
            self.logger.warning(
                "Node %s is inactive, reassigned %s tasks",
                node_id, len(tasks_to_reassign))

    async def add_task(self, task, priority=1):
        await self.task_queue.put((priority, task))

    async def distribute_tasks(self):
        self.logger.debug("Starting task distribution")
        active_nodes = await self.get_active_nodes()
        self.logger.debug(f"Active nodes: {list(active_nodes.keys())}")
        self.logger.debug(f"Tasks in queue: {self.task_queue.qsize()}")
        
        while not self.task_queue.empty() and active_nodes:
            _, task = await self.task_queue.get()
            self.logger.debug(f"Processing task: {task['task_id']}")
            
            # Instead of selecting a specific node, we'll send tasks to Nginx
            await self.send_task_to_nginx(task)
            
            self.logger.info("Task %s sent to Nginx for distribution", task['task_id'])
        
        self.logger.debug("Task distribution completed")

    async def send_task_to_nginx(self, task: Dict[str, Any]):
        self.logger.debug(f"Attempting to send task {task['task_id']} to Nginx")
        try:
            _, writer = await asyncio.open_connection(self.nginx_address, self.nginx_port)
            message = json.dumps({
                "type": "TASK",
                "task_id": task['task_id'],
                "ip_batch": [str(ip) for ip in task['ip_batch']]
            })
            self.logger.debug(f"Sending message to Nginx: {message}")
            writer.write(message.encode())
            await writer.drain()
            writer.close()
            await writer.wait_closed()
            self.logger.info("Task %s sent to Nginx", task['task_id'])
        except Exception as e:
            self.logger.error(
                "Failed to send task to Nginx: %s", str(e), exc_info=True)
            self.logger.debug(f"Re-queueing task {task['task_id']} due to send failure")
            await self.add_task(task)

    async def receive_result(self, node_id: str, task_id: str, result):
        self.logger.debug(f"Received result for task {task_id} from node {node_id}")
        async with self.lock:
            if node_id in self.nodes:
                self.logger.info(
                    "Result received for task %s from node %s",
                    task_id,
                    node_id)
                self.nodes[node_id]['tasks'] = [
                    t for t in self.nodes[node_id]['tasks'] if t['task_id'] != task_id]
                self.update_node_performance(node_id, result)
                self.logger.debug(f"Processing result for task {task_id}")
                processed_results = await self.task_processor.process_tasks([result])
                for processed_result in processed_results:
                    self.logger.info("Processed result: %s", processed_result)
            else:
                self.logger.warning(
                    "Received result from unknown node %s for task %s",
                    node_id,
                    task_id)

    async def reassign_tasks(self, tasks_to_reassign: List[Dict[str, Any]]):
        for task in tasks_to_reassign:
            await self.add_task(task)
        self.logger.info("Reassigned %d tasks", len(tasks_to_reassign))

    async def monitor_performance(self):
        # This method might need to be adjusted based on how you want to monitor performance with Nginx
        pass

    async def update_node_metrics(self, node_id: str, metrics):
        if node_id in self.nodes:
            self.nodes[node_id].update(metrics)
            self.logger.debug(
                "Updated metrics for node %s: %s", node_id, metrics)
        else:
            self.logger.warning(
                "Received metrics for unknown node %s", node_id)

    def update_node_performance(self, node_id: str, result):
        if node_id in self.nodes:
            execution_time = result.get('execution_time', 1)
            self.nodes[node_id]['performance_score'] = calculate_node_score({
                'speed': 1 / max(execution_time, 0.001),
                'reliability': result.get('reliability', 1),
                'uptime': result.get('uptime', 1)
            })
            self.logger.debug(
                "Updated performance score for node %s: %.2f",
                node_id, self.nodes[node_id]['performance_score'])
        else:
            self.logger.warning(
                "Received performance update for unknown node %s", node_id)

    async def check_node_status(self, node_id: str):
        self.logger.info("Checking status of node: %s", node_id)
        if node_id in self.nodes:
            node = self.nodes[node_id]
            node_url = f"http://{node['ip']}:{node['port']}"
            try:
                is_healthy = await check_node_health(node_url)
                if not is_healthy:
                    alert_message = f"Node {node_id} is unhealthy"
                    self.logger.warning(alert_message)
                    self.alert_logger.warning(alert_message)
                else:
                    self.logger.info("Node %s is healthy", node_id)
            except Exception as e:
                error_message = f"Error checking node {node_id} status: {str(e)}"
                self.logger.error(error_message, exc_info=True)
                self.alert_logger.error(error_message)
        else:
            self.logger.warning("Attempted to check status of unknown node %s", node_id)

if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logger = logging.getLogger(__name__)
    logger.info("Starting NodeManager script")
    
    try:
        logger.info("Initializing Config")
        config = Config("config/processor_config.ini")
        logger.info("Config initialized successfully")
        
        logger.info("Creating NodeManager instance")
        node_manager = NodeManager(config)
        logger.info("NodeManager instance created successfully")
        
        logger.info("Starting NodeManager run loop")
        asyncio.run(node_manager.run())
    except Exception as e:
        logger.error("Error in main execution: %s", str(e), exc_info=True)
    finally:
        logger.info("NodeManager script finished")