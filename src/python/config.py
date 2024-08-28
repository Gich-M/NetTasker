"""
Configuration settings for nodes in the network.

Attributes:
    max_nodes (int): The maximum number of nodes allowed in the network.
    default_port (int): The default port number for communication.
    health_check_interval (int): The interval in seconds for health checks between nodes.
"""
NODE_CONFIG = {
    'log_level': 'INFO',
    'log_file': 'logs/nmanager.log',
    'error_log_file': 'logs/nmanager.error.log',
    'max_nodes': 100,
    'default_port': 8080,
    'ip_batch_size': 50,
    'max_tasks_per_node': 10,
    'health_check_interval': 10,
    'file_path': 'data/200k.txt'
}
