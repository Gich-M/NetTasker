"""
This module provides configuration management for the distributed
    task processor. It defines various configuration classes and
    a main Config class to load and manage settings.
"""
import logging
from dataclasses import dataclass
from configparser import ConfigParser
from typing import List

logger = logging.getLogger(__name__)


@dataclass
class ProcessorConfig:
    """
    Configuration settings for the processor.
    """
    host: str
    port: int
    port_range_start: int
    port_range_end: int
    num_workers: int


@dataclass
class TaskConfig:
    """
    Configuration settings for tasks.
    """
    ip_batch_size: int
    max_tasks_per_worker: int
    health_check_interval: int


@dataclass
class FileConfig:
    """
    Configuration settings for file operations.
    """
    file_path: str


@dataclass
class LogConfig:
    """
    Configuration settings for logging.
    """
    log_level: str
    log_file: str
    error_log_file: str
    alert_log_file: str


@dataclass
class NginxConfig:
    """
    Configuration settings for Nginx.
    """
    address: str
    port: int
    enabled: bool


@dataclass
class PerformanceConfig:
    """
    Configuration settings for performance monitoring.
    """
    performance_update_interval: int
    min_performance_score: float


@dataclass
class ScalingConfig:
    """
    Configuration settings for auto-scaling.
    """
    auto_scale: bool
    scale_up_threshold: float
    scale_down_threshold: float
    scale_up_step: int
    scale_down_step: int
    min_workers: int


class Config:
    """
    Class to manage configuration settings for the distributed task processor.
    """

    def __init__(self, config_file: str = 'config/processor_config.ini'):
        """
        Initializes the Config with the provided configuration file.

        Args:
            config_file (str): The path to the configuration file. Defaults to
            'config/processor_config.ini'.
        """
        logger.debug("Initializing Config with file: %s", config_file)
        self.config_file: str = config_file
        self.config: ConfigParser = ConfigParser()
        self.reload_config()
        logger.debug("Config initialized successfully")

    def reload_config(self) -> None:
        """
        Reloads the configuration from the config file.

        It reads the config file, validates its contents, and loads
        the configuration for each component (processor, task, file, log,
        nginx, performance, and scaling).

        Raises:
            FileNotFoundError: If the config file is not found.
            ValueError: If the config file is
                missing required sections or fields.
        """
        logger.debug("Reloading config from file: %s", self.config_file)
        if not self.config.read(self.config_file):
            logger.error("Config file not found: %s", self.config_file)
            raise FileNotFoundError(
                f"Config file not found: {self.config_file}")
        self.validate_config()
        self.processor: ProcessorConfig = self._load_processor_config()
        self.task: TaskConfig = self._load_task_config()
        self.file: FileConfig = self._load_file_config()
        self.log: LogConfig = self._load_log_config()
        self.nginx: NginxConfig = self._load_nginx_config()
        self.performance: PerformanceConfig = self._load_performance_config()
        self.scaling: ScalingConfig = self._load_scaling_config()
        logger.debug("Config reloaded successfully")

    def validate_config(self) -> None:
        """
        Validates the configuration file.

        It checks if all required sections and fields are present
        in the configuration file.

        Raises:
            ValueError: If a required section or field is missing.
        """
        required_sections: List[str] = [
            'Processor',
            'Task',
            'File',
            'Log',
            'Nginx',
            'Performance',
            'Scaling']
        for section in required_sections:
            if section not in self.config:
                raise ValueError(f"Missing required section: {section}")

        required_fields: dict[str, List[str]] = {
            'Processor': [
                'host',
                'port',
                'port_range_start',
                'port_range_end',
                'num_workers'],
            'Task': [
                'ip_batch_size',
                'max_tasks_per_worker',
                'health_check_interval'],
            'File': ['file_path'],
            'Log': [
                'log_level',
                'log_file',
                'alert_log_file',
                'error_log_file'],
            'Nginx': [
                'address',
                'port',
                'enabled'],
            'Performance': [
                'performance_update_interval',
                'min_performance_score'],
            'Scaling': [
                'auto_scale',
                'scale_up_threshold',
                'scale_down_threshold',
                'scale_up_step',
                'scale_down_step',
                'min_workers']}

        for section, fields in required_fields.items():
            for field in fields:
                if not self.config.has_option(section, field):
                    raise ValueError(
                        f"Missing required field '{field}' in \
                            section '{section}'")

    def _load_processor_config(self) -> ProcessorConfig:
        """
        Loads the processor configuration.

        Return:
            ProcessorConfig: The loaded processor configuration.
        """
        return ProcessorConfig(
            host=self.config.get('Processor', 'host'),
            port=self.config.get('Processor', 'port'),
            port_range_start=self.config.getint(
                'Processor', 'port_range_start'),
            port_range_end=self.config.getint(
                'Processor', 'port_range_end'),
            num_workers=self.config.getint(
                'Processor', 'num_workers'))

    def _load_task_config(self) -> TaskConfig:
        """
        Loads the task configuration.

        Return:
            TaskConfig: The loaded task configuration.
        """
        return TaskConfig(
            ip_batch_size=self.config.getint('Task', 'ip_batch_size'),
            max_tasks_per_worker=self.config.getint(
                'Task',
                'max_tasks_per_worker'),
            health_check_interval=self.config.getint(
                'Task',
                'health_check_interval')
        )

    def _load_file_config(self) -> FileConfig:
        """
        Loads the file configuration.

        Return:
            FileConfig: The loaded file configuration.
        """
        return FileConfig(
            file_path=self.config.get('File', 'file_path')
        )

    def _load_log_config(self) -> LogConfig:
        """
        Loads the log configuration.

        Return:
            LogConfig: The loaded log configuration.
        """
        return LogConfig(
            log_level=self.config.get('Log', 'log_level'),
            log_file=self.config.get('Log', 'log_file'),
            alert_log_file=self.config.get('Log', 'alert_log_file'),
            error_log_file=self.config.get('Log', 'error_log_file')
        )

    def _load_nginx_config(self) -> NginxConfig:
        """
        Loads the Nginx configuration.

        Return:
            NginxConfig: The loaded Nginx configuration.
        """
        return NginxConfig(
            address=self.config.get('Nginx', 'address'),
            port=self.config.getint('Nginx', 'port'),
            enabled=self.config.getboolean('Nginx', 'enabled')
        )

    def _load_performance_config(self) -> PerformanceConfig:
        """
        Loads the performance configuration.

        Return:
            PerformanceConfig: The loaded performance configuration.
        """
        return PerformanceConfig(
            performance_update_interval=self.config.getint(
                'Performance',
                'performance_update_interval'),
            min_performance_score=self.config.getfloat(
                'Performance',
                'min_performance_score'))

    def _load_scaling_config(self) -> ScalingConfig:
        """
        Loads the scaling configuration.

        Return:
            ScalingConfig: The loaded scaling configuration.
        """
        return ScalingConfig(
            auto_scale=self.config.getboolean(
                'Scaling', 'auto_scale'),
            scale_up_threshold=self.config.getfloat(
                'Scaling', 'scale_up_threshold'),
            scale_down_threshold=self.config.getfloat(
                'Scaling', 'scale_down_threshold'),
            scale_up_step=self.config.getint(
                'Scaling', 'scale_up_step'),
            scale_down_step=self.config.getint(
                'Scaling',
                'scale_down_step'),
            min_workers=self.config.getint(
                'Scaling',
                'min_workers'))

    def get_worker_ports(self) -> List[int]:
        """
        Returns the list of worker ports.

        Return:
            List[int]: A list of ports for workers.
        """
        return list(
            range(
                self.processor.port_range_start,
                self.processor.port_range_end +
                1))

    @classmethod
    def load_from_file(cls, config_file: str) -> 'Config':
        """
        Loads the configuration from a file.

        Args:
            config_file: The  path to the configuration file.

        Return:
            Config: A new Config instance loaded from the specified file.
        """
        logger.debug("Loading Config from file: %s", config_file)
        return cls(config_file)
