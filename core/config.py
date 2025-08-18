"""
Configuration management supporting both file-based and environment variable configurations.
"""
import os
import json
import logging
from typing import Any, Dict, Optional
from pathlib import Path


class Config:
    """Configuration manager with defaults and environment variable support"""
    
    # Default configuration values
    DEFAULTS = {
        # Server settings
        'server': {
            'host': '127.0.0.1',
            'port': 9999,
            'max_connections': 100,
            'connection_timeout': 300,  # 5 minutes
            'heartbeat_interval': 30,   # seconds
        },
        
        # Storage settings
        'storage': {
            'data_dir': './storage/data',
            'snapshot_dir': './storage/snapshots',
            'snapshot_interval': 60,    # seconds
            'max_log_size': 1024 * 1024 * 100,  # 100MB
            'sync_interval': 1,         # seconds
        },
        
        # Topic settings
        'topics': {
            'default_partitions': 1,
            'max_partitions': 16,
            'retention_hours': 24,
            'max_message_size': 1024 * 1024,  # 1MB
        },
        
        # Client settings
        'client': {
            'max_retries': 3,
            'retry_delay': 1,           # seconds
            'request_timeout': 30,      # seconds
            'auto_ack': True,
        },
        
        # Logging
        'logging': {
            'level': 'INFO',
            'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            'file': None,  # None means console only
        }
    }
    
    def __init__(self, config_file: Optional[str] = None):
        self._config = self._deep_copy(self.DEFAULTS)
        
        # Load from file if provided
        if config_file and os.path.exists(config_file):
            self._load_from_file(config_file)
        
        # Override with environment variables
        self._load_from_env()
        
        # Ensure directories exist
        self._ensure_directories()
    
    def get(self, key_path: str, default: Any = None) -> Any:
        """
        Get configuration value using dot notation.
        Example: config.get('server.port') returns the port value
        """
        keys = key_path.split('.')
        value = self._config
        
        try:
            for key in keys:
                value = value[key]
            return value
        except (KeyError, TypeError):
            return default
    
    def set(self, key_path: str, value: Any) -> None:
        """Set configuration value using dot notation"""
        keys = key_path.split('.')
        config = self._config
        
        for key in keys[:-1]:
            if key not in config:
                config[key] = {}
            config = config[key]
        
        config[keys[-1]] = value
    
    def _load_from_file(self, config_file: str) -> None:
        """Load configuration from JSON file"""
        try:
            with open(config_file, 'r') as f:
                file_config = json.load(f)
            self._merge_config(self._config, file_config)
        except Exception as e:
            logging.warning(f"Failed to load config file {config_file}: {e}")
    
    def _load_from_env(self) -> None:
        """Load configuration from environment variables"""
        env_mappings = {
            'QB_SERVER_HOST': 'server.host',
            'QB_SERVER_PORT': 'server.port',
            'QB_DATA_DIR': 'storage.data_dir',
            'QB_SNAPSHOT_DIR': 'storage.snapshot_dir',
            'QB_SNAPSHOT_INTERVAL': 'storage.snapshot_interval',
            'QB_LOG_LEVEL': 'logging.level',
            'QB_MAX_CONNECTIONS': 'server.max_connections',
            'QB_DEFAULT_PARTITIONS': 'topics.default_partitions',
            'QB_MAX_RETRIES': 'client.max_retries',
        }
        
        for env_var, config_key in env_mappings.items():
            env_value = os.getenv(env_var)
            if env_value is not None:
                # Try to convert to appropriate type
                value = self._convert_env_value(env_value)
                self.set(config_key, value)
    
    def _convert_env_value(self, value: str) -> Any:
        """Convert environment variable string to appropriate type"""
        # Try integer
        try:
            return int(value)
        except ValueError:
            pass
        
        # Try float
        try:
            return float(value)
        except ValueError:
            pass
        
        # Try boolean
        if value.lower() in ('true', '1', 'yes', 'on'):
            return True
        elif value.lower() in ('false', '0', 'no', 'off'):
            return False
        
        # Return as string
        return value
    
    def _merge_config(self, base: Dict, override: Dict) -> None:
        """Recursively merge configuration dictionaries"""
        for key, value in override.items():
            if key in base and isinstance(base[key], dict) and isinstance(value, dict):
                self._merge_config(base[key], value)
            else:
                base[key] = value
    
    def _deep_copy(self, obj: Any) -> Any:
        """Deep copy configuration (avoiding external dependencies)"""
        if isinstance(obj, dict):
            return {k: self._deep_copy(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [self._deep_copy(item) for item in obj]
        else:
            return obj
    
    def _ensure_directories(self) -> None:
        """Ensure required directories exist"""
        dirs_to_create = [
            self.get('storage.data_dir'),
            self.get('storage.snapshot_dir'),
        ]
        
        for dir_path in dirs_to_create:
            if dir_path:
                Path(dir_path).mkdir(parents=True, exist_ok=True)
    
    def save_to_file(self, config_file: str) -> None:
        """Save current configuration to file"""
        try:
            with open(config_file, 'w') as f:
                json.dump(self._config, f, indent=2)
        except Exception as e:
            logging.error(f"Failed to save config to {config_file}: {e}")
    
    def __str__(self) -> str:
        return json.dumps(self._config, indent=2)


# Global configuration instance
_config_instance: Optional[Config] = None

def get_config() -> Config:
    """Get global configuration instance"""
    global _config_instance
    if _config_instance is None:
        config_file = os.getenv('QB_CONFIG_FILE', 'config/broker.json')
        _config_instance = Config(config_file)
    return _config_instance

def initialize_config(config_file: Optional[str] = None) -> Config:
    """Initialize global configuration"""
    global _config_instance
    _config_instance = Config(config_file)
    return _config_instance
