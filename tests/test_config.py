"""
Unit tests for the configuration module.
Tests configuration loading, validation, and environment variable support.
"""
import unittest
import os
import tempfile
import json
from core.config import get_config
import core.config
import core.config


class MockEnvironment:
    """Mock environment variables without external libraries"""
    
    def __init__(self):
        self.original_getenv = os.getenv
        self.mock_vars = {}
    
    def set(self, key, value):
        """Set a mock environment variable"""
        self.mock_vars[key] = value
    
    def clear(self):
        """Clear all mock variables"""
        self.mock_vars.clear()
    
    def mock_getenv(self, key, default=None):
        """Mock implementation of os.getenv"""
        return self.mock_vars.get(key, default)
    
    def start_mocking(self):
        """Start mocking os.getenv"""
        os.getenv = self.mock_getenv
    
    def stop_mocking(self):
        """Stop mocking os.getenv"""
        os.getenv = self.original_getenv


class TestConfig(unittest.TestCase):
    """Test cases for configuration management"""
    
    def setUp(self):
        """Set up test fixtures"""
        # Reset the singleton instance before each test
        core.config._config_instance = None
        
        # Set up environment variable mocking
        self.mock_env = MockEnvironment()
        self.mock_env.start_mocking()
    
    def tearDown(self):
        """Clean up after tests"""
        # Stop mocking environment variables
        self.mock_env.stop_mocking()
        
        # Reset singleton instance
        core.config._config_instance = None
    
    def test_default_config(self):
        """Test default configuration values"""
        config = get_config()
        
        # Test server defaults
        self.assertEqual(config.get('server.host'), '127.0.0.1')
        self.assertEqual(config.get('server.port'), 9999)
        self.assertEqual(config.get('server.max_connections'), 100)
        self.assertEqual(config.get('server.heartbeat_interval'), 30)
        self.assertEqual(config.get('server.connection_timeout'), 300)
        
        # Test storage defaults
        self.assertEqual(config.get('storage.data_dir'), './storage/data')
        self.assertEqual(config.get('storage.snapshot_interval'), 60)
        
        # Test logging defaults
        self.assertEqual(config.get('logging.level'), 'INFO')
    
    def test_config_file_loading(self):
        """Test loading configuration from file"""
        test_config = {
            "server": {
                "host": "0.0.0.0",
                "port": 8888,
                "max_connections": 500
            },
            "storage": {
                "data_dir": "/tmp/test-storage"
            }
        }
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump(test_config, f)
            config_file = f.name
        
        try:
            # Set the config file via environment variable
            self.mock_env.set('QB_CONFIG_FILE', config_file)

            config = get_config()
            
            # Test overridden values
            self.assertEqual(config.get('server.host'), '0.0.0.0')
            self.assertEqual(config.get('server.port'), 8888)
            self.assertEqual(config.get('server.max_connections'), 500)
            self.assertEqual(config.get('storage.data_dir'), '/tmp/test-storage')
            
            # Test that defaults are preserved for non-overridden values
            self.assertEqual(config.get('server.heartbeat_interval'), 30)
        finally:
            os.unlink(config_file)
    
    def test_environment_variable_override(self):
        """Test environment variable overrides"""
        # Set mock environment variables
        self.mock_env.set('QB_SERVER_HOST', '192.168.1.100')
        self.mock_env.set('QB_SERVER_PORT', '7777')
        self.mock_env.set('QB_DATA_DIR', '/custom/path')
        
        config = get_config()
        
        self.assertEqual(config.get('server.host'), '192.168.1.100')
        self.assertEqual(config.get('server.port'), 7777)
        self.assertEqual(config.get('storage.data_dir'), '/custom/path')
    
    def test_environment_variable_type_conversion(self):
        """Test proper type conversion for environment variables"""
        self.mock_env.set('QB_SERVER_PORT', '8080')
        self.mock_env.set('QB_MAX_CONNECTIONS', '1500')
        
        config = get_config()
        
        self.assertIsInstance(config.get('server.port'), int)
        self.assertEqual(config.get('server.port'), 8080)
        self.assertIsInstance(config.get('server.max_connections'), int)
        self.assertEqual(config.get('server.max_connections'), 1500)
    
    def test_invalid_config_file(self):
        """Test handling of invalid config file"""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            f.write("invalid json content {")
            config_file = f.name
        
        try:
            # Set the invalid config file via environment variable
            self.mock_env.set('QB_CONFIG_FILE', config_file)
            
            # Suppress expected warning from invalid JSON
            with self.assertLogs('root', level='WARNING') as log:
                # Should fall back to defaults without crashing
                config = get_config()
                self.assertEqual(config.get('server.port'), 9999)  # Default value
                
            # Verify the warning was logged
            self.assertTrue(any('Failed to load config file' in message for message in log.output))
        finally:
            os.unlink(config_file)
    
    def test_nonexistent_config_file(self):
        """Test handling of non-existent config file"""
        self.mock_env.set('QB_CONFIG_FILE', "/path/that/does/not/exist.json")
        config = get_config()
        self.assertEqual(config.get('server.port'), 9999)  # Default value
    
    def test_partial_config_file(self):
        """Test config file with partial configuration"""
        partial_config = {
            "server": {
                "port": 9090
            }
        }
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump(partial_config, f)
            config_file = f.name
        
        try:
            # Set the config file via environment variable
            self.mock_env.set('QB_CONFIG_FILE', config_file)
            config = get_config()
            
            # Test overridden value
            self.assertEqual(config.get('server.port'), 9090)
            
            # Test that other defaults are preserved
            self.assertEqual(config.get('server.host'), '127.0.0.1')
            self.assertEqual(config.get('storage.data_dir'), './storage/data')
        finally:
            os.unlink(config_file)
    
    def test_config_get_method(self):
        """Test the get method with dot notation"""
        config = get_config()
        
        # Test getting nested values
        self.assertEqual(config.get('server.host'), '127.0.0.1')
        self.assertEqual(config.get('server.port'), 9999)
        self.assertEqual(config.get('storage.data_dir'), './storage/data')
        
        # Test default values
        self.assertEqual(config.get('nonexistent.key', 'default'), 'default')
        self.assertIsNone(config.get('nonexistent.key'))
    
    def test_config_get_method_with_override(self):
        """Test get method with environment variable override"""
        self.mock_env.set('QB_SERVER_HOST', 'localhost')
        
        config = get_config()
        self.assertEqual(config.get('server.host'), 'localhost')


if __name__ == '__main__':
    unittest.main()
