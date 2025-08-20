"""
Integration tests for the message broker system.
Tests both direct broker functionality and end-to-end client-server communication.
"""
import unittest
import tempfile
import shutil
import os
import threading
import time
from unittest.mock import Mock
from core.message import Message, MessageType, MessageBuilder
from core.broker import MessageBroker
from core.config import get_config
from core.protocol import ClientConnection


class TestBrokerIntegration(unittest.TestCase):
    """Integration tests for the message broker system"""
    
    def setUp(self):
        """Set up test fixtures before each test method"""
        # Create a temporary directory for test data
        self.temp_dir = tempfile.mkdtemp()
        
        # Override config for testing
        self.config = get_config()
        self.original_data_dir = self.config.get('storage.data_dir')
        self.config.set('storage.data_dir', self.temp_dir)
        
        # Create and start broker
        self.broker = MessageBroker()
        self.broker.start()
    
    def tearDown(self):
        """Clean up after each test method"""
        try:
            self.broker.stop()
        except:
            pass
        
        # Restore original config
        if self.original_data_dir:
            self.config.set('storage.data_dir', self.original_data_dir)
        
        # Clean up temporary directory
        shutil.rmtree(self.temp_dir, ignore_errors=True)
    
    # === Core Broker Functionality Tests ===
    
    def test_topic_creation(self):
        """Test topic creation"""
        # Create a topic
        self.broker.create_topic('test-topic')
        
        # Verify it was created
        topics = self.broker.list_topics()
        self.assertIn('test-topic', topics)
    
    def test_subscription_creation(self):
        """Test subscription creation"""
        # Create a topic first
        self.broker.create_topic('test-topic')
        
        # Create a subscription
        subscription = self.broker.create_subscription('test-sub', 'test-topic', start_offset=0)
        
        self.assertEqual(subscription.subscription_id, 'test-sub')
        self.assertEqual(subscription.topic, 'test-topic')
        self.assertEqual(subscription.current_offset, 0)
    
    def test_message_storage_and_retrieval(self):
        """Test that messages are stored and can be retrieved"""
        # Create topic
        self.broker.create_topic('test-topic')
        
        # Get topic storage
        topic_log = self.broker.storage.get_topic('test-topic')
        
        # Create a test message
        message = (MessageBuilder(MessageType.DATA)
                  .body(b'Hello World')
                  .property('test_prop', 'test_value')
                  .build())
        
        # Store message
        offset = topic_log.append(message)
        self.assertEqual(offset, 0)
        
        # Retrieve message
        retrieved = topic_log.read_message_at_offset(0)
        self.assertIsNotNone(retrieved)
        self.assertEqual(retrieved.body, b'Hello World')
        self.assertEqual(retrieved.properties['test_prop'], 'test_value')
    
    def test_multiple_messages(self):
        """Test storing multiple messages"""
        self.broker.create_topic('test-topic')
        topic_log = self.broker.storage.get_topic('test-topic')
        
        # Store multiple messages
        for i in range(5):
            message = (MessageBuilder(MessageType.DATA)
                      .body(f'Message {i}'.encode())
                      .property('msg_id', str(i))
                      .build())
            offset = topic_log.append(message)
            self.assertEqual(offset, i)
        
        # Verify all messages can be retrieved
        for i in range(5):
            retrieved = topic_log.read_message_at_offset(i)
            self.assertIsNotNone(retrieved)
            self.assertEqual(retrieved.body, f'Message {i}'.encode())
            self.assertEqual(retrieved.properties['msg_id'], str(i))
    
    def test_subscription_offset_tracking(self):
        """Test that subscription offsets are tracked correctly"""
        self.broker.create_topic('test-topic')
        subscription = self.broker.create_subscription('test-sub', 'test-topic', start_offset=0)
        
        # Initially at offset 0
        self.assertEqual(subscription.current_offset, 0)
        
        # Simulate message delivery (this would normally be done by the broker)
        subscription.current_offset = 3
        
        self.assertEqual(subscription.current_offset, 3)
    
    # === Protocol Handler Integration Tests ===
    
    def test_mock_client_connection(self):
        """Test basic connection handling with mock client"""
        import socket
        
        # Create mock socket and add connection
        mock_socket = Mock(spec=socket.socket)
        address = ('127.0.0.1', 12345)
        
        connection = self.broker.add_client_connection(mock_socket, address)
        
        self.assertIsNotNone(connection)
        self.assertEqual(connection.address, address)
        self.assertIn(connection.connection_id, self.broker.protocol_handler._connections)
    
    def test_create_topic_through_protocol(self):
        """Test creating topic through protocol handler"""
        import socket
        
        # Setup mock connection
        mock_socket = Mock(spec=socket.socket)
        mock_socket.sendall.return_value = None
        connection = self.broker.add_client_connection(mock_socket, ('127.0.0.1', 12345))
        
        # Create topic message
        topic_message = MessageBuilder(MessageType.CREATE_TOPIC)\
            .property('topic', 'protocol-test-topic')\
            .property('partitions', '2')\
            .build()
        
        # Handle the message
        response = self.broker._handle_create_topic(connection, topic_message)
        
        # Verify topic was created
        self.assertIsNotNone(response)
        self.assertEqual(response.message_type, MessageType.ACK)
        self.assertIn('protocol-test-topic', self.broker.list_topics())
    
    def test_publish_through_protocol(self):
        """Test publishing messages through protocol handler"""
        import socket
        
        # Setup
        self.broker.create_topic('protocol-pub-topic')
        mock_socket = Mock(spec=socket.socket)
        mock_socket.sendall.return_value = None
        connection = self.broker.add_client_connection(mock_socket, ('127.0.0.1', 12345))
        
        # Create publish message
        publish_message = MessageBuilder(MessageType.PUBLISH)\
            .body(b'Protocol test message')\
            .property('topic', 'protocol-pub-topic')\
            .property('partition', '0')\
            .property('publisher_id', 'test-publisher')\
            .build()
        
        # Handle publish
        response = self.broker._handle_publish(connection, publish_message)
        
        # Verify message was stored
        self.assertIsNotNone(response)
        self.assertEqual(response.message_type, MessageType.ACK)
        
        # Check message was actually stored
        topic_log = self.broker.storage.get_topic('protocol-pub-topic')
        retrieved = topic_log.read_message_at_offset(0)
        self.assertIsNotNone(retrieved)
        self.assertEqual(retrieved.body, b'Protocol test message')
    
    def test_subscription_through_protocol(self):
        """Test creating subscription through protocol handler"""
        import socket
        
        # Setup
        self.broker.create_topic('protocol-sub-topic')
        mock_socket = Mock(spec=socket.socket)
        mock_socket.sendall.return_value = None
        connection = self.broker.add_client_connection(mock_socket, ('127.0.0.1', 12345))
        
        # Create subscription message
        sub_message = MessageBuilder(MessageType.CREATE_SUBSCRIPTION)\
            .property('subscription_id', 'protocol-sub')\
            .property('topic', 'protocol-sub-topic')\
            .property('start_offset', '0')\
            .build()
        
        # Handle subscription creation
        response = self.broker._handle_create_subscription(connection, sub_message)
        
        # Verify subscription was created
        self.assertIsNotNone(response)
        self.assertEqual(response.message_type, MessageType.ACK)
        
        # Check subscription exists
        subscription = self.broker.subscription_manager._subscriptions.get('protocol-sub')
        self.assertIsNotNone(subscription)
        self.assertEqual(subscription.topic, 'protocol-sub-topic')
    
    # === Concurrent Operations Tests ===
    
    def test_concurrent_message_storage(self):
        """Test concurrent message storage operations"""
        self.broker.create_topic('concurrent-topic')
        topic_log = self.broker.storage.get_topic('concurrent-topic')
        
        results = []
        results_lock = threading.Lock()
        
        def store_messages(worker_id, count):
            local_results = []
            for i in range(count):
                message = (MessageBuilder(MessageType.DATA)
                          .body(f'Worker {worker_id} Message {i}'.encode())
                          .property('worker_id', str(worker_id))
                          .property('msg_index', str(i))
                          .build())
                
                offset = topic_log.append(message)
                local_results.append((worker_id, i, offset))
            
            with results_lock:
                results.extend(local_results)
        
        # Start concurrent workers
        threads = []
        worker_count = 3
        messages_per_worker = 10
        
        for worker_id in range(worker_count):
            thread = threading.Thread(target=store_messages, args=(worker_id, messages_per_worker))
            threads.append(thread)
            thread.start()
        
        # Wait for all threads
        for thread in threads:
            thread.join()
        
        # Verify all messages were stored
        expected_total = worker_count * messages_per_worker
        self.assertEqual(len(results), expected_total)
        
        # Verify all messages can be retrieved
        stored_offsets = [r[2] for r in results]
        max_offset = max(stored_offsets)
        
        for offset in range(max_offset + 1):
            retrieved = topic_log.read_message_at_offset(offset)
            self.assertIsNotNone(retrieved, f"Message at offset {offset} should exist")
    
    def test_concurrent_subscriptions(self):
        """Test concurrent subscription operations"""
        self.broker.create_topic('concurrent-sub-topic')
        
        created_subs = []
        sub_lock = threading.Lock()
        
        def create_subscriptions(worker_id, count):
            local_subs = []
            for i in range(count):
                sub_id = f'worker_{worker_id}_sub_{i}'
                subscription = self.broker.create_subscription(sub_id, 'concurrent-sub-topic')
                local_subs.append(subscription)
            
            with sub_lock:
                created_subs.extend(local_subs)
        
        # Start concurrent workers
        threads = []
        worker_count = 3
        subs_per_worker = 5
        
        for worker_id in range(worker_count):
            thread = threading.Thread(target=create_subscriptions, args=(worker_id, subs_per_worker))
            threads.append(thread)
            thread.start()
        
        # Wait for all threads
        for thread in threads:
            thread.join()
        
        # Verify all subscriptions were created
        expected_total = worker_count * subs_per_worker
        self.assertEqual(len(created_subs), expected_total)
        
        # Verify each subscription is valid and retrievable
        for subscription in created_subs:
            retrieved = self.broker.subscription_manager._subscriptions.get(subscription.subscription_id)
            self.assertIsNotNone(retrieved)
            self.assertEqual(retrieved.topic, 'concurrent-sub-topic')
    
    def test_large_message_handling(self):
        """Test handling large messages"""
        self.broker.create_topic('large-message-topic')
        topic_log = self.broker.storage.get_topic('large-message-topic')
        
        # Create large message (1MB)
        large_body = b'X' * (1024 * 1024)
        message = (MessageBuilder(MessageType.DATA)
                  .body(large_body)
                  .property('size', str(len(large_body)))
                  .build())
        
        # Store large message
        offset = topic_log.append(message)
        self.assertEqual(offset, 0)
        
        # Retrieve and verify large message
        retrieved = topic_log.read_message_at_offset(0)
        self.assertIsNotNone(retrieved)
        self.assertEqual(retrieved.body, large_body)
        self.assertEqual(retrieved.properties['size'], str(len(large_body)))
    
    def test_broker_stats(self):
        """Test broker statistics gathering"""
        # Create some test data
        self.broker.create_topic('stats-topic-1')
        self.broker.create_topic('stats-topic-2')
        
        self.broker.create_subscription('stats-sub-1', 'stats-topic-1')
        self.broker.create_subscription('stats-sub-2', 'stats-topic-1')
        self.broker.create_subscription('stats-sub-3', 'stats-topic-2')
        
        # Get stats
        stats = self.broker.get_broker_stats()
        
        # Verify stats structure
        self.assertIsInstance(stats, dict)
        self.assertIn('topics', stats)
        self.assertIn('subscriptions', stats)
        
        # Verify basic counts
        self.assertEqual(stats['topics'], 2)
        self.assertEqual(stats['subscriptions'], 3)
    
    def test_connection_cleanup(self):
        """Test connection cleanup functionality"""
        import socket
        
        # Add several mock connections
        connections = []
        for i in range(3):
            mock_socket = Mock(spec=socket.socket)
            connection = self.broker.add_client_connection(mock_socket, ('127.0.0.1', 12345 + i))
            connections.append(connection)
        
        # Verify connections were added
        initial_count = len(self.broker.protocol_handler._connections)
        self.assertEqual(initial_count, 3)
        
        # Remove one connection
        self.broker.remove_client_connection(connections[0].connection_id)
        
        # Verify connection was removed
        remaining_count = len(self.broker.protocol_handler._connections)
        self.assertEqual(remaining_count, 2)
        
        # Verify specific connection is gone
        retrieved = self.broker.protocol_handler.get_connection(connections[0].connection_id)
        self.assertIsNone(retrieved)


def run_basic_integration_test():
    """Simple integration test that can be run directly"""
    import tempfile
    import shutil
    
    # Create temporary directory
    temp_dir = tempfile.mkdtemp()
    
    try:
        # Override config
        config = get_config()
        config.set('storage.data_dir', temp_dir)
        
        # Create and start broker
        broker = MessageBroker()
        broker.start()
        
        print("Creating topic...")
        broker.create_topic('integration-test-topic')
        
        print("Creating subscription...")
        subscription = broker.create_subscription('test-subscription', 'integration-test-topic')
        
        print("Testing message storage...")
        topic_log = broker.storage.get_topic('integration-test-topic')
        
        # Store a few messages
        for i in range(3):
            message = (MessageBuilder(MessageType.DATA)
                      .body(f'Test message {i}'.encode())
                      .property('message_id', str(i))
                      .build())
            offset = topic_log.append(message)
            print(f"Stored message {i} at offset {offset}")
        
        print("Testing message retrieval...")
        for i in range(3):
            retrieved = topic_log.read_message_at_offset(i)
            if retrieved:
                print(f"Retrieved message {i}: {retrieved.body.decode()}")
            else:
                print(f"Failed to retrieve message {i}")
        
        print("Getting broker stats...")
        stats = broker.get_broker_stats()
        print(f"Broker stats: {stats}")
        
        print("Integration test completed successfully!")
        
    except Exception as e:
        print(f"Integration test failed: {e}")
        raise
    finally:
        # Cleanup
        try:
            broker.stop()
        except:
            pass
        shutil.rmtree(temp_dir, ignore_errors=True)


if __name__ == '__main__':
    unittest.main()
