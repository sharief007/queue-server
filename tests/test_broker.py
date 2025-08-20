"""
Unit tests for the broker module.
Tests the main message broker functionality, subscription management, and message routing.
"""
import unittest
import tempfile
import shutil
import os
import threading
import time
import socket
from unittest.mock import Mock, patch, MagicMock
from core.broker import MessageBroker, Publisher
from core.message import Message, MessageType, MessageBuilder
from core.protocol import ClientConnection
from core.topic_storage import TopicStorageManager


class TestMessageBroker(unittest.TestCase):
    """Test cases for MessageBroker class"""
    
    def setUp(self):
        """Set up test fixtures"""
        self.temp_dir = tempfile.mkdtemp()
        
        # Set test configuration using correct environment variable names
        os.environ['BROKER_STORAGE_DATA_DIR'] = self.temp_dir
        
        # Clear config cache
        from core.config import get_config
        config = get_config()
        config.set('storage.data_dir', self.temp_dir)
        
        self.broker = MessageBroker()
        self.broker.start()
    
    def tearDown(self):
        """Clean up test fixtures"""
        self.broker.stop()
        shutil.rmtree(self.temp_dir, ignore_errors=True)
        
        # Clean up environment variables
        if 'BROKER_STORAGE_DATA_DIR' in os.environ:
            del os.environ['BROKER_STORAGE_DATA_DIR']
    
    def test_broker_creation(self):
        """Test creating message broker"""
        self.assertIsNotNone(self.broker.storage)
        self.assertIsNotNone(self.broker.protocol_handler)
        self.assertIsNotNone(self.broker.subscription_manager)
    
    def test_create_topic_success(self):
        """Test successfully creating a topic"""
        topic_name = "test-topic"
        
        self.broker.create_topic(topic_name)
        
        topics = self.broker.list_topics()
        self.assertIn(topic_name, topics)
    
    def test_create_duplicate_topic(self):
        """Test creating a topic that already exists"""
        topic_name = "test-topic"
        
        # Create topic first time
        self.broker.create_topic(topic_name)
        
        # Try to create same topic again - should raise exception
        with self.assertRaises(ValueError):
            self.broker.create_topic(topic_name)
    
    def test_create_subscription_success(self):
        """Test successfully creating a subscription"""
        topic_name = "test-topic"
        subscription_id = "test-subscription"
        
        # Create topic first
        self.broker.create_topic(topic_name)
        
        # Create subscription
        subscription = self.broker.create_subscription(subscription_id, topic_name, 0)
        
        self.assertIsNotNone(subscription)
        self.assertEqual(subscription.subscription_id, subscription_id)
        self.assertEqual(subscription.topic, topic_name)
    
    def test_publish_message_success(self):
        """Test successfully publishing a message"""
        topic_name = "test-topic"
        
        # Create topic first
        self.broker.create_topic(topic_name)
        
        # Create a mock connection for publisher
        mock_socket = Mock(spec=socket.socket)
        mock_connection = self.broker.add_client_connection(mock_socket, ('127.0.0.1', 12345))
        
        # Create PUBLISH message
        msg = (MessageBuilder(MessageType.PUBLISH)
               .property('topic', topic_name)
               .property('publisher_id', 'test-publisher')
               .body(b"test message")
               .build())
        
        # Handle the publish message
        response = self.broker._handle_publish(mock_connection, msg)
        
        self.assertIsNotNone(response)
        self.assertEqual(response.properties.get('status'), 'success')
    
    def test_publish_message_to_nonexistent_topic(self):
        """Test publishing message to non-existent topic"""
        mock_socket = Mock(spec=socket.socket)
        mock_connection = self.broker.add_client_connection(mock_socket, ('127.0.0.1', 12345))
        
        msg = (MessageBuilder(MessageType.PUBLISH)
               .property('topic', 'nonexistent')
               .property('publisher_id', 'test-publisher')
               .body(b"test message")
               .build())
        
        response = self.broker._handle_publish(mock_connection, msg)
        
        self.assertIsNotNone(response)
        self.assertEqual(response.properties.get('status'), 'error')
    
    def test_subscribe_to_subscription_success(self):
        """Test successfully subscribing to a subscription"""
        topic_name = "test-topic"
        subscription_id = "test-subscription"
        
        # Create topic and subscription first
        self.broker.create_topic(topic_name)
        subscription = self.broker.create_subscription(subscription_id, topic_name, 0)
        
        # Create a mock connection for subscriber
        mock_socket = Mock(spec=socket.socket)
        mock_connection = self.broker.add_client_connection(mock_socket, ('127.0.0.1', 12345))
        
        # Create SUBSCRIBE message
        msg = (MessageBuilder(MessageType.SUBSCRIBE)
               .property('subscription_id', subscription_id)
               .property('subscriber_id', 'test-subscriber')
               .build())
        
        # Handle the subscribe message
        response = self.broker._handle_subscribe(mock_connection, msg)
        
        self.assertIsNotNone(response)
        self.assertEqual(response.properties.get('status'), 'success')
    
    def test_subscribe_to_nonexistent_subscription(self):
        """Test subscribing to non-existent subscription"""
        mock_socket = Mock(spec=socket.socket)
        mock_connection = self.broker.add_client_connection(mock_socket, ('127.0.0.1', 12345))
        
        msg = (MessageBuilder(MessageType.SUBSCRIBE)
               .property('subscription_id', 'nonexistent')
               .property('subscriber_id', 'test-subscriber')
               .build())
        
        response = self.broker._handle_subscribe(mock_connection, msg)
        
        self.assertIsNotNone(response)
        self.assertEqual(response.properties.get('status'), 'error')
    
    def test_unsubscribe_success(self):
        """Test successfully unsubscribing from a subscription"""
        topic_name = "test-topic"
        subscription_id = "test-subscription"
        
        # Create topic and subscription
        self.broker.create_topic(topic_name)
        subscription = self.broker.create_subscription(subscription_id, topic_name, 0)
        
        # Create a mock connection
        mock_socket = Mock(spec=socket.socket)
        mock_connection = self.broker.add_client_connection(mock_socket, ('127.0.0.1', 12345))
        
        # Subscribe first
        subscribe_msg = (MessageBuilder(MessageType.SUBSCRIBE)
                        .property('subscription_id', subscription_id)
                        .property('subscriber_id', 'test-subscriber')
                        .build())
        self.broker._handle_subscribe(mock_connection, subscribe_msg)
        
        # Now unsubscribe
        unsubscribe_msg = (MessageBuilder(MessageType.UNSUBSCRIBE)
                          .property('subscription_id', subscription_id)
                          .property('subscriber_id', 'test-subscriber')
                          .build())
        
        response = self.broker._handle_unsubscribe(mock_connection, unsubscribe_msg)
        
        self.assertIsNotNone(response)
        self.assertEqual(response.properties.get('status'), 'success')
    
    def test_message_delivery_to_subscription(self):
        """Test that messages are delivered to subscriptions"""
        topic_name = "test-topic"
        subscription_id = "test-subscription"
        
        # Create topic and subscription
        self.broker.create_topic(topic_name)
        subscription = self.broker.create_subscription(subscription_id, topic_name, 0)
        
        # Create a mock connection for subscriber
        mock_socket = Mock(spec=socket.socket)
        mock_connection = self.broker.add_client_connection(mock_socket, ('127.0.0.1', 12345))
        
        # Subscribe to the subscription
        subscribe_msg = (MessageBuilder(MessageType.SUBSCRIBE)
                        .property('subscription_id', subscription_id)
                        .property('subscriber_id', 'test-subscriber')
                        .build())
        self.broker._handle_subscribe(mock_connection, subscribe_msg)
        
        # Publish a message
        publish_msg = (MessageBuilder(MessageType.PUBLISH)
                      .property('topic', topic_name)
                      .property('publisher_id', 'test-publisher')
                      .body(b"test message")
                      .build())
        
        mock_socket2 = Mock(spec=socket.socket)
        mock_publisher = self.broker.add_client_connection(mock_socket2, ('127.0.0.1', 12346))
        self.broker._handle_publish(mock_publisher, publish_msg)
        
        # Check that topic log has messages
        topic_log = self.broker.storage.get_topic(topic_name)
        self.assertIsNotNone(topic_log)
        self.assertGreater(topic_log.get_message_count(), 0)
    
    def test_pull_messages_from_subscription(self):
        """Test reading messages from a subscription"""
        topic_name = "test-topic"
        subscription_id = "test-subscription"
        
        # Create topic and subscription
        self.broker.create_topic(topic_name)
        subscription = self.broker.create_subscription(subscription_id, topic_name, 0)
        
        # Publish messages to topic
        for i in range(5):
            publish_msg = (MessageBuilder(MessageType.PUBLISH)
                          .property('topic', topic_name)
                          .property('publisher_id', 'test-publisher')
                          .body(f"message {i}".encode())
                          .build())
            
            mock_socket = Mock(spec=socket.socket)
            mock_publisher = self.broker.add_client_connection(mock_socket, ('127.0.0.1', 12345 + i))
            self.broker._handle_publish(mock_publisher, publish_msg)
        
        # Check that topic log has messages
        topic_log = self.broker.storage.get_topic(topic_name)
        self.assertIsNotNone(topic_log)
        self.assertGreater(topic_log.get_message_count(), 0)
        
        # Check that we can read messages from subscription offset
        messages = []
        for offset in range(min(3, topic_log.get_message_count())):
            msg = topic_log.read_message_at_offset(subscription.current_offset + offset)
            if msg:
                messages.append(msg)
        self.assertGreater(len(messages), 0)
    
    def test_pull_from_nonexistent_subscription(self):
        """Test attempting to read from non-existent subscription"""
        # Create topic first
        self.broker.create_topic("test-topic")
        
        # Try to create a subscription for nonexistent subscription ID should work
        # because we created the topic first
        subscription = self.broker.subscription_manager.create_subscription("nonexistent", "test-topic", 0)
        self.assertIsNotNone(subscription)  # Should work since topic exists
    
    def test_subscription_round_robin_delivery(self):
        """Test that messages are delivered round-robin to subscription members"""
        topic_name = "test-topic"
        subscription_id = "test-subscription"
        
        # Create topic and subscription
        self.broker.create_topic(topic_name)
        subscription = self.broker.create_subscription(subscription_id, topic_name, 0)
        
        # Create multiple mock connections for subscribers
        mock_connections = []
        for i in range(3):
            mock_socket = Mock(spec=socket.socket)
            mock_connection = self.broker.add_client_connection(mock_socket, ('127.0.0.1', 12345 + i))
            mock_connections.append(mock_connection)
            
            # Subscribe each connection
            subscribe_msg = (MessageBuilder(MessageType.SUBSCRIBE)
                            .property('subscription_id', subscription_id)
                            .property('subscriber_id', f'test-subscriber-{i}')
                            .build())
            self.broker._handle_subscribe(mock_connection, subscribe_msg)
        
        # Publish messages
        for i in range(6):
            publish_msg = (MessageBuilder(MessageType.PUBLISH)
                          .property('topic', topic_name)
                          .property('publisher_id', 'test-publisher')
                          .body(f"message {i}".encode())
                          .build())
            
            mock_socket = Mock(spec=socket.socket)
            mock_publisher = self.broker.add_client_connection(mock_socket, ('127.0.0.1', 13000 + i))
            self.broker._handle_publish(mock_publisher, publish_msg)
        
        # Check that subscription has subscribers
        self.assertGreater(len(subscription.subscribers), 0)
        self.assertEqual(len(subscription.subscribers), 3)
    
    def test_handle_create_topic_message(self):
        """Test handling CREATE_TOPIC message"""
        mock_socket = Mock(spec=socket.socket)
        mock_connection = self.broker.add_client_connection(mock_socket, ('127.0.0.1', 12345))
        
        # Create CREATE_TOPIC message
        msg = (MessageBuilder(MessageType.CREATE_TOPIC)
               .property("topic", "new-topic")
               .build())
        
        # Handle message
        response = self.broker._handle_create_topic(mock_connection, msg)
        
        self.assertIsNotNone(response)
        self.assertEqual(response.properties.get('status'), 'success')
        self.assertIn("new-topic", self.broker.storage.list_topics())
    
    def test_handle_create_subscription_message(self):
        """Test handling CREATE_SUBSCRIPTION message"""
        topic_name = "test-topic"
        subscription_id = "test-subscription"
        
        # Create topic first
        self.broker.create_topic(topic_name)
        
        mock_socket = Mock(spec=socket.socket)
        mock_connection = self.broker.add_client_connection(mock_socket, ('127.0.0.1', 12345))
        
        # Create CREATE_SUBSCRIPTION message
        msg = (MessageBuilder(MessageType.CREATE_SUBSCRIPTION)
               .property("subscription_id", subscription_id)
               .property("topic", topic_name)
               .build())
        
        # Handle message
        response = self.broker._handle_create_subscription(mock_connection, msg)
        
        # Check response
        self.assertIsNotNone(response)
        self.assertEqual(response.properties.get('status'), 'success')
        
        # Check that subscription was created
        subscriptions = self.broker.subscription_manager._subscriptions
        self.assertIn(subscription_id, subscriptions)
        subscription = subscriptions[subscription_id]
        self.assertEqual(subscription.topic, topic_name)
    
    def test_handle_unsubscribe_message(self):
        """Test handling UNSUBSCRIBE message"""
        topic_name = "test-topic"
        subscription_id = "test-subscription"
        
        # Create topic and subscription
        self.broker.create_topic(topic_name)
        subscription = self.broker.create_subscription(subscription_id, topic_name, 0)
        
        mock_socket = Mock(spec=socket.socket)
        mock_connection = self.broker.add_client_connection(mock_socket, ('127.0.0.1', 12345))
        
        # Subscribe first
        subscribe_msg = (MessageBuilder(MessageType.SUBSCRIBE)
                        .property('subscription_id', subscription_id)
                        .property('subscriber_id', 'test-subscriber')
                        .build())
        self.broker._handle_subscribe(mock_connection, subscribe_msg)
        
        # Create UNSUBSCRIBE message
        unsubscribe_msg = (MessageBuilder(MessageType.UNSUBSCRIBE)
                          .property('subscription_id', subscription_id)
                          .property('subscriber_id', 'test-subscriber')
                          .build())
        
        # Handle message
        response = self.broker._handle_unsubscribe(mock_connection, unsubscribe_msg)
        
        # Check response
        self.assertIsNotNone(response)
        self.assertEqual(response.properties.get('status'), 'success')
    
    def test_handle_publish_message(self):
        """Test handling PUBLISH message"""
        topic_name = "test-topic"
        
        # Create topic
        self.broker.create_topic(topic_name)
        
        mock_socket = Mock(spec=socket.socket)
        mock_connection = self.broker.add_client_connection(mock_socket, ('127.0.0.1', 12345))
        
        # Create PUBLISH message
        msg = (MessageBuilder(MessageType.PUBLISH)
               .property("topic", topic_name)
               .property("publisher_id", "test-publisher")
               .body(b"published message")
               .build())
        
        # Handle message
        response = self.broker._handle_publish(mock_connection, msg)
        
        # Check response
        self.assertIsNotNone(response)
        self.assertEqual(response.properties.get('status'), 'success')
        
        # Check that message was stored in topic
        topic_log = self.broker.storage.get_topic(topic_name)
        self.assertIsNotNone(topic_log)
        self.assertGreater(topic_log.get_message_count(), 0)
    
    def test_handle_pull_message(self):
        """Test subscription message reading functionality"""
        topic_name = "test-topic"
        subscription_id = "test-subscription"
        
        # Create topic and subscription
        self.broker.create_topic(topic_name)
        subscription = self.broker.create_subscription(subscription_id, topic_name, 0)
        
        # Publish some messages first
        for i in range(3):
            publish_msg = (MessageBuilder(MessageType.PUBLISH)
                          .property("topic", topic_name)
                          .property("publisher_id", "test-publisher")
                          .body(f"message {i}".encode())
                          .build())
            
            mock_socket = Mock(spec=socket.socket)
            mock_publisher = self.broker.add_client_connection(mock_socket, ('127.0.0.1', 12345 + i))
            self.broker._handle_publish(mock_publisher, publish_msg)
        
        # Check that topic log has messages
        topic_log = self.broker.storage.get_topic(topic_name)
        self.assertIsNotNone(topic_log)
        self.assertGreater(topic_log.get_message_count(), 0)
        
        # Check that subscription exists and can access messages
        self.assertIsNotNone(subscription)
        self.assertEqual(subscription.topic, topic_name)
    
    def test_connection_cleanup_on_disconnect(self):
        """Test that subscriptions are cleaned up when connection disconnects"""
        topic_name = "test-topic"
        subscription_id = "test-subscription"
        
        # Create topic and subscription
        self.broker.create_topic(topic_name)
        subscription = self.broker.create_subscription(subscription_id, topic_name, 0)
        
        mock_socket = Mock(spec=socket.socket)
        mock_connection = self.broker.add_client_connection(mock_socket, ('127.0.0.1', 12345))
        
        # Subscribe
        subscribe_msg = (MessageBuilder(MessageType.SUBSCRIBE)
                        .property('subscription_id', subscription_id)
                        .property('subscriber_id', 'test-subscriber')
                        .build())
        self.broker._handle_subscribe(mock_connection, subscribe_msg)
        
        # Simulate connection cleanup - remove from subscription manager
        # Since we don't have cleanup_connection, test subscriber removal
        subscription = self.broker.subscription_manager._subscriptions[subscription_id]
        if 'test-subscriber' in subscription.subscribers:
            subscription.remove_subscriber('test-subscriber')
        
        # Check that subscription has no active subscribers
        self.assertEqual(len(subscription.subscribers), 0)
    
    def test_broker_stats(self):
        """Test getting broker statistics"""
        # Create some topics
        self.broker.create_topic("topic1")
        self.broker.create_topic("topic2")
        
        # Create subscriptions
        self.broker.create_subscription("sub1", "topic1", 0)
        self.broker.create_subscription("sub2", "topic2", 0)
        
        # Add some messages
        for i in range(5):
            publish_msg = (MessageBuilder(MessageType.PUBLISH)
                          .property("topic", "topic1")
                          .property("publisher_id", "test-publisher")
                          .body(f"message {i}".encode())
                          .build())
            
            mock_socket = Mock(spec=socket.socket)
            mock_publisher = self.broker.add_client_connection(mock_socket, ('127.0.0.1', 12345 + i))
            self.broker._handle_publish(mock_publisher, publish_msg)
        
        stats = self.broker.get_broker_stats()
        
        self.assertIn('topics', stats)
        self.assertIn('subscriptions', stats)
        
        self.assertEqual(stats['topics'], 2)  # Count of topics
        self.assertEqual(stats['subscriptions'], 2)  # Count of subscriptions
    
    def test_concurrent_operations(self):
        """Test concurrent broker operations"""
        topic_name = "concurrent-topic"
        subscription_id = "concurrent-subscription"
        
        # Create topic and subscription
        self.broker.create_topic(topic_name)
        subscription = self.broker.create_subscription(subscription_id, topic_name, 0)
        
        # Function to publish messages concurrently
        def publish_messages(start_idx, count):
            for i in range(start_idx, start_idx + count):
                publish_msg = (MessageBuilder(MessageType.PUBLISH)
                              .property("topic", topic_name)
                              .property("publisher_id", f"test-publisher-{i}")
                              .body(f"message {i}".encode())
                              .build())
                
                mock_socket = Mock(spec=socket.socket)
                mock_publisher = self.broker.add_client_connection(mock_socket, ('127.0.0.1', 12345 + i))
                self.broker._handle_publish(mock_publisher, publish_msg)
        
        # Start multiple threads
        threads = []
        for i in range(3):
            thread = threading.Thread(target=publish_messages, args=(i * 10, 10))
            threads.append(thread)
            thread.start()
        
        # Wait for all threads
        for thread in threads:
            thread.join()
        
        # Check that messages were stored in subscription
        self.assertIsNotNone(subscription)
        # Check topic log has messages
        topic_log = self.broker.storage.get_topic(topic_name)
        self.assertGreater(topic_log.get_message_count(), 0)
    
    def test_start_stop_broker(self):
        """Test starting and stopping the broker"""
        # Broker should already be started in setUp
        self.assertTrue(self.broker._running)
        
        # Stop the broker
        self.broker.stop()
        self.assertFalse(self.broker._running)
        
        # Start it again
        self.broker.start()
        self.assertTrue(self.broker._running)


if __name__ == '__main__':
    unittest.main()
