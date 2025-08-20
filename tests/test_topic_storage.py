"""
Unit tests for the topic storage module.
Tests topic logs, topic storage manager, and message persistence functionality.
"""
import unittest
import tempfile
import shutil
import os
import time
from pathlib import Path
from core.topic_storage import TopicLog, TopicStorageManager, MessageOffset
from core.message import Message, MessageType, MessageBuilder
from core.config import get_config


def create_test_message(body: bytes, topic: str = "test-topic") -> Message:
    """Helper function to create test messages"""
    return MessageBuilder(MessageType.DATA).body(body).property("topic", topic).build()


class TestTopicLog(unittest.TestCase):
    """Test cases for TopicLog class"""
    
    def setUp(self):
        """Set up test fixtures"""
        self.temp_dir = tempfile.mkdtemp()
        self.topic = "test-topic"
    
    def tearDown(self):
        """Clean up test fixtures"""
        # Force garbage collection to close any open file handles
        import gc
        gc.collect()
        shutil.rmtree(self.temp_dir, ignore_errors=True)
    
    def test_topic_log_creation(self):
        """Test creating a topic log"""
        log = TopicLog(self.topic, self.temp_dir)
        try:
            self.assertEqual(log.topic, self.topic)
            self.assertEqual(log.get_next_offset(), 0)
            self.assertEqual(log.get_message_count(), 0)
        finally:
            log.close()
    
    def test_append_single_message(self):
        """Test appending a single message"""
        log = TopicLog(self.topic, self.temp_dir)
        try:
            message = create_test_message(b"test message", self.topic)
            offset = log.append(message)
            
            self.assertEqual(offset, 0)
            self.assertEqual(log.get_next_offset(), 1)
            self.assertEqual(log.get_message_count(), 1)
        finally:
            log.close()
        self.assertEqual(message.sequence_number, 0)
    
    def test_append_multiple_messages(self):
        """Test appending multiple messages"""
        log = TopicLog(self.topic, self.temp_dir)
        try:
            messages = []
            for i in range(10):
                msg = create_test_message(f"test message {i}".encode(), self.topic)
                offset = log.append(msg)
                messages.append(msg)
                self.assertEqual(offset, i)
            
            self.assertEqual(log.get_next_offset(), 10)
            self.assertEqual(log.get_message_count(), 10)
        finally:
            log.close()
    
    def test_read_single_message(self):
        """Test reading a single message by offset"""
        log = TopicLog(self.topic, self.temp_dir)
        try:
            # Append test message
            original_msg = create_test_message(b"test message", self.topic)
            offset = log.append(original_msg)
            
            # Read the message back
            read_msg = log.read_message_at_offset(offset)
            
            self.assertIsNotNone(read_msg)
            self.assertEqual(read_msg.body, original_msg.body)
            self.assertEqual(read_msg.sequence_number, offset)
        finally:
            log.close()
    
    def test_read_nonexistent_message(self):
        """Test reading message at nonexistent offset"""
        log = TopicLog(self.topic, self.temp_dir)
        try:
            # Try to read from empty log
            read_msg = log.read_message_at_offset(0)
            self.assertIsNone(read_msg)
            
            # Add one message
            log.append(create_test_message(b"test", self.topic))
            
            # Try to read beyond available
            read_msg = log.read_message_at_offset(5)
            self.assertIsNone(read_msg)
        finally:
            log.close()
    
    def test_persistence_to_disk(self):
        """Test that messages are persisted to disk"""
        log = TopicLog(self.topic, self.temp_dir)
        try:
            # Append a message
            msg = create_test_message(b"persistent message", self.topic)
            log.append(msg)
            
            # Check that log and index files exist
            log_file = Path(self.temp_dir) / f"{self.topic}.log"
            index_file = Path(self.temp_dir) / f"{self.topic}.idx"
            
            self.assertTrue(log_file.exists())
            self.assertTrue(index_file.exists())
            self.assertGreater(log_file.stat().st_size, 0)
            self.assertGreater(index_file.stat().st_size, 0)
        finally:
            log.close()
    
    def test_reload_from_disk(self):
        """Test reloading messages from disk"""
        # Create first log and add messages
        log1 = TopicLog(self.topic, self.temp_dir)
        
        original_messages = []
        for i in range(5):
            msg = create_test_message(f"message {i}".encode(), self.topic)
            log1.append(msg)
            original_messages.append(msg)
        
        log1.close()
        
        # Create second log from same directory (should reload from disk)
        log2 = TopicLog(self.topic, self.temp_dir)
        
        self.assertEqual(log2.get_message_count(), 5)
        self.assertEqual(log2.get_next_offset(), 5)
        
        # Read messages from second log
        for i in range(5):
            read_msg = log2.read_message_at_offset(i)
            self.assertIsNotNone(read_msg)
            self.assertEqual(read_msg.body, original_messages[i].body)
        
        log2.close()


class TestTopicStorageManager(unittest.TestCase):
    """Test cases for TopicStorageManager class"""
    
    def setUp(self):
        """Set up test fixtures"""
        self.temp_dir = tempfile.mkdtemp()
        
        # Set test configuration
        os.environ['QB_DATA_DIR'] = self.temp_dir
        
        # Clear config cache to pick up new environment
        import core.config
        core.config._config_instance = None
    
    def tearDown(self):
        """Clean up test fixtures"""
        # Force garbage collection to close any open file handles
        import gc
        gc.collect()
        shutil.rmtree(self.temp_dir, ignore_errors=True)
        
        # Clean up environment
        if 'QB_DATA_DIR' in os.environ:
            del os.environ['QB_DATA_DIR']
        
        # Clear config cache
        import core.config
        core.config._config_instance = None
    
    def test_storage_manager_creation(self):
        """Test creating storage manager"""
        manager = TopicStorageManager()
        try:
            self.assertIsNotNone(manager)
            self.assertEqual(len(manager.list_topics()), 0)
        finally:
            manager.close_all()
    
    def test_create_topic(self):
        """Test creating a topic"""
        manager = TopicStorageManager()
        try:
            topic_name = "test-topic"
            manager.create_topic(topic_name)
            
            topics = manager.list_topics()
            self.assertIn(topic_name, topics)
            
            # Verify topic log was created
            topic_log = manager.get_topic(topic_name)
            self.assertIsNotNone(topic_log)
            self.assertEqual(topic_log.topic, topic_name)
        finally:
            manager.close_all()
    
    def test_create_duplicate_topic(self):
        """Test creating a topic that already exists"""
        manager = TopicStorageManager()
        try:
            topic_name = "test-topic"
            manager.create_topic(topic_name)
            
            # Try to create same topic again - should raise exception
            with self.assertRaises(ValueError):
                manager.create_topic(topic_name)
        finally:
            manager.close_all()
    
    def test_delete_topic(self):
        """Test deleting a topic"""
        manager = TopicStorageManager()
        
        topic_name = "test-topic"
        manager.create_topic(topic_name)
        self.assertIn(topic_name, manager.list_topics())
        
        # Delete topic
        manager.delete_topic(topic_name)
        self.assertNotIn(topic_name, manager.list_topics())
    
    def test_delete_nonexistent_topic(self):
        """Test deleting a topic that doesn't exist"""
        manager = TopicStorageManager()
        
        # Should raise exception
        with self.assertRaises(ValueError):
            manager.delete_topic("nonexistent-topic")
    
    def test_get_topic(self):
        """Test getting a topic log"""
        manager = TopicStorageManager()
        try:
            topic_name = "test-topic"
            manager.create_topic(topic_name)
            
            topic_log = manager.get_topic(topic_name)
            self.assertIsNotNone(topic_log)
            self.assertEqual(topic_log.topic, topic_name)
        finally:
            manager.close_all()
    
    def test_get_nonexistent_topic(self):
        """Test getting a nonexistent topic"""
        manager = TopicStorageManager()
        
        # Should raise exception
        with self.assertRaises(ValueError):
            manager.get_topic("nonexistent-topic")
    
    def test_append_message(self):
        """Test appending a message to a topic"""
        manager = TopicStorageManager()
        try:
            topic_name = "test-topic"
            manager.create_topic(topic_name)
            
            msg = create_test_message(b"test message", topic_name)
            offset = manager.append_message(topic_name, msg)
            
            self.assertIsInstance(offset, MessageOffset)
            self.assertEqual(offset.topic, topic_name)
            self.assertEqual(offset.offset, 0)
        finally:
            manager.close_all()
    
    def test_append_message_to_nonexistent_topic(self):
        """Test appending message to nonexistent topic"""
        manager = TopicStorageManager()
        
        msg = create_test_message(b"test message", "nonexistent")
        
        # Should raise exception
        with self.assertRaises(ValueError):
            manager.append_message("nonexistent", msg)
    
    def test_list_topics(self):
        """Test listing all topics"""
        manager = TopicStorageManager()
        try:
            # Initially empty
            self.assertEqual(len(manager.list_topics()), 0)
            
            # Create some topics
            topics = ["topic1", "topic2", "topic3"]
            for topic in topics:
                manager.create_topic(topic)
            
            listed_topics = manager.list_topics()
            self.assertEqual(len(listed_topics), 3)
            for topic in topics:
                self.assertIn(topic, listed_topics)
        finally:
            manager.close_all()
    
    def test_close_all(self):
        """Test closing all topic logs"""
        manager = TopicStorageManager()
        
        # Create some topics
        topics = ["topic1", "topic2"]
        for topic in topics:
            manager.create_topic(topic)
        
        # Should not raise exception
        manager.close_all()
        
        # After close_all, no topics should be available
        self.assertEqual(len(manager.list_topics()), 0)


class TestMessageOffset(unittest.TestCase):
    """Test cases for MessageOffset class"""
    
    def test_message_offset_creation(self):
        """Test creating a message offset"""
        offset = MessageOffset("test-topic", 42)
        
        self.assertEqual(offset.topic, "test-topic")
        self.assertEqual(offset.offset, 42)
    
    def test_message_offset_equality(self):
        """Test message offset equality"""
        offset1 = MessageOffset("topic", 10)
        offset2 = MessageOffset("topic", 10)
        offset3 = MessageOffset("topic", 20)
        offset4 = MessageOffset("other", 10)
        
        self.assertEqual(offset1, offset2)
        self.assertNotEqual(offset1, offset3)
        self.assertNotEqual(offset1, offset4)
    
    def test_message_offset_string_representation(self):
        """Test message offset string representation"""
        offset = MessageOffset("test-topic", 42)
        str_repr = str(offset)
        
        self.assertIn("test-topic", str_repr)
        self.assertIn("42", str_repr)


if __name__ == '__main__':
    unittest.main()
