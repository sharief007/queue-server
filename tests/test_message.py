"""
Unit tests for the message module.
Tests message serialization, deserialization, and validation.
"""

import unittest
import time
import struct
from core.message import (
    Message,
    MessageType,
    MessageBuilder,
    create_data_message,
    create_ack_message,
    create_heartbeat_message,
)


class TestMessage(unittest.TestCase):
    """Test cases for the Message class"""

    def setUp(self):
        """Set up test fixtures"""
        self.test_timestamp = time.time()
        self.test_body = b"Hello, World!"
        self.test_properties = {"key1": "value1", "key2": "value2"}

    def test_message_creation(self):
        """Test basic message creation"""
        msg = Message(
            message_type=MessageType.DATA,
            body=self.test_body,
            properties=self.test_properties,
            sequence_number=42,
            timestamp=self.test_timestamp,
        )

        self.assertEqual(msg.message_type, MessageType.DATA)
        self.assertEqual(msg.body, self.test_body)
        self.assertEqual(msg.sequence_number, 42)
        self.assertEqual(msg.timestamp, self.test_timestamp)
        self.assertEqual(msg.properties["key1"], "value1")
        self.assertEqual(msg.properties["key2"], "value2")

    def test_message_default_properties(self):
        """Test that default properties are added"""
        msg = Message(MessageType.DATA, self.test_body)

        self.assertIn("created_at", msg.properties)
        self.assertIn("version", msg.properties)
        self.assertEqual(msg.properties["version"], "1.0")

    def test_message_serialization(self):
        """Test message serialization"""
        msg = Message(
            message_type=MessageType.DATA,
            body=self.test_body,
            properties=self.test_properties,
            sequence_number=42,
            timestamp=self.test_timestamp,
        )

        serialized = msg.serialize()
        self.assertIsInstance(serialized, bytes)
        self.assertGreater(len(serialized), Message.HEADER_SIZE)

    def test_message_deserialization(self):
        """Test message deserialization"""
        original = Message(
            message_type=MessageType.DATA,
            body=self.test_body,
            properties=self.test_properties,
            sequence_number=42,
            timestamp=self.test_timestamp,
        )

        serialized = original.serialize()
        deserialized = Message.deserialize(serialized)

        self.assertEqual(deserialized.message_type, original.message_type)
        self.assertEqual(deserialized.body, original.body)
        self.assertEqual(deserialized.sequence_number, original.sequence_number)
        self.assertAlmostEqual(deserialized.timestamp, original.timestamp, places=1)

        # Check properties (excluding auto-added ones)
        for key, value in self.test_properties.items():
            self.assertEqual(deserialized.properties[key], value)

    def test_message_roundtrip(self):
        """Test serialization -> deserialization roundtrip"""
        for msg_type in MessageType:
            with self.subTest(message_type=msg_type):
                original = Message(
                    message_type=msg_type,
                    body=f"Test body for {msg_type.name}".encode("utf-8"),
                    properties={"test": f"value_{msg_type.value}"},
                    sequence_number=msg_type.value * 10,
                )

                serialized = original.serialize()
                deserialized = Message.deserialize(serialized)

                self.assertEqual(deserialized.message_type, original.message_type)
                self.assertEqual(deserialized.body, original.body)
                self.assertEqual(deserialized.sequence_number, original.sequence_number)

    def test_empty_message(self):
        """Test message with empty body and no properties"""
        msg = Message(MessageType.HEARTBEAT)

        serialized = msg.serialize()
        deserialized = Message.deserialize(serialized)

        self.assertEqual(deserialized.message_type, MessageType.HEARTBEAT)
        self.assertEqual(deserialized.body, b"")

    def test_large_message(self):
        """Test message with large body"""
        large_body = b"X" * (1024 * 1024)  # 1MB
        msg = Message(MessageType.DATA, large_body)

        serialized = msg.serialize()
        deserialized = Message.deserialize(serialized)

        self.assertEqual(deserialized.body, large_body)

    def test_unicode_properties(self):
        """Test message with Unicode properties"""
        unicode_props = {
            "emoji": "🚀",
            "chinese": "你好",
            "arabic": "مرحبا",
            "japanese": "こんにちは",
        }

        msg = Message(MessageType.DATA, b"test", unicode_props)

        serialized = msg.serialize()
        deserialized = Message.deserialize(serialized)

        for key, value in unicode_props.items():
            self.assertEqual(deserialized.properties[key], value)

    def test_invalid_message_data(self):
        """Test deserialization with invalid data"""
        with self.assertRaises(ValueError):
            Message.deserialize(b"too short")

        with self.assertRaises(ValueError):
            # Header claims longer length than actual data
            fake_header = struct.pack(">IIQQ II", 1000, 1, 0, 0, 0, 0)
            Message.deserialize(fake_header + b"short")

    def test_message_string_representation(self):
        """Test message string representation"""
        msg = Message(MessageType.DATA, self.test_body, self.test_properties, 42)
        str_repr = str(msg)

        self.assertIn("DATA", str_repr)
        self.assertIn("seq=42", str_repr)
        self.assertIn(f"body_size={len(self.test_body)}", str_repr)


class TestMessageBuilder(unittest.TestCase):
    """Test cases for the MessageBuilder class"""

    def test_builder_pattern(self):
        """Test the builder pattern"""
        msg = (
            MessageBuilder(MessageType.PUBLISH)
            .body(b"test message")
            .property("topic", "test-topic")
            .property("partition", "0")
            .sequence_number(100)
            .build()
        )

        self.assertEqual(msg.message_type, MessageType.PUBLISH)
        self.assertEqual(msg.body, b"test message")
        self.assertEqual(msg.properties["topic"], "test-topic")
        self.assertEqual(msg.properties["partition"], "0")
        self.assertEqual(msg.sequence_number, 100)

    def test_builder_string_body(self):
        """Test builder with text body"""
        msg = MessageBuilder(MessageType.DATA).text_body("test string").build()

        self.assertEqual(msg.body, b"test string")

    def test_builder_multiple_properties(self):
        """Test builder with multiple properties"""
        props = {"key1": "value1", "key2": "value2", "key3": "value3"}

        builder = MessageBuilder(MessageType.DATA)
        for key, value in props.items():
            builder.property(key, value)

        msg = builder.build()

        for key, value in props.items():
            self.assertEqual(msg.properties[key], value)

    def test_builder_data_message_factory(self):
        """Test data message factory function"""
        msg = create_data_message(
            body=b"data content", topic="test-topic", partition="1"
        )

        self.assertEqual(msg.message_type, MessageType.DATA)
        self.assertEqual(msg.body, b"data content")
        self.assertEqual(msg.properties["topic"], "test-topic")
        self.assertEqual(msg.properties["partition"], "1")

    def test_builder_ack_message_factory(self):
        """Test ACK message factory function"""
        msg = create_ack_message(sequence_number=42)

        self.assertEqual(msg.message_type, MessageType.ACK)
        self.assertEqual(msg.sequence_number, 42)

    def test_builder_heartbeat_message_factory(self):
        """Test heartbeat message factory function"""
        msg = create_heartbeat_message()

        self.assertEqual(msg.message_type, MessageType.HEARTBEAT)
        self.assertEqual(msg.body, b"")


class TestMessageType(unittest.TestCase):
    """Test cases for MessageType enum"""

    def test_message_type_values(self):
        """Test that message types have expected values"""
        expected_types = {
            "DATA": 1,
            "ACK": 2,
            "HEARTBEAT": 3,
            "CREATE_TOPIC": 4,
            "CREATE_SUBSCRIPTION": 5,
            "DELETE_SUBSCRIPTION": 6,
            "SUBSCRIBE": 7,
            "UNSUBSCRIBE": 8,
            "PUBLISH": 9,
        }

        for name, value in expected_types.items():
            msg_type = getattr(MessageType, name)
            self.assertEqual(msg_type.value, value)

    def test_message_type_from_int(self):
        """Test creating MessageType from integer"""
        for i in range(1, 10):  # MessageType values go from 1 to 9
            msg_type = MessageType(i)
            self.assertEqual(msg_type.value, i)

    def test_invalid_message_type(self):
        """Test invalid message type values"""
        with self.assertRaises(ValueError):
            MessageType(999)


if __name__ == "__main__":
    unittest.main()
