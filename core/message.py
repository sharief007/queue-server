"""
Message format implementation using Python's struct module for binary serialization.
Supports properties (key-value pairs) and message body with metadata.
"""
import struct
import time
from typing import Dict, Any, Optional, Tuple
from enum import IntEnum


class MessageType(IntEnum):
    """Message types for different operations"""
    DATA = 1
    ACK = 2
    HEARTBEAT = 3
    CREATE_TOPIC = 4
    SUBSCRIBE = 5
    UNSUBSCRIBE = 6
    PUBLISH = 7
    PULL_MESSAGES = 8
    PULL_START = 9      # Start streaming pull
    PULL_STOP = 10      # Stop streaming pull 


class Message:
    """
    Binary message format:
    [4 bytes: total_length][4 bytes: message_type][8 bytes: sequence_number]
    [8 bytes: timestamp][4 bytes: properties_length][properties_data]
    [4 bytes: body_length][body_data]
    """
    
    HEADER_SIZE = 32  # Fixed header size in bytes
    
    def __init__(self, 
                 message_type: MessageType,
                 body: bytes = b'',
                 properties: Optional[Dict[str, str]] = None,
                 sequence_number: int = 0,
                 timestamp: Optional[float] = None):
        self.message_type = message_type
        self.body = body
        self.properties = properties or {}
        self.sequence_number = sequence_number
        self.timestamp = timestamp or time.time()
        
        # Standard properties
        self.properties.setdefault('created_at', str(self.timestamp))
        self.properties.setdefault('version', '1.0')
    
    def serialize(self) -> bytes:
        """Serialize message to binary format"""
        # Encode properties as key=value pairs separated by \x00
        props_data = b''
        for key, value in self.properties.items():
            props_data += f"{key}={value}".encode('utf-8') + b'\x00'
        
        props_length = len(props_data)
        body_length = len(self.body)
        total_length = self.HEADER_SIZE + props_length + body_length
        
        # Pack header
        header = struct.pack('>IIQQ II',
                           total_length,      # 4 bytes
                           self.message_type,  # 4 bytes
                           self.sequence_number,  # 8 bytes
                           int(self.timestamp * 1000),  # 8 bytes (ms)
                           props_length,      # 4 bytes
                           body_length)       # 4 bytes
        
        return header + props_data + self.body
    
    @classmethod
    def deserialize(cls, data: bytes) -> 'Message':
        """Deserialize binary data to Message object"""
        if len(data) < cls.HEADER_SIZE:
            raise ValueError("Invalid message: too short")
        
        # Unpack header
        header = struct.unpack('>IIQQ II', data[:cls.HEADER_SIZE])
        total_length, msg_type, seq_num, timestamp_ms, props_len, body_len = header
        
        if len(data) != total_length:
            raise ValueError(f"Invalid message length: expected {total_length}, got {len(data)}")
        
        # Extract properties
        props_start = cls.HEADER_SIZE
        props_end = props_start + props_len
        props_data = data[props_start:props_end]
        
        properties = {}
        if props_data:
            props_str = props_data.decode('utf-8').rstrip('\x00')
            for prop in props_str.split('\x00'):
                if '=' in prop:
                    key, value = prop.split('=', 1)
                    properties[key] = value
        
        # Extract body
        body_start = props_end
        body = data[body_start:body_start + body_len]
        
        timestamp = timestamp_ms / 1000.0
        
        return cls(
            message_type=MessageType(msg_type),
            body=body,
            properties=properties,
            sequence_number=seq_num,
            timestamp=timestamp
        )
    
    def __str__(self) -> str:
        return (f"Message(type={self.message_type.name}, seq={self.sequence_number}, "
                f"props={len(self.properties)}, body_size={len(self.body)})")


class MessageBuilder:
    """Builder pattern for creating messages"""
    
    def __init__(self, message_type: MessageType):
        self._message_type = message_type
        self._body = b''
        self._properties = {}
        self._sequence_number = 0
    
    def body(self, data: bytes) -> 'MessageBuilder':
        self._body = data
        return self
    
    def text_body(self, text: str) -> 'MessageBuilder':
        self._body = text.encode('utf-8')
        return self
    
    def property(self, key: str, value: str) -> 'MessageBuilder':
        self._properties[key] = value
        return self
    
    def properties(self, props: Dict[str, str]) -> 'MessageBuilder':
        self._properties.update(props)
        return self
    
    def sequence_number(self, seq: int) -> 'MessageBuilder':
        self._sequence_number = seq
        return self
    
    def build(self) -> Message:
        return Message(
            message_type=self._message_type,
            body=self._body,
            properties=self._properties,
            sequence_number=self._sequence_number
        )


def create_data_message(body: bytes, **properties) -> Message:
    """Convenience function to create data messages"""
    return MessageBuilder(MessageType.DATA).body(body).properties(properties).build()

def create_ack_message(sequence_number: int) -> Message:
    """Convenience function to create ACK messages"""
    return MessageBuilder(MessageType.ACK).sequence_number(sequence_number).build()

def create_heartbeat_message() -> Message:
    """Convenience function to create heartbeat messages"""
    return MessageBuilder(MessageType.HEARTBEAT).build()
