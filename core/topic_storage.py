"""
Topic storage implementation without partitioning.
Messages are stored sequentially with immediate disk persistence.
"""
import threading
import time
import struct
import logging
import os
from typing import Dict, List, Optional, BinaryIO
from pathlib import Path
from dataclasses import dataclass

from .message import Message
from .config import get_config


logger = logging.getLogger(__name__)


@dataclass
class MessageOffset:
    """Represents an offset within a topic"""
    topic: str
    offset: int


class TopicLog:
    """Thread-safe append-only log for a single topic"""
    
    def __init__(self, topic: str, data_dir: str):
        self.topic = topic
        self.data_dir = Path(data_dir)
        self._lock = threading.RLock()
        self._next_offset = 0
        
        # File handles
        self.log_file: Optional[BinaryIO] = None
        self.index_file: Optional[BinaryIO] = None
        
        # File paths
        self.log_path = self.data_dir / f"{topic}.log"
        self.index_path = self.data_dir / f"{topic}.idx"
        
        # Ensure directory exists
        self.data_dir.mkdir(parents=True, exist_ok=True)
        
        # Initialize files and load existing data
        self._initialize_files()
        self._load_existing_data()
    
    def _initialize_files(self):
        """Initialize or open log and index files"""
        self.log_file = open(self.log_path, 'ab')
        self.index_file = open(self.index_path, 'ab')
    
    def _load_existing_data(self):
        """Load existing messages to determine next offset"""
        if not self.index_path.exists():
            self._next_offset = 0
            return
        
        try:
            # Count entries in index file (each entry is 4 bytes)
            index_size = self.index_path.stat().st_size
            self._next_offset = index_size // 4
            logger.info(f"Topic {self.topic}: loaded {self._next_offset} existing messages")
        except Exception as e:
            logger.error(f"Failed to load existing data for topic {self.topic}: {e}")
            self._next_offset = 0
    
    def append(self, message: Message) -> int:
        """Append message with guaranteed durability and return offset"""
        with self._lock:
            # Assign sequence number (which is also the offset)
            message.sequence_number = self._next_offset
            offset = self._next_offset
            self._next_offset += 1
            
            # Serialize message
            data = message.serialize()
            
            # Get current file position before writing
            position = self.log_file.tell()
            
            # Write to log: [4-byte length][message data]
            self.log_file.write(struct.pack('>I', len(data)))
            self.log_file.write(data)
            self.log_file.flush()
            
            # Write to index: [4-byte file position]
            self.index_file.write(struct.pack('>I', position))
            self.index_file.flush()
            
            # Force sync to disk for durability
            os.fsync(self.log_file.fileno())
            os.fsync(self.index_file.fileno())
            
            logger.debug(f"Appended message to topic {self.topic} at offset {offset}")
            return offset
    
    def read_message_at_offset(self, offset: int) -> Optional[Message]:
        """Read a single message at the given offset"""
        with self._lock:
            if offset >= self._next_offset:
                return None
            
            try:
                # Read file position from index
                with open(self.index_path, 'rb') as idx_file:
                    idx_file.seek(offset * 4)
                    position_data = idx_file.read(4)
                    if len(position_data) != 4:
                        return None
                    position = struct.unpack('>I', position_data)[0]
                
                # Read message from log
                with open(self.log_path, 'rb') as log_file:
                    log_file.seek(position)
                    
                    # Read message length
                    length_data = log_file.read(4)
                    if len(length_data) != 4:
                        return None
                    message_length = struct.unpack('>I', length_data)[0]
                    
                    # Read message data
                    message_data = log_file.read(message_length)
                    if len(message_data) != message_length:
                        return None
                    
                    # Reconstruct full message with length prefix
                    full_data = length_data + message_data
                    return Message.deserialize(message_data)
                    
            except Exception as e:
                logger.error(f"Failed to read message at offset {offset} for topic {self.topic}: {e}")
                return None
    
    def get_next_offset(self) -> int:
        """Get the next offset (where next message will be written)"""
        with self._lock:
            return self._next_offset
    
    def get_message_count(self) -> int:
        """Get total number of messages"""
        with self._lock:
            return self._next_offset
    
    def get_log_file_path(self) -> Path:
        """Get the log file path for sendfile operations"""
        return self.log_path
    
    def get_message_file_info(self, offset: int) -> Optional[tuple]:
        """Get file position and length for sendfile operation"""
        with self._lock:
            if offset >= self._next_offset:
                return None
            
            try:
                # Read file position from index
                with open(self.index_path, 'rb') as idx_file:
                    idx_file.seek(offset * 4)
                    position_data = idx_file.read(4)
                    if len(position_data) != 4:
                        return None
                    position = struct.unpack('>I', position_data)[0]
                
                # Read message length from log
                with open(self.log_path, 'rb') as log_file:
                    log_file.seek(position)
                    length_data = log_file.read(4)
                    if len(length_data) != 4:
                        return None
                    message_length = struct.unpack('>I', length_data)[0]
                    
                    # Return position and total length (4 bytes length + message data)
                    return (position, 4 + message_length)
                    
            except Exception as e:
                logger.error(f"Failed to get file info for offset {offset} in topic {self.topic}: {e}")
                return None
    
    def close(self):
        """Close file handles"""
        with self._lock:
            if self.log_file:
                self.log_file.close()
                self.log_file = None
            if self.index_file:
                self.index_file.close()
                self.index_file = None


class TopicStorageManager:
    """Manages storage for all topics"""
    
    def __init__(self):
        self.config = get_config()
        self.data_dir = self.config.get('storage.data_dir')
        
        self._topics: Dict[str, TopicLog] = {}
        self._lock = threading.RLock()
        
        # Ensure data directory exists
        Path(self.data_dir).mkdir(parents=True, exist_ok=True)
    
    def create_topic(self, name: str) -> None:
        """Create a new topic"""
        with self._lock:
            if name in self._topics:
                raise ValueError(f"Topic '{name}' already exists")
            
            self._topics[name] = TopicLog(name, self.data_dir)
            logger.info(f"Created topic: {name}")
    
    def delete_topic(self, name: str) -> None:
        """Delete a topic"""
        with self._lock:
            if name not in self._topics:
                raise ValueError(f"Topic '{name}' does not exist")
            
            # Close the topic log
            self._topics[name].close()
            
            # Remove files
            topic_log = self._topics[name]
            try:
                topic_log.log_path.unlink(missing_ok=True)
                topic_log.index_path.unlink(missing_ok=True)
            except Exception as e:
                logger.warning(f"Failed to delete topic files for {name}: {e}")
            
            del self._topics[name]
            logger.info(f"Deleted topic: {name}")
    
    def get_topic(self, name: str) -> TopicLog:
        """Get topic storage"""
        with self._lock:
            if name not in self._topics:
                raise ValueError(f"Topic '{name}' does not exist")
            return self._topics[name]
    
    def list_topics(self) -> List[str]:
        """List all topics"""
        with self._lock:
            return list(self._topics.keys())
    
    def append_message(self, topic: str, message: Message) -> MessageOffset:
        """Append message to topic"""
        topic_log = self.get_topic(topic)
        offset = topic_log.append(message)
        return MessageOffset(topic, offset)
    
    def close_all(self):
        """Close all topic logs"""
        with self._lock:
            for topic_log in self._topics.values():
                topic_log.close()
            self._topics.clear()
