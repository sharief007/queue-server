"""
Storage layer implementation with append-based logs and periodic snapshots.
Supports in-memory operations with disk persistence.
"""
import os
import threading
import time
import struct
import logging
from typing import Dict, List, Optional, Iterator, Tuple
from pathlib import Path
from dataclasses import dataclass
from collections import defaultdict, deque

from .message import Message, MessageType
from .config import get_config


logger = logging.getLogger(__name__)


@dataclass
class PartitionOffset:
    """Represents an offset within a partition"""
    partition: int
    offset: int


class AppendLog:
    """Thread-safe append-only log for a single partition"""
    
    def __init__(self, topic: str, partition: int, data_dir: str):
        self.topic = topic
        self.partition = partition
        self.data_dir = Path(data_dir)
        self.log_file = self.data_dir / f"{topic}_p{partition}.log"
        self.index_file = self.data_dir / f"{topic}_p{partition}.idx"
        
        self._lock = threading.RLock()
        self._messages: List[Message] = []
        self._offsets: List[int] = []  # File offsets for each message
        self._next_offset = 0
        self._file_size = 0
        
        # Ensure directory exists
        self.data_dir.mkdir(parents=True, exist_ok=True)
        
        # Load existing data
        self._load_from_disk()
    
    def append(self, message: Message) -> int:
        """Append message and return its offset"""
        with self._lock:
            # Assign sequence number
            message.sequence_number = self._next_offset
            
            # Add to memory
            self._messages.append(message)
            self._offsets.append(self._file_size)
            
            # Write to disk
            serialized = message.serialize()
            self._write_to_disk(serialized)
            
            offset = self._next_offset
            self._next_offset += 1
            
            logger.debug(f"Appended message to {self.topic}[{self.partition}] at offset {offset}")
            return offset
    
    def read(self, start_offset: int = 0, max_count: int = 100) -> List[Message]:
        """Read messages starting from offset"""
        with self._lock:
            if start_offset >= len(self._messages):
                return []
            
            end_offset = min(start_offset + max_count, len(self._messages))
            return self._messages[start_offset:end_offset]
    
    def get_latest_offset(self) -> int:
        """Get the latest offset (next message will have this offset)"""
        with self._lock:
            return self._next_offset
    
    def get_message_count(self) -> int:
        """Get total number of messages"""
        with self._lock:
            return len(self._messages)
    
    def _write_to_disk(self, data: bytes) -> None:
        """Write serialized message to disk"""
        try:
            with open(self.log_file, 'ab') as f:
                # Write message length first
                f.write(struct.pack('>I', len(data)))
                f.write(data)
                f.flush()
            
            # Update index
            with open(self.index_file, 'ab') as f:
                f.write(struct.pack('>I', self._file_size))
                f.flush()
            
            self._file_size += 4 + len(data)  # 4 bytes for length + message
            
        except Exception as e:
            logger.error(f"Failed to write to disk: {e}")
            raise
    
    def _load_from_disk(self) -> None:
        """Load existing messages from disk"""
        if not self.log_file.exists():
            return
        
        try:
            with open(self.log_file, 'rb') as f:
                file_offset = 0
                
                while True:
                    # Read message length
                    length_data = f.read(4)
                    if len(length_data) < 4:
                        break
                    
                    message_length = struct.unpack('>I', length_data)[0]
                    
                    # Read message data
                    message_data = f.read(message_length)
                    if len(message_data) < message_length:
                        logger.warning(f"Incomplete message in {self.log_file}")
                        break
                    
                    # Deserialize message
                    try:
                        message = Message.deserialize(message_data)
                        self._messages.append(message)
                        self._offsets.append(file_offset)
                        
                        # Update counters
                        self._next_offset = max(self._next_offset, message.sequence_number + 1)
                        file_offset += 4 + message_length
                        
                    except Exception as e:
                        logger.error(f"Failed to deserialize message: {e}")
                        break
                
                self._file_size = file_offset
                
            logger.info(f"Loaded {len(self._messages)} messages from {self.log_file}")
            
        except Exception as e:
            logger.error(f"Failed to load from disk: {e}")


class TopicStorage:
    """Manages storage for a single topic with multiple partitions"""
    
    def __init__(self, name: str, partition_count: int, data_dir: str):
        self.name = name
        self.partition_count = partition_count
        self.data_dir = data_dir
        
        self._partitions: Dict[int, AppendLog] = {}
        self._lock = threading.RLock()
        
        # Initialize partitions
        for partition_id in range(partition_count):
            self._partitions[partition_id] = AppendLog(name, partition_id, data_dir)
    
    def append(self, message: Message, partition: Optional[int] = None) -> PartitionOffset:
        """Append message to a partition"""
        if partition is None:
            # Simple round-robin partitioning
            partition = hash(message.properties.get('key', '')) % self.partition_count
        
        if partition not in self._partitions:
            raise ValueError(f"Invalid partition {partition}")
        
        offset = self._partitions[partition].append(message)
        return PartitionOffset(partition, offset)
    
    def read(self, partition: int, start_offset: int = 0, max_count: int = 100) -> List[Message]:
        """Read messages from a specific partition"""
        if partition not in self._partitions:
            raise ValueError(f"Invalid partition {partition}")
        
        return self._partitions[partition].read(start_offset, max_count)
    
    def get_latest_offsets(self) -> Dict[int, int]:
        """Get latest offset for each partition"""
        return {p: log.get_latest_offset() for p, log in self._partitions.items()}
    
    def get_partition_count(self) -> int:
        """Get number of partitions"""
        return self.partition_count


class StorageManager:
    """Main storage manager handling multiple topics"""
    
    def __init__(self):
        self.config = get_config()
        self.data_dir = self.config.get('storage.data_dir')
        self.snapshot_dir = self.config.get('storage.snapshot_dir')
        self.snapshot_interval = self.config.get('storage.snapshot_interval')
        
        self._topics: Dict[str, TopicStorage] = {}
        self._lock = threading.RLock()
        self._snapshot_thread: Optional[threading.Thread] = None
        self._running = False
        
        # Ensure directories exist
        Path(self.data_dir).mkdir(parents=True, exist_ok=True)
        Path(self.snapshot_dir).mkdir(parents=True, exist_ok=True)
    
    def start(self) -> None:
        """Start the storage manager"""
        with self._lock:
            if self._running:
                return
            
            self._running = True
            self._snapshot_thread = threading.Thread(
                target=self._snapshot_worker,
                daemon=True,
                name="SnapshotWorker"
            )
            self._snapshot_thread.start()
            
            logger.info("Storage manager started")
    
    def stop(self) -> None:
        """Stop the storage manager"""
        with self._lock:
            self._running = False
            
            if self._snapshot_thread and self._snapshot_thread.is_alive():
                self._snapshot_thread.join(timeout=5)
            
            logger.info("Storage manager stopped")
    
    def create_topic(self, name: str, partition_count: int) -> None:
        """Create a new topic"""
        with self._lock:
            if name in self._topics:
                raise ValueError(f"Topic {name} already exists")
            
            self._topics[name] = TopicStorage(name, partition_count, self.data_dir)
            logger.info(f"Created topic {name} with {partition_count} partitions")
    
    def delete_topic(self, name: str) -> None:
        """Delete a topic"""
        with self._lock:
            if name not in self._topics:
                raise ValueError(f"Topic {name} does not exist")
            
            del self._topics[name]
            logger.info(f"Deleted topic {name}")
    
    def get_topic(self, name: str) -> TopicStorage:
        """Get topic storage"""
        with self._lock:
            if name not in self._topics:
                raise ValueError(f"Topic {name} does not exist")
            return self._topics[name]
    
    def list_topics(self) -> List[str]:
        """List all topics"""
        with self._lock:
            return list(self._topics.keys())
    
    def append_message(self, topic: str, message: Message, partition: Optional[int] = None) -> PartitionOffset:
        """Append message to topic"""
        topic_storage = self.get_topic(topic)
        return topic_storage.append(message, partition)
    
    def read_messages(self, topic: str, partition: int, start_offset: int = 0, max_count: int = 100) -> List[Message]:
        """Read messages from topic partition"""
        topic_storage = self.get_topic(topic)
        return topic_storage.read(partition, start_offset, max_count)
    
    def _snapshot_worker(self) -> None:
        """Background worker for creating snapshots"""
        while self._running:
            try:
                time.sleep(self.snapshot_interval)
                if self._running:
                    self._create_snapshot()
            except Exception as e:
                logger.error(f"Snapshot worker error: {e}")
    
    def _create_snapshot(self) -> None:
        """Create a snapshot of current state"""
        # For now, just log the state
        # In a production system, this would serialize topics metadata to disk
        with self._lock:
            total_messages = 0
            for topic_name, topic in self._topics.items():
                for partition_id in range(topic.partition_count):
                    count = topic._partitions[partition_id].get_message_count()
                    total_messages += count
            
            logger.debug(f"Snapshot: {len(self._topics)} topics, {total_messages} total messages")
