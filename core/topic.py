"""
Topic management with partitioning support and subscription handling.
"""
import logging
import threading
from typing import Dict, List, Set, Optional, Callable
from dataclasses import dataclass
from collections import defaultdict

from .storage import StorageManager, PartitionOffset
from .message import Message, MessageType
from .config import get_config


logger = logging.getLogger(__name__)


@dataclass
class Subscription:
    """Represents a client subscription to a topic"""
    topic: str
    partition: int
    client_id: str
    offset: int
    auto_ack: bool
    callback: Optional[Callable[[Message], None]] = None


@dataclass
class TopicInfo:
    """Topic metadata"""
    name: str
    partition_count: int
    created_at: float
    retention_hours: int
    max_message_size: int


class TopicManager:
    """Manages topics, subscriptions, and message routing"""
    
    def __init__(self, storage_manager: StorageManager):
        self.storage = storage_manager
        self.config = get_config()
        
        self._topics: Dict[str, TopicInfo] = {}
        self._subscriptions: Dict[str, List[Subscription]] = defaultdict(list)  # topic -> subscriptions
        self._client_subscriptions: Dict[str, List[Subscription]] = defaultdict(list)  # client_id -> subscriptions
        
        self._lock = threading.RLock()
        
        # Message routing
        self._message_handlers: Dict[str, List[Callable[[Message], None]]] = defaultdict(list)
    
    def create_topic(self, 
                    name: str, 
                    partition_count: Optional[int] = None,
                    retention_hours: Optional[int] = None,
                    max_message_size: Optional[int] = None) -> TopicInfo:
        """Create a new topic"""
        with self._lock:
            if name in self._topics:
                raise ValueError(f"Topic '{name}' already exists")
            
            # Use defaults from config
            partition_count = partition_count or self.config.get('topics.default_partitions')
            retention_hours = retention_hours or self.config.get('topics.retention_hours')
            max_message_size = max_message_size or self.config.get('topics.max_message_size')
            
            # Validate partition count
            max_partitions = self.config.get('topics.max_partitions')
            if partition_count > max_partitions:
                raise ValueError(f"Partition count {partition_count} exceeds maximum {max_partitions}")
            
            # Create in storage
            self.storage.create_topic(name, partition_count)
            
            # Create topic info
            import time
            topic_info = TopicInfo(
                name=name,
                partition_count=partition_count,
                created_at=time.time(),
                retention_hours=retention_hours,
                max_message_size=max_message_size
            )
            
            self._topics[name] = topic_info
            logger.info(f"Created topic '{name}' with {partition_count} partitions")
            
            return topic_info
    
    def delete_topic(self, name: str) -> None:
        """Delete a topic and all its subscriptions"""
        with self._lock:
            if name not in self._topics:
                raise ValueError(f"Topic '{name}' does not exist")
            
            # Remove all subscriptions for this topic
            subscriptions_to_remove = self._subscriptions[name][:]
            for subscription in subscriptions_to_remove:
                self._remove_subscription(subscription)
            
            # Delete from storage
            self.storage.delete_topic(name)
            
            # Remove topic info
            del self._topics[name]
            
            logger.info(f"Deleted topic '{name}'")
    
    def get_topic_info(self, name: str) -> TopicInfo:
        """Get topic information"""
        with self._lock:
            if name not in self._topics:
                raise ValueError(f"Topic '{name}' does not exist")
            return self._topics[name]
    
    def list_topics(self) -> List[TopicInfo]:
        """List all topics"""
        with self._lock:
            return list(self._topics.values())
    
    def publish_message(self, 
                       topic: str, 
                       message: Message, 
                       partition: Optional[int] = None) -> PartitionOffset:
        """Publish a message to a topic"""
        with self._lock:
            if topic not in self._topics:
                raise ValueError(f"Topic '{topic}' does not exist")
            
            topic_info = self._topics[topic]
            
            # Validate message size
            serialized = message.serialize()
            if len(serialized) > topic_info.max_message_size:
                raise ValueError(f"Message size {len(serialized)} exceeds maximum {topic_info.max_message_size}")
            
            # Add topic to message properties
            message.properties['topic'] = topic
            
            # Store message
            partition_offset = self.storage.append_message(topic, message, partition)
            
            # Notify subscribers
            self._notify_subscribers(topic, partition_offset.partition, message)
            
            logger.debug(f"Published message to {topic}[{partition_offset.partition}] at offset {partition_offset.offset}")
            
            return partition_offset
    
    def subscribe(self, 
                 topic: str, 
                 client_id: str,
                 partition: int = 0,
                 start_offset: int = 0,
                 auto_ack: bool = True,
                 callback: Optional[Callable[[Message], None]] = None) -> Subscription:
        """Subscribe a client to a topic partition"""
        with self._lock:
            if topic not in self._topics:
                raise ValueError(f"Topic '{topic}' does not exist")
            
            topic_info = self._topics[topic]
            if partition >= topic_info.partition_count:
                raise ValueError(f"Partition {partition} does not exist for topic '{topic}'")
            
            # Check if client already subscribed to this topic/partition
            for sub in self._client_subscriptions[client_id]:
                if sub.topic == topic and sub.partition == partition:
                    raise ValueError(f"Client {client_id} already subscribed to {topic}[{partition}]")
            
            subscription = Subscription(
                topic=topic,
                partition=partition,
                client_id=client_id,
                offset=start_offset,
                auto_ack=auto_ack,
                callback=callback
            )
            
            self._subscriptions[topic].append(subscription)
            self._client_subscriptions[client_id].append(subscription)
            
            logger.info(f"Client {client_id} subscribed to {topic}[{partition}] from offset {start_offset}")
            
            return subscription
    
    def unsubscribe(self, topic: str, client_id: str, partition: int = 0) -> None:
        """Unsubscribe a client from a topic partition"""
        with self._lock:
            subscription = None
            
            # Find the subscription
            for sub in self._client_subscriptions[client_id]:
                if sub.topic == topic and sub.partition == partition:
                    subscription = sub
                    break
            
            if subscription is None:
                raise ValueError(f"Client {client_id} not subscribed to {topic}[{partition}]")
            
            self._remove_subscription(subscription)
            
            logger.info(f"Client {client_id} unsubscribed from {topic}[{partition}]")
    
    def unsubscribe_client(self, client_id: str) -> None:
        """Unsubscribe a client from all topics"""
        with self._lock:
            subscriptions = self._client_subscriptions[client_id][:]
            for subscription in subscriptions:
                self._remove_subscription(subscription)
            
            logger.info(f"Unsubscribed client {client_id} from all topics")
    
    def get_client_subscriptions(self, client_id: str) -> List[Subscription]:
        """Get all subscriptions for a client"""
        with self._lock:
            return self._client_subscriptions[client_id][:]
    
    def pull_messages(self, 
                     topic: str, 
                     partition: int,
                     start_offset: int = 0,
                     max_count: int = 100) -> List[Message]:
        """Pull messages from a topic partition"""
        with self._lock:
            if topic not in self._topics:
                raise ValueError(f"Topic '{topic}' does not exist")
            
            return self.storage.read_messages(topic, partition, start_offset, max_count)
    
    def acknowledge_message(self, 
                           topic: str, 
                           partition: int,
                           offset: int,
                           client_id: str) -> None:
        """Acknowledge message consumption"""
        with self._lock:
            # Find client's subscription
            subscription = None
            for sub in self._client_subscriptions[client_id]:
                if sub.topic == topic and sub.partition == partition:
                    subscription = sub
                    break
            
            if subscription is None:
                raise ValueError(f"Client {client_id} not subscribed to {topic}[{partition}]")
            
            # Update offset
            subscription.offset = max(subscription.offset, offset + 1)
            
            logger.debug(f"Client {client_id} acknowledged {topic}[{partition}] offset {offset}")
    
    def _remove_subscription(self, subscription: Subscription) -> None:
        """Remove a subscription from all tracking structures"""
        # Remove from topic subscriptions
        if subscription in self._subscriptions[subscription.topic]:
            self._subscriptions[subscription.topic].remove(subscription)
        
        # Remove from client subscriptions
        if subscription in self._client_subscriptions[subscription.client_id]:
            self._client_subscriptions[subscription.client_id].remove(subscription)
    
    def _notify_subscribers(self, topic: str, partition: int, message: Message) -> None:
        """Notify subscribers of new message"""
        subscriptions = [sub for sub in self._subscriptions[topic] 
                        if sub.partition == partition]
        
        for subscription in subscriptions:
            try:
                if subscription.callback:
                    subscription.callback(message)
                
                # Update offset if auto-ack
                if subscription.auto_ack:
                    subscription.offset = message.sequence_number + 1
                    
            except Exception as e:
                logger.error(f"Error notifying subscriber {subscription.client_id}: {e}")
    
    def get_topic_stats(self, topic: str) -> Dict:
        """Get statistics for a topic"""
        with self._lock:
            if topic not in self._topics:
                raise ValueError(f"Topic '{topic}' does not exist")
            
            topic_info = self._topics[topic]
            topic_storage = self.storage.get_topic(topic)
            
            partition_stats = []
            total_messages = 0
            
            for partition_id in range(topic_info.partition_count):
                log = topic_storage._partitions[partition_id]
                message_count = log.get_message_count()
                latest_offset = log.get_latest_offset()
                
                total_messages += message_count
                
                partition_stats.append({
                    'partition': partition_id,
                    'message_count': message_count,
                    'latest_offset': latest_offset,
                    'subscribers': len([sub for sub in self._subscriptions[topic] 
                                      if sub.partition == partition_id])
                })
            
            return {
                'topic': topic,
                'partition_count': topic_info.partition_count,
                'total_messages': total_messages,
                'subscribers': len(self._subscriptions[topic]),
                'partitions': partition_stats,
                'created_at': topic_info.created_at,
                'retention_hours': topic_info.retention_hours
            }
