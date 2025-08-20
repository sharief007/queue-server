"""
Subscription management for the message broker.
Handles subscriptions (consumer groups) and round-robin delivery to subscribers.
"""
import threading
import time
import socket
import logging
from typing import Dict, List, Set, Optional
from dataclasses import dataclass, field

from .message import Message


logger = logging.getLogger(__name__)


@dataclass
class Subscriber:
    """Represents a subscriber within a subscription"""
    subscriber_id: str
    connection_id: str
    socket: socket.socket
    last_heartbeat: float = field(default_factory=time.time)
    active: bool = True


@dataclass
class Subscription:
    """Represents a subscription (consumer group) for a topic"""
    subscription_id: str
    topic: str
    current_offset: int
    subscribers: Dict[str, Subscriber] = field(default_factory=dict)
    round_robin_index: int = 0
    created_at: float = field(default_factory=time.time)
    
    def add_subscriber(self, subscriber: Subscriber):
        """Add a subscriber to this subscription"""
        self.subscribers[subscriber.subscriber_id] = subscriber
        logger.info(f"Added subscriber {subscriber.subscriber_id} to subscription {self.subscription_id}")
    
    def remove_subscriber(self, subscriber_id: str):
        """Remove a subscriber from this subscription"""
        if subscriber_id in self.subscribers:
            del self.subscribers[subscriber_id]
            # Reset round robin if we removed the current subscriber
            if self.round_robin_index >= len(self.subscribers):
                self.round_robin_index = 0
            logger.info(f"Removed subscriber {subscriber_id} from subscription {self.subscription_id}")
    
    def get_next_subscriber(self) -> Optional[Subscriber]:
        """Get the next subscriber in round-robin fashion"""
        if not self.subscribers:
            return None
        
        active_subscribers = [s for s in self.subscribers.values() if s.active]
        if not active_subscribers:
            return None
        
        # Get subscriber by round-robin index
        subscriber = active_subscribers[self.round_robin_index % len(active_subscribers)]
        self.round_robin_index = (self.round_robin_index + 1) % len(active_subscribers)
        
        return subscriber
    
    def mark_subscriber_inactive(self, subscriber_id: str):
        """Mark a subscriber as inactive"""
        if subscriber_id in self.subscribers:
            self.subscribers[subscriber_id].active = False
    
    def get_active_subscriber_count(self) -> int:
        """Get count of active subscribers"""
        return sum(1 for s in self.subscribers.values() if s.active)


class SubscriptionManager:
    """Manages all subscriptions and message delivery"""
    
    def __init__(self):
        self._subscriptions: Dict[str, Subscription] = {}  # subscription_id -> Subscription
        self._topic_subscriptions: Dict[str, Set[str]] = {}  # topic -> set of subscription_ids
        self._subscriber_to_subscription: Dict[str, str] = {}  # subscriber_id -> subscription_id
        self._connection_to_subscriber: Dict[str, str] = {}  # connection_id -> subscriber_id
        
        self._lock = threading.RLock()
        
        # Heartbeat management
        self._heartbeat_timeout = 60.0  # seconds
        self._cleanup_thread: Optional[threading.Thread] = None
        self._running = False
    
    def start(self):
        """Start the subscription manager"""
        with self._lock:
            if self._running:
                return
            
            self._running = True
            self._cleanup_thread = threading.Thread(
                target=self._cleanup_worker,
                daemon=True,
                name="SubscriptionCleanup"
            )
            self._cleanup_thread.start()
            logger.info("Subscription manager started")
    
    def stop(self):
        """Stop the subscription manager"""
        with self._lock:
            self._running = False
            
            if self._cleanup_thread and self._cleanup_thread.is_alive():
                self._cleanup_thread.join(timeout=5)
            
            logger.info("Subscription manager stopped")
    
    def create_subscription(self, subscription_id: str, topic: str, start_offset: int = 0) -> Subscription:
        """Create a new subscription for a topic"""
        with self._lock:
            if subscription_id in self._subscriptions:
                raise ValueError(f"Subscription '{subscription_id}' already exists")
            
            subscription = Subscription(
                subscription_id=subscription_id,
                topic=topic,
                current_offset=start_offset
            )
            
            self._subscriptions[subscription_id] = subscription
            
            # Track topic subscriptions
            if topic not in self._topic_subscriptions:
                self._topic_subscriptions[topic] = set()
            self._topic_subscriptions[topic].add(subscription_id)
            
            logger.info(f"Created subscription {subscription_id} for topic {topic} starting at offset {start_offset}")
            return subscription
    
    def delete_subscription(self, subscription_id: str):
        """Delete a subscription"""
        with self._lock:
            if subscription_id not in self._subscriptions:
                raise ValueError(f"Subscription '{subscription_id}' does not exist")
            
            subscription = self._subscriptions[subscription_id]
            
            # Remove all subscribers
            subscriber_ids = list(subscription.subscribers.keys())
            for subscriber_id in subscriber_ids:
                self._remove_subscriber_internal(subscriber_id)
            
            # Remove from topic tracking
            topic = subscription.topic
            if topic in self._topic_subscriptions:
                self._topic_subscriptions[topic].discard(subscription_id)
                if not self._topic_subscriptions[topic]:
                    del self._topic_subscriptions[topic]
            
            del self._subscriptions[subscription_id]
            logger.info(f"Deleted subscription {subscription_id}")
    
    def add_subscriber(self, 
                      subscription_id: str, 
                      subscriber_id: str, 
                      connection_id: str, 
                      sock: socket.socket) -> None:
        """Add a subscriber to a subscription"""
        with self._lock:
            if subscription_id not in self._subscriptions:
                raise ValueError(f"Subscription '{subscription_id}' does not exist")
            
            if subscriber_id in self._subscriber_to_subscription:
                raise ValueError(f"Subscriber '{subscriber_id}' already exists")
            
            subscriber = Subscriber(
                subscriber_id=subscriber_id,
                connection_id=connection_id,
                socket=sock
            )
            
            subscription = self._subscriptions[subscription_id]
            subscription.add_subscriber(subscriber)
            
            # Track mappings
            self._subscriber_to_subscription[subscriber_id] = subscription_id
            self._connection_to_subscriber[connection_id] = subscriber_id
    
    def remove_subscriber(self, subscriber_id: str):
        """Remove a subscriber"""
        with self._lock:
            self._remove_subscriber_internal(subscriber_id)
    
    def remove_subscriber_by_connection(self, connection_id: str):
        """Remove a subscriber by connection ID"""
        with self._lock:
            subscriber_id = self._connection_to_subscriber.get(connection_id)
            if subscriber_id:
                self._remove_subscriber_internal(subscriber_id)
    
    def _remove_subscriber_internal(self, subscriber_id: str):
        """Internal method to remove a subscriber"""
        subscription_id = self._subscriber_to_subscription.get(subscriber_id)
        if not subscription_id:
            return
        
        subscription = self._subscriptions.get(subscription_id)
        if subscription:
            subscriber = subscription.subscribers.get(subscriber_id)
            if subscriber:
                subscription.remove_subscriber(subscriber_id)
                # Remove mappings
                del self._subscriber_to_subscription[subscriber_id]
                if subscriber.connection_id in self._connection_to_subscriber:
                    del self._connection_to_subscriber[subscriber.connection_id]
    
    def heartbeat_subscriber(self, subscriber_id: str):
        """Update heartbeat for a subscriber"""
        with self._lock:
            subscription_id = self._subscriber_to_subscription.get(subscriber_id)
            if not subscription_id:
                return
            
            subscription = self._subscriptions.get(subscription_id)
            if subscription and subscriber_id in subscription.subscribers:
                subscription.subscribers[subscriber_id].last_heartbeat = time.time()
    
    def get_subscriptions_for_topic(self, topic: str) -> List[Subscription]:
        """Get all subscriptions for a topic"""
        with self._lock:
            subscription_ids = self._topic_subscriptions.get(topic, set())
            return [self._subscriptions[sid] for sid in subscription_ids if sid in self._subscriptions]
    
    def deliver_message_to_topic(self, topic: str, message: Message, topic_log) -> int:
        """Deliver a message to all subscriptions of a topic"""
        delivered_count = 0
        subscriptions = self.get_subscriptions_for_topic(topic)
        
        for subscription in subscriptions:
            if self._deliver_message_to_subscription(subscription, message, topic_log):
                delivered_count += 1
        
        return delivered_count
    
    def _deliver_message_to_subscription(self, subscription: Subscription, message: Message, topic_log) -> bool:
        """Deliver a message to a specific subscription using round-robin"""
        with self._lock:
            # Check if this subscription should receive this message
            if message.sequence_number < subscription.current_offset:
                return False  # Already processed
            
            if message.sequence_number > subscription.current_offset:
                # There's a gap, we need to catch up
                logger.warning(f"Gap detected in subscription {subscription.subscription_id}: "
                             f"expected {subscription.current_offset}, got {message.sequence_number}")
                return False
            
            # Get next subscriber in round-robin
            subscriber = subscription.get_next_subscriber()
            if not subscriber:
                logger.warning(f"No active subscribers for subscription {subscription.subscription_id}")
                return False
            
            # Try to deliver using sendfile for zero-copy
            try:
                file_info = topic_log.get_message_file_info(message.sequence_number)
                if file_info:
                    position, length = file_info
                    with open(topic_log.get_log_file_path(), 'rb') as log_file:
                        sent = subscriber.socket.sendfile(log_file, position, length)
                        if sent == length:
                            # Successfully delivered, advance offset
                            subscription.current_offset += 1
                            logger.debug(f"Delivered message {message.sequence_number} to subscriber {subscriber.subscriber_id} "
                                       f"in subscription {subscription.subscription_id}")
                            return True
                        else:
                            logger.warning(f"Partial send to subscriber {subscriber.subscriber_id}: {sent}/{length}")
                            subscriber.active = False
                            return False
                else:
                    logger.error(f"Could not get file info for message {message.sequence_number}")
                    return False
                    
            except Exception as e:
                logger.error(f"Failed to deliver message to subscriber {subscriber.subscriber_id}: {e}")
                subscriber.active = False
                return False
    
    def _cleanup_worker(self):
        """Background worker to cleanup inactive subscribers"""
        while self._running:
            try:
                time.sleep(30)  # Check every 30 seconds
                self._cleanup_inactive_subscribers()
            except Exception as e:
                logger.error(f"Error in subscription cleanup: {e}")
    
    def _cleanup_inactive_subscribers(self):
        """Remove subscribers that haven't sent heartbeats"""
        with self._lock:
            now = time.time()
            subscribers_to_remove = []
            
            for subscription in self._subscriptions.values():
                for subscriber_id, subscriber in subscription.subscribers.items():
                    if now - subscriber.last_heartbeat > self._heartbeat_timeout:
                        subscribers_to_remove.append(subscriber_id)
                        logger.info(f"Removing inactive subscriber {subscriber_id} "
                                  f"(last heartbeat: {now - subscriber.last_heartbeat:.1f}s ago)")
            
            for subscriber_id in subscribers_to_remove:
                self._remove_subscriber_internal(subscriber_id)
    
    def get_subscription_stats(self, subscription_id: str) -> Optional[Dict]:
        """Get statistics for a subscription"""
        with self._lock:
            subscription = self._subscriptions.get(subscription_id)
            if not subscription:
                return None
            
            return {
                'subscription_id': subscription_id,
                'topic': subscription.topic,
                'current_offset': subscription.current_offset,
                'active_subscribers': subscription.get_active_subscriber_count(),
                'total_subscribers': len(subscription.subscribers),
                'created_at': subscription.created_at,
                'subscribers': [
                    {
                        'subscriber_id': s.subscriber_id,
                        'connection_id': s.connection_id,
                        'active': s.active,
                        'last_heartbeat': s.last_heartbeat
                    }
                    for s in subscription.subscribers.values()
                ]
            }
    
    def get_all_subscription_stats(self) -> List[Dict]:
        """Get statistics for all subscriptions"""
        with self._lock:
            return [self.get_subscription_stats(sid) for sid in self._subscriptions.keys()]
