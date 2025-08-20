"""
Main message broker implementation.
Coordinates Publishers, Subscribers, and Subscriptions.
"""
import threading
import time
import logging
from typing import Dict, List, Optional, Any

from .topic_storage import TopicStorageManager
from .subscription import SubscriptionManager, Subscription
from .protocol import ProtocolHandler, ClientConnection
from .message import Message, MessageType, MessageBuilder
from .config import get_config


logger = logging.getLogger(__name__)


class Publisher:
    """Represents a publisher client"""
    
    def __init__(self, publisher_id: str, connection: ClientConnection):
        self.publisher_id = publisher_id
        self.connection = connection
        self.created_at = time.time()
        self.last_activity = time.time()
    
    def update_activity(self):
        """Update last activity timestamp"""
        self.last_activity = time.time()


class MessageBroker:
    """Main message broker coordinating all components"""
    
    def __init__(self):
        self.config = get_config()
        
        # Initialize components
        self.storage = TopicStorageManager()
        self.subscription_manager = SubscriptionManager()
        self.protocol_handler = ProtocolHandler()
        
        self._running = False
        self._lock = threading.RLock()
        
        # Track publishers and connections
        self._publishers: Dict[str, Publisher] = {}  # publisher_id -> Publisher
        self._connection_to_publisher: Dict[str, str] = {}  # connection_id -> publisher_id
        self._client_info: Dict[str, Dict[str, Any]] = {}  # connection_id -> client_info
        
        # Register protocol message handlers
        self._register_protocol_handlers()
    
    def start(self) -> None:
        """Start the message broker"""
        with self._lock:
            if self._running:
                return
            
            logger.info("Starting message broker...")
            
            # Start components
            self.subscription_manager.start()
            self.protocol_handler.start()
            
            self._running = True
            
            logger.info("Message broker started successfully")
    
    def stop(self) -> None:
        """Stop the message broker"""
        with self._lock:
            if not self._running:
                return
            
            logger.info("Stopping message broker...")
            
            self._running = False
            
            # Stop components
            self.protocol_handler.stop()
            self.subscription_manager.stop()
            self.storage.close_all()
            
            logger.info("Message broker stopped")
    
    def add_client_connection(self, client_socket, address) -> ClientConnection:
        """Add a new client connection"""
        connection = self.protocol_handler.add_connection(client_socket, address)
        
        # Initialize client info
        self._client_info[connection.connection_id] = {
            'address': address,
            'connected_at': time.time(),
            'client_type': None,  # 'publisher' or 'subscriber'
            'client_id': None,
        }
        
        return connection
    
    def remove_client_connection(self, connection_id: str) -> None:
        """Remove a client connection and cleanup"""
        # Get client info before removing connection
        client_info = self._client_info.get(connection_id, {})
        client_type = client_info.get('client_type')
        client_id = client_info.get('client_id')
        
        # Remove from protocol handler
        self.protocol_handler.remove_connection(connection_id)
        
        # Cleanup based on client type
        if client_type == 'publisher' and client_id:
            self._remove_publisher(client_id)
        elif client_type == 'subscriber':
            self.subscription_manager.remove_subscriber_by_connection(connection_id)
        
        # Remove client info
        if connection_id in self._client_info:
            del self._client_info[connection_id]
        
        logger.info(f"Cleaned up client connection {connection_id}")
    
    def _register_protocol_handlers(self) -> None:
        """Register message handlers with the protocol handler"""
        handlers = {
            MessageType.CREATE_TOPIC: self._handle_create_topic,
            MessageType.PUBLISH: self._handle_publish,
            MessageType.CREATE_SUBSCRIPTION: self._handle_create_subscription,
            MessageType.DELETE_SUBSCRIPTION: self._handle_delete_subscription,
            MessageType.SUBSCRIBE: self._handle_subscribe,
            MessageType.UNSUBSCRIBE: self._handle_unsubscribe,
            MessageType.HEARTBEAT: self._handle_heartbeat,
        }
        
        for message_type, handler in handlers.items():
            self.protocol_handler.register_handler(message_type, handler)
    
    def _handle_create_topic(self, connection: ClientConnection, message: Message) -> Optional[Message]:
        """Handle topic creation request"""
        try:
            topic_name = message.properties.get('topic')
            if not topic_name:
                return self._create_error_response("Missing 'topic' property", message.sequence_number)
            
            # Create topic
            self.storage.create_topic(topic_name)
            
            # Return success response
            return (MessageBuilder(MessageType.ACK)
                   .property('status', 'success')
                   .property('topic', topic_name)
                   .sequence_number(message.sequence_number)
                   .build())
        
        except Exception as e:
            logger.error(f"Create topic error: {e}")
            return self._create_error_response(str(e), message.sequence_number)
    
    def _handle_publish(self, connection: ClientConnection, message: Message) -> Optional[Message]:
        """Handle message publish request"""
        try:
            topic_name = message.properties.get('topic')
            publisher_id = message.properties.get('publisher_id')
            
            if not topic_name:
                return self._create_error_response("Missing 'topic' property", message.sequence_number)
            
            if not publisher_id:
                return self._create_error_response("Missing 'publisher_id' property", message.sequence_number)
            
            # Register publisher if not already registered
            if publisher_id not in self._publishers:
                self._register_publisher(publisher_id, connection)
            
            # Update publisher activity
            self._publishers[publisher_id].update_activity()
            
            # Create data message from the request
            data_message = MessageBuilder(MessageType.DATA)\
                .body(message.body)\
                .properties(message.properties)\
                .build()
            
            # Store message to disk
            message_offset = self.storage.append_message(topic_name, data_message)
            
            # Deliver to all subscriptions for this topic
            topic_log = self.storage.get_topic(topic_name)
            delivered_count = self.subscription_manager.deliver_message_to_topic(
                topic_name, data_message, topic_log
            )
            
            # Return success response
            return (MessageBuilder(MessageType.ACK)
                   .property('status', 'success')
                   .property('topic', topic_name)
                   .property('offset', str(message_offset.offset))
                   .property('delivered_to', str(delivered_count))
                   .sequence_number(message.sequence_number)
                   .build())
        
        except Exception as e:
            logger.error(f"Publish error: {e}")
            return self._create_error_response(str(e), message.sequence_number)
    
    def _handle_create_subscription(self, connection: ClientConnection, message: Message) -> Optional[Message]:
        """Handle subscription creation request"""
        try:
            subscription_id = message.properties.get('subscription_id')
            topic_name = message.properties.get('topic')
            start_offset = int(message.properties.get('start_offset', 0))
            
            if not subscription_id:
                return self._create_error_response("Missing 'subscription_id' property", message.sequence_number)
            
            if not topic_name:
                return self._create_error_response("Missing 'topic' property", message.sequence_number)
            
            # Create subscription
            subscription = self.subscription_manager.create_subscription(
                subscription_id, topic_name, start_offset
            )
            
            return (MessageBuilder(MessageType.ACK)
                   .property('status', 'success')
                   .property('subscription_id', subscription_id)
                   .property('topic', topic_name)
                   .property('start_offset', str(start_offset))
                   .sequence_number(message.sequence_number)
                   .build())
        
        except Exception as e:
            logger.error(f"Create subscription error: {e}")
            return self._create_error_response(str(e), message.sequence_number)
    
    def _handle_delete_subscription(self, connection: ClientConnection, message: Message) -> Optional[Message]:
        """Handle subscription deletion request"""
        try:
            subscription_id = message.properties.get('subscription_id')
            
            if not subscription_id:
                return self._create_error_response("Missing 'subscription_id' property", message.sequence_number)
            
            # Delete subscription
            self.subscription_manager.delete_subscription(subscription_id)
            
            return (MessageBuilder(MessageType.ACK)
                   .property('status', 'success')
                   .property('subscription_id', subscription_id)
                   .sequence_number(message.sequence_number)
                   .build())
        
        except Exception as e:
            logger.error(f"Delete subscription error: {e}")
            return self._create_error_response(str(e), message.sequence_number)
    
    def _handle_subscribe(self, connection: ClientConnection, message: Message) -> Optional[Message]:
        """Handle subscriber join request"""
        try:
            subscription_id = message.properties.get('subscription_id')
            subscriber_id = message.properties.get('subscriber_id')
            
            if not subscription_id:
                return self._create_error_response("Missing 'subscription_id' property", message.sequence_number)
            
            if not subscriber_id:
                return self._create_error_response("Missing 'subscriber_id' property", message.sequence_number)
            
            # Add subscriber to subscription
            self.subscription_manager.add_subscriber(
                subscription_id, subscriber_id, connection.connection_id, connection.socket
            )
            
            # Update client info
            self._client_info[connection.connection_id]['client_type'] = 'subscriber'
            self._client_info[connection.connection_id]['client_id'] = subscriber_id
            
            return (MessageBuilder(MessageType.ACK)
                   .property('status', 'success')
                   .property('subscription_id', subscription_id)
                   .property('subscriber_id', subscriber_id)
                   .sequence_number(message.sequence_number)
                   .build())
        
        except Exception as e:
            logger.error(f"Subscribe error: {e}")
            return self._create_error_response(str(e), message.sequence_number)
    
    def _handle_unsubscribe(self, connection: ClientConnection, message: Message) -> Optional[Message]:
        """Handle subscriber leave request"""
        try:
            subscriber_id = message.properties.get('subscriber_id')
            
            if not subscriber_id:
                return self._create_error_response("Missing 'subscriber_id' property", message.sequence_number)
            
            # Remove subscriber
            self.subscription_manager.remove_subscriber(subscriber_id)
            
            return (MessageBuilder(MessageType.ACK)
                   .property('status', 'success')
                   .property('subscriber_id', subscriber_id)
                   .sequence_number(message.sequence_number)
                   .build())
        
        except Exception as e:
            logger.error(f"Unsubscribe error: {e}")
            return self._create_error_response(str(e), message.sequence_number)
    
    def _handle_heartbeat(self, connection: ClientConnection, message: Message) -> Optional[Message]:
        """Handle heartbeat message"""
        # Update heartbeat for subscriber if applicable
        client_info = self._client_info.get(connection.connection_id, {})
        if client_info.get('client_type') == 'subscriber':
            subscriber_id = client_info.get('client_id')
            if subscriber_id:
                self.subscription_manager.heartbeat_subscriber(subscriber_id)
        
        # Return heartbeat response
        return MessageBuilder(MessageType.HEARTBEAT).sequence_number(message.sequence_number).build()
    
    def _register_publisher(self, publisher_id: str, connection: ClientConnection):
        """Register a new publisher"""
        publisher = Publisher(publisher_id, connection)
        self._publishers[publisher_id] = publisher
        self._connection_to_publisher[connection.connection_id] = publisher_id
        
        # Update client info
        self._client_info[connection.connection_id]['client_type'] = 'publisher'
        self._client_info[connection.connection_id]['client_id'] = publisher_id
        
        logger.info(f"Registered publisher {publisher_id}")
    
    def _remove_publisher(self, publisher_id: str):
        """Remove a publisher"""
        if publisher_id in self._publishers:
            publisher = self._publishers[publisher_id]
            if publisher.connection.connection_id in self._connection_to_publisher:
                del self._connection_to_publisher[publisher.connection.connection_id]
            del self._publishers[publisher_id]
            logger.info(f"Removed publisher {publisher_id}")
    
    def _create_error_response(self, error_message: str, sequence_number: int = 0) -> Message:
        """Create an error response message"""
        return (MessageBuilder(MessageType.ACK)
               .property('status', 'error')
               .property('error', error_message)
               .sequence_number(sequence_number)
               .build())
    
    # Management APIs
    
    def create_topic(self, name: str) -> None:
        """Create a topic programmatically"""
        self.storage.create_topic(name)
    
    def list_topics(self) -> List[str]:
        """List all topics"""
        return self.storage.list_topics()
    
    def create_subscription(self, subscription_id: str, topic: str, start_offset: int = 0) -> Subscription:
        """Create a subscription programmatically"""
        return self.subscription_manager.create_subscription(subscription_id, topic, start_offset)
    
    def get_broker_stats(self) -> Dict[str, Any]:
        """Get overall broker statistics"""
        topics = self.list_topics()
        subscription_stats = self.subscription_manager.get_all_subscription_stats()
        protocol_stats = self.protocol_handler.get_stats()
        
        return {
            'topics': len(topics),
            'topic_list': topics,
            'publishers': len(self._publishers),
            'subscriptions': len(subscription_stats),
            'subscription_stats': subscription_stats,
            'active_connections': protocol_stats['active_connections'],
            'storage_dir': self.config.get('storage.data_dir'),
        }
