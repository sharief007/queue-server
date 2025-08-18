"""
Main message broker implementation.
Coordinates between storage, topics, and protocol layers.
"""
import logging
import threading
import time
from typing import Dict, List, Optional, Any

from .storage import StorageManager
from .topic import TopicManager, TopicInfo, Subscription
from .protocol import ProtocolHandler, ClientConnection
from .message import Message, MessageType, MessageBuilder
from .config import get_config


logger = logging.getLogger(__name__)


class MessageBroker:
    """Main message broker coordinating all components"""
    
    def __init__(self):
        self.config = get_config()
        
        # Initialize components
        self.storage = StorageManager()
        self.topic_manager = TopicManager(self.storage)
        self.protocol_handler = ProtocolHandler()
        
        self._running = False
        self._lock = threading.RLock()
        
        # Track client info for cleanup
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
            self.storage.start()
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
            self.storage.stop()
            
            logger.info("Message broker stopped")
    
    def add_client_connection(self, client_socket, address) -> ClientConnection:
        """Add a new client connection"""
        connection = self.protocol_handler.add_connection(client_socket, address)
        
        # Initialize client info
        self._client_info[connection.connection_id] = {
            'address': address,
            'connected_at': time.time(),
            'client_id': None,  # Will be set during authentication/registration
        }
        
        return connection
    
    def remove_client_connection(self, connection_id: str) -> None:
        """Remove a client connection and cleanup subscriptions"""
        # Get client_id before removing connection
        client_info = self._client_info.get(connection_id, {})
        client_id = client_info.get('client_id')
        
        # Remove from protocol handler
        self.protocol_handler.remove_connection(connection_id)
        
        # Cleanup subscriptions if client was registered
        if client_id:
            try:
                self.topic_manager.unsubscribe_client(client_id)
            except Exception as e:
                logger.warning(f"Error cleaning up subscriptions for {client_id}: {e}")
        
        # Remove client info
        if connection_id in self._client_info:
            del self._client_info[connection_id]
        
        logger.info(f"Cleaned up client connection {connection_id}")
    
    def _register_protocol_handlers(self) -> None:
        """Register message handlers with the protocol handler"""
        handlers = {
            MessageType.CREATE_TOPIC: self._handle_create_topic,
            MessageType.PUBLISH: self._handle_publish,
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
            
            partition_count = int(message.properties.get('partitions', self.config.get('topics.default_partitions')))
            retention_hours = int(message.properties.get('retention_hours', self.config.get('topics.retention_hours')))
            
            # Create topic
            topic_info = self.topic_manager.create_topic(
                name=topic_name,
                partition_count=partition_count,
                retention_hours=retention_hours
            )
            
            # Return success response
            return (MessageBuilder(MessageType.ACK)
                   .property('status', 'success')
                   .property('topic', topic_name)
                   .property('partitions', str(partition_count))
                   .sequence_number(message.sequence_number)
                   .build())
        
        except Exception as e:
            logger.error(f"Create topic error: {e}")
            return self._create_error_response(str(e), message.sequence_number)
    
    def _handle_publish(self, connection: ClientConnection, message: Message) -> Optional[Message]:
        """Handle message publish request"""
        try:
            topic_name = message.properties.get('topic')
            if not topic_name:
                return self._create_error_response("Missing 'topic' property", message.sequence_number)
            
            # Get partition if specified
            partition = None
            if 'partition' in message.properties:
                partition = int(message.properties['partition'])
            
            # Create data message from the body
            data_message = MessageBuilder(MessageType.DATA).body(message.body).properties(message.properties).build()
            
            # Publish to topic
            partition_offset = self.topic_manager.publish_message(topic_name, data_message, partition)
            
            # Return success response
            return (MessageBuilder(MessageType.ACK)
                   .property('status', 'success')
                   .property('topic', topic_name)
                   .property('partition', str(partition_offset.partition))
                   .property('offset', str(partition_offset.offset))
                   .sequence_number(message.sequence_number)
                   .build())
        
        except Exception as e:
            logger.error(f"Publish error: {e}")
            return self._create_error_response(str(e), message.sequence_number)
    
    def _handle_subscribe(self, connection: ClientConnection, message: Message) -> Optional[Message]:
        """Handle subscription request"""
        try:
            topic_name = message.properties.get('topic')
            client_id = message.properties.get('client_id')
            
            if not topic_name or not client_id:
                return self._create_error_response("Missing 'topic' or 'client_id' property", message.sequence_number)
            
            # Update client info
            self._client_info[connection.connection_id]['client_id'] = client_id
            
            partition = int(message.properties.get('partition', 0))
            start_offset = int(message.properties.get('offset', 0))
            auto_ack = message.properties.get('auto_ack', 'true').lower() == 'true'
            
            # Create subscription
            subscription = self.topic_manager.subscribe(
                topic=topic_name,
                client_id=client_id,
                partition=partition,
                start_offset=start_offset,
                auto_ack=auto_ack,
                callback=lambda msg: self._send_message_to_client(connection, msg)
            )
            
            # Return success response
            return (MessageBuilder(MessageType.ACK)
                   .property('status', 'success')
                   .property('topic', topic_name)
                   .property('partition', str(partition))
                   .property('client_id', client_id)
                   .sequence_number(message.sequence_number)
                   .build())
        
        except Exception as e:
            logger.error(f"Subscribe error: {e}")
            return self._create_error_response(str(e), message.sequence_number)
    
    def _handle_unsubscribe(self, connection: ClientConnection, message: Message) -> Optional[Message]:
        """Handle unsubscribe request"""
        try:
            topic_name = message.properties.get('topic')
            client_id = message.properties.get('client_id')
            
            if not topic_name or not client_id:
                return self._create_error_response("Missing 'topic' or 'client_id' property", message.sequence_number)
            
            partition = int(message.properties.get('partition', 0))
            
            # Unsubscribe
            self.topic_manager.unsubscribe(topic_name, client_id, partition)
            
            # Return success response
            return (MessageBuilder(MessageType.ACK)
                   .property('status', 'success')
                   .property('topic', topic_name)
                   .property('partition', str(partition))
                   .property('client_id', client_id)
                   .sequence_number(message.sequence_number)
                   .build())
        
        except Exception as e:
            logger.error(f"Unsubscribe error: {e}")
            return self._create_error_response(str(e), message.sequence_number)
    
    def _handle_heartbeat(self, connection: ClientConnection, message: Message) -> Optional[Message]:
        """Handle heartbeat message"""
        # Just return a heartbeat response
        return MessageBuilder(MessageType.HEARTBEAT).sequence_number(message.sequence_number).build()
    
    def _send_message_to_client(self, connection: ClientConnection, message: Message) -> None:
        """Send a message to a client (used by subscription callbacks)"""
        try:
            connection.send_message(message)
        except Exception as e:
            logger.warning(f"Failed to send message to client {connection.connection_id}: {e}")
    
    def _create_error_response(self, error_message: str, sequence_number: int = 0) -> Message:
        """Create an error response message"""
        return (MessageBuilder(MessageType.ACK)
               .property('status', 'error')
               .property('error', error_message)
               .sequence_number(sequence_number)
               .build())
    
    # Management APIs
    
    def create_topic(self, name: str, partition_count: int = None, **kwargs) -> TopicInfo:
        """Create a topic programmatically"""
        return self.topic_manager.create_topic(name, partition_count, **kwargs)
    
    def list_topics(self) -> List[TopicInfo]:
        """List all topics"""
        return self.topic_manager.list_topics()
    
    def get_topic_stats(self, topic: str) -> Dict[str, Any]:
        """Get topic statistics"""
        return self.topic_manager.get_topic_stats(topic)
    
    def get_broker_stats(self) -> Dict[str, Any]:
        """Get overall broker statistics"""
        topics = self.list_topics()
        protocol_stats = self.protocol_handler.get_stats()
        
        total_messages = 0
        total_partitions = 0
        total_subscribers = 0
        
        for topic in topics:
            try:
                stats = self.get_topic_stats(topic.name)
                total_messages += stats['total_messages']
                total_partitions += stats['partition_count']
                total_subscribers += stats['subscribers']
            except Exception:
                pass
        
        return {
            'topics': len(topics),
            'total_partitions': total_partitions,
            'total_messages': total_messages,
            'total_subscribers': total_subscribers,
            'active_connections': protocol_stats['active_connections'],
            'uptime': time.time() - (min([info.get('connected_at', time.time()) 
                                        for info in self._client_info.values()]) 
                                   if self._client_info else time.time()),
            'storage_dir': self.config.get('storage.data_dir'),
        }
