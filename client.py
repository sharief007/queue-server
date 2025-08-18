"""
Client library and CLI for the message broker.
"""
import socket
import threading
import time
import logging
import argparse
import json
from typing import Optional, Callable, List, Dict, Any

from core.message import Message, MessageType, MessageBuilder
from core.config import Config


logger = logging.getLogger(__name__)


class BrokerClient:
    """Client for connecting to the message broker"""
    
    def __init__(self, host: str = '127.0.0.1', port: int = 9999, client_id: str = None):
        self.host = host
        self.port = port
        self.client_id = client_id or f"client_{int(time.time())}"
        
        self._socket: Optional[socket.socket] = None
        self._connected = False
        self._sequence_number = 0
        self._lock = threading.RLock()
        
        # Message handling
        self._response_handlers: Dict[int, threading.Event] = {}
        self._responses: Dict[int, Message] = {}
        self._subscription_handlers: Dict[str, Callable[[Message], None]] = {}
        
        # Background threads
        self._receive_thread: Optional[threading.Thread] = None
        self._running = False
    
    def connect(self) -> None:
        """Connect to the broker"""
        if self._connected:
            return
        
        try:
            self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self._socket.connect((self.host, self.port))
            
            self._connected = True
            self._running = True
            
            # Start receive thread
            self._receive_thread = threading.Thread(
                target=self._receive_worker,
                daemon=True,
                name=f"ClientReceive-{self.client_id}"
            )
            self._receive_thread.start()
            
            logger.info(f"Connected to broker at {self.host}:{self.port}")
            
        except Exception as e:
            self._connected = False
            logger.error(f"Failed to connect to broker: {e}")
            raise
    
    def disconnect(self) -> None:
        """Disconnect from the broker"""
        if not self._connected:
            return
        
        self._running = False
        self._connected = False
        
        try:
            if self._socket:
                self._socket.close()
        except Exception:
            pass
        
        if self._receive_thread and self._receive_thread.is_alive():
            self._receive_thread.join(timeout=5)
        
        logger.info("Disconnected from broker")
    
    def create_topic(self, topic: str, partitions: int = 1, retention_hours: int = 24) -> bool:
        """Create a new topic"""
        message = (MessageBuilder(MessageType.CREATE_TOPIC)
                  .property('topic', topic)
                  .property('partitions', str(partitions))
                  .property('retention_hours', str(retention_hours))
                  .sequence_number(self._get_next_sequence())
                  .build())
        
        response = self._send_and_wait(message)
        return response and response.properties.get('status') == 'success'
    
    def publish(self, topic: str, data: bytes, partition: Optional[int] = None, **properties) -> bool:
        """Publish a message to a topic"""
        builder = (MessageBuilder(MessageType.PUBLISH)
                  .property('topic', topic)
                  .property('client_id', self.client_id)
                  .body(data)
                  .sequence_number(self._get_next_sequence()))
        
        if partition is not None:
            builder.property('partition', str(partition))
        
        for key, value in properties.items():
            builder.property(key, str(value))
        
        message = builder.build()
        response = self._send_and_wait(message)
        return response and response.properties.get('status') == 'success'
    
    def publish_text(self, topic: str, text: str, partition: Optional[int] = None, **properties) -> bool:
        """Publish a text message to a topic"""
        return self.publish(topic, text.encode('utf-8'), partition, **properties)
    
    def subscribe(self, 
                 topic: str, 
                 partition: int = 0,
                 offset: int = 0,
                 auto_ack: bool = True,
                 handler: Optional[Callable[[Message], None]] = None) -> bool:
        """Subscribe to a topic partition"""
        message = (MessageBuilder(MessageType.SUBSCRIBE)
                  .property('topic', topic)
                  .property('client_id', self.client_id)
                  .property('partition', str(partition))
                  .property('offset', str(offset))
                  .property('auto_ack', str(auto_ack).lower())
                  .sequence_number(self._get_next_sequence())
                  .build())
        
        response = self._send_and_wait(message)
        if response and response.properties.get('status') == 'success':
            # Register handler for this topic
            if handler:
                self._subscription_handlers[f"{topic}:{partition}"] = handler
            return True
        
        return False
    
    def unsubscribe(self, topic: str, partition: int = 0) -> bool:
        """Unsubscribe from a topic partition"""
        message = (MessageBuilder(MessageType.UNSUBSCRIBE)
                  .property('topic', topic)
                  .property('client_id', self.client_id)
                  .property('partition', str(partition))
                  .sequence_number(self._get_next_sequence())
                  .build())
        
        response = self._send_and_wait(message)
        if response and response.properties.get('status') == 'success':
            # Remove handler
            key = f"{topic}:{partition}"
            if key in self._subscription_handlers:
                del self._subscription_handlers[key]
            return True
        
        return False
    
    def _send_and_wait(self, message: Message, timeout: float = 30.0) -> Optional[Message]:
        """Send a message and wait for response"""
        if not self._connected:
            raise RuntimeError("Not connected to broker")
        
        seq_num = message.sequence_number
        
        # Setup response handling
        event = threading.Event()
        self._response_handlers[seq_num] = event
        
        try:
            # Send message
            self._send_message(message)
            
            # Wait for response
            if event.wait(timeout):
                return self._responses.pop(seq_num, None)
            else:
                logger.warning(f"Timeout waiting for response to message {seq_num}")
                return None
                
        finally:
            # Cleanup
            if seq_num in self._response_handlers:
                del self._response_handlers[seq_num]
            if seq_num in self._responses:
                del self._responses[seq_num]
    
    def _send_message(self, message: Message) -> None:
        """Send a message to the broker"""
        try:
            data = message.serialize()
            self._socket.sendall(data)
        except Exception as e:
            logger.error(f"Failed to send message: {e}")
            self._connected = False
            raise
    
    def _receive_worker(self) -> None:
        """Background worker for receiving messages"""
        while self._running and self._connected:
            try:
                message = self._receive_message()
                if message:
                    self._handle_received_message(message)
            except Exception as e:
                if self._running:
                    logger.error(f"Receive worker error: {e}")
                    self._connected = False
                break
    
    def _receive_message(self) -> Optional[Message]:
        """Receive a message from the broker"""
        try:
            # Set timeout
            self._socket.settimeout(1.0)
            
            # Read message header for length
            header_data = self._recv_exact(4)
            if not header_data:
                return None
            
            import struct
            total_length = struct.unpack('>I', header_data)[0]
            
            # Read rest of message
            remaining_data = self._recv_exact(total_length - 4)
            if not remaining_data:
                return None
            
            # Deserialize
            full_data = header_data + remaining_data
            return Message.deserialize(full_data)
            
        except socket.timeout:
            return None
        except Exception as e:
            if self._running:
                logger.error(f"Failed to receive message: {e}")
            return None
        finally:
            if self._socket:
                self._socket.settimeout(None)
    
    def _recv_exact(self, length: int) -> Optional[bytes]:
        """Receive exactly the specified number of bytes"""
        data = b''
        while len(data) < length:
            try:
                chunk = self._socket.recv(length - len(data))
                if not chunk:
                    return None
                data += chunk
            except Exception:
                return None
        return data
    
    def _handle_received_message(self, message: Message) -> None:
        """Handle a received message"""
        # Check if it's a response to a request
        seq_num = message.sequence_number
        if seq_num in self._response_handlers:
            self._responses[seq_num] = message
            self._response_handlers[seq_num].set()
            return
        
        # Check if it's a subscription message
        if message.message_type == MessageType.DATA:
            topic = message.properties.get('topic')
            partition = message.properties.get('partition', '0')
            
            if topic:
                key = f"{topic}:{partition}"
                handler = self._subscription_handlers.get(key)
                if handler:
                    try:
                        handler(message)
                    except Exception as e:
                        logger.error(f"Subscription handler error: {e}")
    
    def _get_next_sequence(self) -> int:
        """Get next sequence number"""
        with self._lock:
            self._sequence_number += 1
            return self._sequence_number
    
    def __enter__(self):
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.disconnect()


def print_message(message: Message) -> None:
    """Print a received message"""
    topic = message.properties.get('topic', 'unknown')
    partition = message.properties.get('partition', '0')
    offset = message.sequence_number
    
    body = message.body.decode('utf-8', errors='replace')
    
    print(f"[{topic}:{partition}@{offset}] {body}")
    
    # Print properties if any
    props = {k: v for k, v in message.properties.items() 
            if k not in ['topic', 'partition', 'created_at', 'version']}
    if props:
        print(f"  Properties: {props}")


def cli_main():
    """CLI entry point"""
    parser = argparse.ArgumentParser(description='Message Broker Client')
    parser.add_argument('--host', default='127.0.0.1', help='Broker host')
    parser.add_argument('--port', type=int, default=9999, help='Broker port')
    parser.add_argument('--client-id', help='Client ID')
    
    subparsers = parser.add_subparsers(dest='command', help='Commands')
    
    # Create topic command
    create_parser = subparsers.add_parser('create-topic', help='Create a topic')
    create_parser.add_argument('topic', help='Topic name')
    create_parser.add_argument('--partitions', type=int, default=1, help='Number of partitions')
    create_parser.add_argument('--retention', type=int, default=24, help='Retention hours')
    
    # Publish command
    pub_parser = subparsers.add_parser('publish', help='Publish a message')
    pub_parser.add_argument('topic', help='Topic name')
    pub_parser.add_argument('message', help='Message to publish')
    pub_parser.add_argument('--partition', type=int, help='Partition number')
    pub_parser.add_argument('--properties', help='Message properties (JSON)')
    
    # Subscribe command
    sub_parser = subparsers.add_parser('subscribe', help='Subscribe to a topic')
    sub_parser.add_argument('topic', help='Topic name')
    sub_parser.add_argument('--partition', type=int, default=0, help='Partition number')
    sub_parser.add_argument('--offset', type=int, default=0, help='Start offset')
    sub_parser.add_argument('--count', type=int, help='Max messages to receive')
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        return
    
    # Setup logging
    logging.basicConfig(level=logging.WARNING)
    
    try:
        with BrokerClient(args.host, args.port, args.client_id) as client:
            
            if args.command == 'create-topic':
                success = client.create_topic(args.topic, args.partitions, args.retention)
                if success:
                    print(f"Created topic '{args.topic}' with {args.partitions} partitions")
                else:
                    print(f"Failed to create topic '{args.topic}'")
            
            elif args.command == 'publish':
                properties = {}
                if args.properties:
                    properties = json.loads(args.properties)
                
                success = client.publish_text(args.topic, args.message, args.partition, **properties)
                if success:
                    print(f"Published message to topic '{args.topic}'")
                else:
                    print(f"Failed to publish message to topic '{args.topic}'")
            
            elif args.command == 'subscribe':
                print(f"Subscribing to {args.topic}:{args.partition} from offset {args.offset}")
                print("Press Ctrl+C to stop...")
                
                message_count = 0
                
                def handle_message(message: Message):
                    nonlocal message_count
                    print_message(message)
                    message_count += 1
                    
                    if args.count and message_count >= args.count:
                        return
                
                success = client.subscribe(args.topic, args.partition, args.offset, True, handle_message)
                if success:
                    try:
                        while True:
                            if args.count and message_count >= args.count:
                                break
                            time.sleep(0.1)
                    except KeyboardInterrupt:
                        print("\nStopping...")
                else:
                    print(f"Failed to subscribe to topic '{args.topic}'")
    
    except Exception as e:
        print(f"Error: {e}")


if __name__ == '__main__':
    cli_main()
