"""
Custom TCP protocol implementation for the message broker.
Handles persistent connections, heartbeats, and message framing.
"""
import socket
import struct
import threading
import logging
import time
from typing import Dict, Optional, Callable, Any
from enum import IntEnum

from .message import Message, MessageType, MessageBuilder
from .config import get_config


logger = logging.getLogger(__name__)


class ProtocolError(Exception):
    """Protocol-related errors"""
    pass


class ConnectionState(IntEnum):
    """Connection states"""
    CONNECTING = 1
    CONNECTED = 2
    DISCONNECTING = 3
    DISCONNECTED = 4


class ClientConnection:
    """Represents a client connection with the TCP protocol"""
    
    def __init__(self, socket: socket.socket, address: tuple, connection_id: str):
        self.socket = socket
        self.address = address
        self.connection_id = connection_id
        self.state = ConnectionState.CONNECTED
        
        self.created_at = time.time()
        self.last_heartbeat = time.time()
        self.last_activity = time.time()
        
        self._lock = threading.RLock()
        self._send_lock = threading.Lock()
        
        # Protocol settings
        self.config = get_config()
        self.heartbeat_interval = self.config.get('server.heartbeat_interval', 30)
        self.connection_timeout = self.config.get('server.connection_timeout', 300)
        
        logger.info(f"New client connection {connection_id} from {address}")
    
    def send_message(self, message: Message) -> None:
        """Send a message to the client"""
        if self.state != ConnectionState.CONNECTED:
            raise ProtocolError(f"Cannot send message: connection {self.connection_id} not connected")
        
        try:
            with self._send_lock:
                data = message.serialize()
                self.socket.sendall(data)
                self.last_activity = time.time()
                
        except Exception as e:
            logger.error(f"Failed to send message to {self.connection_id}: {e}")
            self.state = ConnectionState.DISCONNECTED
            raise ProtocolError(f"Send failed: {e}")
    
    def receive_message(self, timeout: Optional[float] = None) -> Optional[Message]:
        """Receive a message from the client"""
        if self.state != ConnectionState.CONNECTED:
            return None
        
        try:
            # Set socket timeout
            if timeout is not None:
                self.socket.settimeout(timeout)
            
            # First, read the message header to get the total length
            header_data = self._recv_exact(4)  # First 4 bytes contain total length
            if not header_data:
                return None
            
            total_length = struct.unpack('>I', header_data)[0]
            
            # Read the rest of the message
            remaining_data = self._recv_exact(total_length - 4)
            if not remaining_data:
                return None
            
            # Combine and deserialize
            full_data = header_data + remaining_data
            message = Message.deserialize(full_data)
            
            self.last_activity = time.time()
            return message
            
        except socket.timeout:
            return None
        except Exception as e:
            logger.error(f"Failed to receive message from {self.connection_id}: {e}")
            self.state = ConnectionState.DISCONNECTED
            return None
        finally:
            # Reset socket timeout
            self.socket.settimeout(None)
    
    def _recv_exact(self, length: int) -> Optional[bytes]:
        """Receive exactly the specified number of bytes"""
        data = b''
        while len(data) < length:
            try:
                chunk = self.socket.recv(length - len(data))
                if not chunk:
                    return None
                data += chunk
            except Exception:
                return None
        return data
    
    def send_heartbeat(self) -> None:
        """Send heartbeat message"""
        try:
            heartbeat = MessageBuilder(MessageType.HEARTBEAT).build()
            self.send_message(heartbeat)
            self.last_heartbeat = time.time()
        except Exception as e:
            logger.warning(f"Failed to send heartbeat to {self.connection_id}: {e}")
    
    def is_alive(self) -> bool:
        """Check if connection is still alive"""
        if self.state != ConnectionState.CONNECTED:
            return False
        
        now = time.time()
        return (now - self.last_activity) < self.connection_timeout
    
    def close(self) -> None:
        """Close the connection"""
        with self._lock:
            if self.state == ConnectionState.DISCONNECTED:
                return
            
            self.state = ConnectionState.DISCONNECTING
            
            try:
                self.socket.close()
            except Exception:
                pass
            
            self.state = ConnectionState.DISCONNECTED
            logger.info(f"Closed connection {self.connection_id}")


class ProtocolHandler:
    """Handles the custom TCP protocol"""
    
    def __init__(self):
        self.config = get_config()
        self._connections: Dict[str, ClientConnection] = {}
        self._connection_counter = 0
        self._lock = threading.RLock()
        
        # Message handlers
        self._message_handlers: Dict[MessageType, Callable[[ClientConnection, Message], Optional[Message]]] = {}
        
        # Heartbeat thread
        self._heartbeat_thread: Optional[threading.Thread] = None
        self._running = False
    
    def start(self) -> None:
        """Start the protocol handler"""
        with self._lock:
            if self._running:
                return
            
            self._running = True
            self._heartbeat_thread = threading.Thread(
                target=self._heartbeat_worker,
                daemon=True,
                name="ProtocolHeartbeat"
            )
            self._heartbeat_thread.start()
            
            logger.info("Protocol handler started")
    
    def stop(self) -> None:
        """Stop the protocol handler"""
        with self._lock:
            self._running = False
            
            # Close all connections
            connections = list(self._connections.values())
            for connection in connections:
                connection.close()
            
            self._connections.clear()
            
            if self._heartbeat_thread and self._heartbeat_thread.is_alive():
                self._heartbeat_thread.join(timeout=5)
            
            logger.info("Protocol handler stopped")
    
    def register_handler(self, message_type: MessageType, handler: Callable[[ClientConnection, Message], Optional[Message]]) -> None:
        """Register a message handler"""
        self._message_handlers[message_type] = handler
        logger.debug(f"Registered handler for {message_type.name}")
    
    def add_connection(self, client_socket: socket.socket, address: tuple) -> ClientConnection:
        """Add a new client connection"""
        with self._lock:
            connection_id = f"conn_{self._connection_counter}"
            self._connection_counter += 1
            
            connection = ClientConnection(client_socket, address, connection_id)
            self._connections[connection_id] = connection
            
            return connection
    
    def remove_connection(self, connection_id: str) -> None:
        """Remove a client connection"""
        with self._lock:
            if connection_id in self._connections:
                connection = self._connections[connection_id]
                connection.close()
                del self._connections[connection_id]
                logger.info(f"Removed connection {connection_id}")
    
    def get_connection(self, connection_id: str) -> Optional[ClientConnection]:
        """Get a connection by ID"""
        with self._lock:
            return self._connections.get(connection_id)
    
    def get_connection_count(self) -> int:
        """Get number of active connections"""
        with self._lock:
            return len(self._connections)
    
    def handle_message(self, connection: ClientConnection, message: Message) -> Optional[Message]:
        """Handle an incoming message"""
        logger.debug(f"Handling {message.message_type.name} from {connection.connection_id}")
        
        # Find handler
        handler = self._message_handlers.get(message.message_type)
        if handler is None:
            logger.warning(f"No handler for message type {message.message_type.name}")
            return self._create_error_response("Unknown message type", message.sequence_number)
        
        try:
            return handler(connection, message)
        except Exception as e:
            logger.error(f"Handler error for {message.message_type.name}: {e}")
            return self._create_error_response(str(e), message.sequence_number)
    
    def broadcast_message(self, message: Message, exclude_connection: Optional[str] = None) -> None:
        """Broadcast a message to all connected clients"""
        with self._lock:
            for connection_id, connection in self._connections.items():
                if exclude_connection and connection_id == exclude_connection:
                    continue
                
                try:
                    connection.send_message(message)
                except Exception as e:
                    logger.warning(f"Failed to broadcast to {connection_id}: {e}")
    
    def _heartbeat_worker(self) -> None:
        """Background worker for heartbeats and connection cleanup"""
        while self._running:
            try:
                self._send_heartbeats()
                self._cleanup_dead_connections()
                time.sleep(self.config.get('server.heartbeat_interval', 30))
            except Exception as e:
                logger.error(f"Heartbeat worker error: {e}")
    
    def _send_heartbeats(self) -> None:
        """Send heartbeats to all connections"""
        with self._lock:
            for connection in list(self._connections.values()):
                try:
                    connection.send_heartbeat()
                except Exception as e:
                    logger.warning(f"Heartbeat failed for {connection.connection_id}: {e}")
    
    def _cleanup_dead_connections(self) -> None:
        """Remove dead connections"""
        with self._lock:
            dead_connections = []
            
            for connection_id, connection in self._connections.items():
                if not connection.is_alive():
                    dead_connections.append(connection_id)
            
            for connection_id in dead_connections:
                self.remove_connection(connection_id)
                logger.info(f"Cleaned up dead connection {connection_id}")
    
    def _create_error_response(self, error_message: str, sequence_number: int = 0) -> Message:
        """Create an error response message"""
        return (MessageBuilder(MessageType.ACK)
                .property('error', error_message)
                .property('status', 'error')
                .sequence_number(sequence_number)
                .build())
    
    def get_stats(self) -> Dict[str, Any]:
        """Get protocol statistics"""
        with self._lock:
            connections = list(self._connections.values())
            
            return {
                'active_connections': len(connections),
                'total_connections': self._connection_counter,
                'connections': [
                    {
                        'id': conn.connection_id,
                        'address': conn.address,
                        'created_at': conn.created_at,
                        'last_activity': conn.last_activity,
                        'state': conn.state.name
                    }
                    for conn in connections
                ]
            }
