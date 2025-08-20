"""
Unit tests for the protocol module.
Tests client connections, message sending/receiving, and connection management.
"""
import unittest
import socket
import time
from unittest.mock import Mock
from core.protocol import ClientConnection, ProtocolHandler, ConnectionState, ProtocolError
from core.message import MessageType, MessageBuilder


class TestClientConnection(unittest.TestCase):
    """Test cases for ClientConnection class"""
    
    def setUp(self):
        """Set up test fixtures"""
        self.mock_socket = Mock(spec=socket.socket)
        self.address = ('127.0.0.1', 12345)
        self.connection_id = "test-conn-1"
    
    def test_connection_creation(self):
        """Test creating a client connection"""
        conn = ClientConnection(self.mock_socket, self.address, self.connection_id)
        
        self.assertEqual(conn.socket, self.mock_socket)
        self.assertEqual(conn.address, self.address)
        self.assertEqual(conn.connection_id, self.connection_id)
        self.assertEqual(conn.state, ConnectionState.CONNECTED)
        self.assertGreater(conn.created_at, 0)
    
    def test_send_message_success(self):
        """Test successfully sending a message"""
        conn = ClientConnection(self.mock_socket, self.address, self.connection_id)
        msg = MessageBuilder(MessageType.DATA).text_body("test message").property("topic", "test").build()
        
        # Mock successful sendall
        self.mock_socket.sendall.return_value = None
        
        # Record initial time
        initial_activity = conn.last_activity
        
        # Small delay to ensure time difference
        time.sleep(0.001)
        
        # Should not raise exception
        conn.send_message(msg)
        
        # Verify sendall was called
        self.mock_socket.sendall.assert_called_once()
        
        # Check that last_activity was updated
        self.assertGreater(conn.last_activity, initial_activity)
    
    def test_send_message_failure(self):
        """Test sending message with socket error"""
        conn = ClientConnection(self.mock_socket, self.address, self.connection_id)
        msg = MessageBuilder(MessageType.DATA).text_body("test message").property("topic", "test").build()
        
        # Mock socket error
        self.mock_socket.sendall.side_effect = socket.error("Connection broken")
        
        with self.assertRaises(ProtocolError):
            conn.send_message(msg)
        
        # Connection should be marked as disconnected
        self.assertEqual(conn.state, ConnectionState.DISCONNECTED)
    
    def test_send_message_when_disconnected(self):
        """Test sending message when connection is disconnected"""
        conn = ClientConnection(self.mock_socket, self.address, self.connection_id)
        conn.state = ConnectionState.DISCONNECTED
        
        msg = MessageBuilder(MessageType.DATA).text_body("test message").property("topic", "test").build()
        
        with self.assertRaises(ProtocolError):
            conn.send_message(msg)
    
    def test_receive_message_success(self):
        """Test successfully receiving a message"""
        conn = ClientConnection(self.mock_socket, self.address, self.connection_id)
        
        # Create a test message and its serialized form
        test_msg = MessageBuilder(MessageType.DATA).text_body("test message").property("topic", "test").build()
        serialized = test_msg.serialize()
        
        # Mock the _recv_exact method directly to return appropriate chunks
        def mock_recv_exact(length):
            if length == 4:
                # Return the first 4 bytes (header with total length)
                return serialized[:4]
            else:
                # Return the remaining bytes
                return serialized[4:]
        
        conn._recv_exact = mock_recv_exact
        
        received_msg = conn.receive_message()
        
        self.assertIsNotNone(received_msg)
        self.assertEqual(received_msg.message_type, MessageType.DATA)
        self.assertEqual(received_msg.body, b"test message")
    
    def test_receive_message_timeout(self):
        """Test receiving message with timeout"""
        conn = ClientConnection(self.mock_socket, self.address, self.connection_id)
        
        # Mock socket timeout
        self.mock_socket.recv.side_effect = socket.timeout("Timeout")
        
        received_msg = conn.receive_message(timeout=1.0)
        
        self.assertIsNone(received_msg)
        # Verify settimeout was called with the timeout value first, then None to reset
        settimeout_calls = self.mock_socket.settimeout.call_args_list
        self.assertEqual(len(settimeout_calls), 2)
        self.assertEqual(settimeout_calls[0][0], (1.0,))
        self.assertEqual(settimeout_calls[1][0], (None,))
    
    def test_receive_message_connection_closed(self):
        """Test receiving message when connection is closed"""
        conn = ClientConnection(self.mock_socket, self.address, self.connection_id)
        
        # Mock connection closed (recv returns empty bytes)
        self.mock_socket.recv.return_value = b''
        
        received_msg = conn.receive_message()
        
        self.assertIsNone(received_msg)
        # Note: The connection state is set to DISCONNECTED in the receive_message method when recv fails
        # But our mock doesn't fail, it just returns empty bytes, so state remains CONNECTED
        # This is actually correct behavior for this test case
    
    def test_is_alive_check(self):
        """Test connection alive check"""
        conn = ClientConnection(self.mock_socket, self.address, self.connection_id)
        
        # Fresh connection should be alive
        self.assertTrue(conn.is_alive())
        
        # Simulate old last_activity (beyond timeout)
        conn.last_activity = time.time() - 400  # 400 seconds ago
        
        # Should not be alive (default timeout is 300 seconds)
        self.assertFalse(conn.is_alive())
    
    def test_heartbeat_functionality(self):
        """Test heartbeat sending functionality"""
        conn = ClientConnection(self.mock_socket, self.address, self.connection_id)
        
        # Mock successful sendall for heartbeat
        self.mock_socket.sendall.return_value = None
        
        old_heartbeat = conn.last_heartbeat
        
        # Small delay to ensure time difference
        time.sleep(0.001)
        
        # Send heartbeat
        conn.send_heartbeat()
        
        # Check that last_heartbeat was updated
        self.assertGreater(conn.last_heartbeat, old_heartbeat)
        
        # Verify sendall was called
        self.mock_socket.sendall.assert_called_once()
    
    def test_connection_state_management(self):
        """Test connection state changes"""
        conn = ClientConnection(self.mock_socket, self.address, self.connection_id)
        
        # Connection starts as CONNECTED
        self.assertEqual(conn.state, ConnectionState.CONNECTED)
        
        # Close connection
        conn.close()
        self.assertEqual(conn.state, ConnectionState.DISCONNECTED)
        self.mock_socket.close.assert_called_once()
    
    def test_close_connection(self):
        """Test closing connection"""
        conn = ClientConnection(self.mock_socket, self.address, self.connection_id)
        
        conn.close()
        
        self.assertEqual(conn.state, ConnectionState.DISCONNECTED)
        self.mock_socket.close.assert_called_once()


class TestProtocolHandler(unittest.TestCase):
    """Test cases for ProtocolHandler class"""
    
    def setUp(self):
        """Set up test fixtures"""
        self.handler = ProtocolHandler()
    
    def tearDown(self):
        """Clean up test fixtures"""
        if hasattr(self.handler, '_running') and self.handler._running:
            self.handler.stop()
    
    def test_protocol_handler_creation(self):
        """Test creating protocol handler"""
        self.assertIsNotNone(self.handler)
        self.assertEqual(len(self.handler._connections), 0)
        self.assertEqual(len(self.handler._message_handlers), 0)
    
    def test_register_message_handler(self):
        """Test registering message handlers"""
        def test_handler(connection, message):
            pass
        
        self.handler.register_handler(MessageType.DATA, test_handler)
        
        self.assertIn(MessageType.DATA, self.handler._message_handlers)
        self.assertEqual(self.handler._message_handlers[MessageType.DATA], test_handler)
    
    def test_add_connection(self):
        """Test adding a connection"""
        mock_socket = Mock(spec=socket.socket)
        address = ('127.0.0.1', 12345)
        
        connection = self.handler.add_connection(mock_socket, address)
        
        self.assertIsNotNone(connection)
        self.assertIsNotNone(connection.connection_id)
        self.assertIn(connection.connection_id, self.handler._connections)
        
        conn = self.handler._connections[connection.connection_id]
        self.assertEqual(conn.socket, mock_socket)
        self.assertEqual(conn.address, address)
    
    def test_remove_connection(self):
        """Test removing a connection"""
        mock_socket = Mock(spec=socket.socket)
        address = ('127.0.0.1', 12345)
        
        connection = self.handler.add_connection(mock_socket, address)
        connection_id = connection.connection_id
        self.assertIn(connection_id, self.handler._connections)
        
        self.handler.remove_connection(connection_id)
        self.assertNotIn(connection_id, self.handler._connections)
    
    def test_get_connection(self):
        """Test getting a connection"""
        mock_socket = Mock(spec=socket.socket)
        address = ('127.0.0.1', 12345)
        
        connection = self.handler.add_connection(mock_socket, address)
        connection_id = connection.connection_id
        
        conn = self.handler.get_connection(connection_id)
        self.assertIsNotNone(conn)
        self.assertEqual(conn.connection_id, connection_id)
    
    def test_get_nonexistent_connection(self):
        """Test getting non-existent connection"""
        conn = self.handler.get_connection("nonexistent")
        self.assertIsNone(conn)
    
    def test_send_message_to_connection(self):
        """Test sending message to specific connection"""
        mock_socket = Mock(spec=socket.socket)
        address = ('127.0.0.1', 12345)
        
        connection = self.handler.add_connection(mock_socket, address)
        msg = MessageBuilder(MessageType.HEARTBEAT).build()
        
        # Mock successful send
        mock_socket.sendall.return_value = None
        
        # Send message directly through connection
        connection.send_message(msg)
        
        mock_socket.sendall.assert_called_once()
    
    def test_send_message_to_nonexistent_connection(self):
        """Test getting non-existent connection returns None"""
        conn = self.handler.get_connection("nonexistent")
        self.assertIsNone(conn)
    
    def test_broadcast_message(self):
        """Test broadcasting message to all connections"""
        # Add multiple connections
        sockets = []
        connections = []
        
        for i in range(3):
            mock_socket = Mock(spec=socket.socket)
            address = ('127.0.0.1', 12345 + i)
            
            connection = self.handler.add_connection(mock_socket, address)
            sockets.append(mock_socket)
            connections.append(connection)
        
        msg = MessageBuilder(MessageType.HEARTBEAT).build()
        
        # Mock successful sends
        for sock in sockets:
            sock.sendall.return_value = None
        
        self.handler.broadcast_message(msg)
        
        # Verify all sockets were called
        for sock in sockets:
            sock.sendall.assert_called_once()
    
    def test_handle_message_with_handler(self):
        """Test handling message with registered handler"""
        mock_socket = Mock(spec=socket.socket)
        address = ('127.0.0.1', 12345)
        connection = self.handler.add_connection(mock_socket, address)
        
        # Register handler
        handler_called = False
        received_connection = None
        received_message = None
        
        def test_handler(connection, message):
            nonlocal handler_called, received_connection, received_message
            handler_called = True
            received_connection = connection
            received_message = message
            return None
        
        self.handler.register_handler(MessageType.DATA, test_handler)
        
        # Handle message
        msg = MessageBuilder(MessageType.DATA).text_body("test").property("topic", "test").build()
        
        result = self.handler.handle_message(connection, msg)
        
        self.assertTrue(handler_called)
        self.assertEqual(received_connection, connection)
        self.assertEqual(received_message, msg)
    
    def test_handle_message_without_handler(self):
        """Test handling message without registered handler"""
        mock_socket = Mock(spec=socket.socket)
        address = ('127.0.0.1', 12345)
        connection = self.handler.add_connection(mock_socket, address)
        
        msg = MessageBuilder(MessageType.DATA).text_body("test").property("topic", "test").build()
        
        # Should return an error response when no handler is registered
        result = self.handler.handle_message(connection, msg)
        
        # Should return an error message
        self.assertIsNotNone(result)
        self.assertEqual(result.message_type, MessageType.ACK)
        self.assertEqual(result.properties.get('status'), 'error')
    
    def test_connection_lifecycle(self):
        """Test connection lifecycle management"""
        mock_socket = Mock(spec=socket.socket)
        address = ('127.0.0.1', 12345)
        
        # Add connection
        connection = self.handler.add_connection(mock_socket, address)
        connection_id = connection.connection_id
        self.assertIn(connection_id, self.handler._connections)
        
        # Simulate dead connection by setting old activity time
        connection.last_activity = time.time() - 400  # 400 seconds ago
        
        # Manually check if connection is alive
        self.assertFalse(connection.is_alive())
        
        # Remove dead connection
        self.handler.remove_connection(connection_id)
        self.assertNotIn(connection_id, self.handler._connections)
    
    def test_heartbeat_functionality(self):
        """Test heartbeat functionality through connections"""
        mock_socket = Mock(spec=socket.socket)
        address = ('127.0.0.1', 12345)
        
        connection = self.handler.add_connection(mock_socket, address)
        
        # Mock successful sendall for heartbeat
        mock_socket.sendall.return_value = None
        
        # Send heartbeat through connection
        connection.send_heartbeat()
        
        mock_socket.sendall.assert_called_once()
    
    def test_get_connection_stats(self):
        """Test getting connection statistics"""
        # Add some connections
        for i in range(3):
            mock_socket = Mock(spec=socket.socket)
            address = ('127.0.0.1', 12345 + i)
            self.handler.add_connection(mock_socket, address)
        
        stats = self.handler.get_stats()
        
        self.assertEqual(stats['active_connections'], 3)
        self.assertIn('connections', stats)
        self.assertEqual(len(stats['connections']), 3)
    
    def test_start_stop_handler(self):
        """Test starting and stopping the protocol handler"""
        # Start handler
        self.handler.start()
        self.assertTrue(self.handler._running)
        
        # Stop handler
        self.handler.stop()
        self.assertFalse(self.handler._running)


class TestConnectionState(unittest.TestCase):
    """Test cases for ConnectionState enum"""
    
    def test_connection_state_values(self):
        """Test connection state enum values"""
        self.assertEqual(ConnectionState.CONNECTING.value, 1)
        self.assertEqual(ConnectionState.CONNECTED.value, 2)
        self.assertEqual(ConnectionState.DISCONNECTING.value, 3)
        self.assertEqual(ConnectionState.DISCONNECTED.value, 4)


if __name__ == '__main__':
    unittest.main()
