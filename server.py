"""
TCP Server implementation for the message broker.
"""
import logging
import signal
import sys
from socketserver import ThreadingTCPServer, BaseRequestHandler
from typing import Optional

from core.broker import MessageBroker
from core.config import initialize_config


# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class BrokerRequestHandler(BaseRequestHandler):
    """Request handler for broker connections"""
    _broker: Optional[MessageBroker] = None

    def setup(self):
        """Setup the connection"""
        self._broker = self.server.broker
        self.connection = self._broker.add_client_connection(
            self.request, 
            self.client_address
        )
        logger.info(f"Client connected: {self.client_address}")


    def handle(self):
        """Handle client communication"""
        try:
            while True:
                # Receive message
                message = self.connection.receive_message(timeout=1.0)
                if message is None:
                    # Check if connection is still alive
                    if not self.connection.is_alive():
                        break
                    continue
                
                # Handle message
                response = self._broker.protocol_handler.handle_message(
                    self.connection, message
                )
                
                # Send response if any
                if response is not None:
                    self.connection.send_message(response)
                
        except Exception as e:
            logger.error(f"Error handling client {self.client_address}: {e}")
        finally:
            self.finish()
    
    def finish(self):
        """Clean up the connection"""
        try:
            self._broker.remove_client_connection(self.connection.connection_id)
        except Exception as e:
            logger.error(f"Error cleaning up connection: {e}")
        
        logger.info(f"Client disconnected: {self.client_address}")


class BrokerServer(ThreadingTCPServer):
    """TCP Server for the message broker"""
    
    allow_reuse_address = True
    daemon_threads = True  # Allow server to exit even if threads are running
    
    def __init__(self, server_address, RequestHandlerClass):
        self.broker = MessageBroker()  # Initialize broker before calling super()
        super().__init__(server_address, RequestHandlerClass)
    
    def server_activate(self):
        """Start the broker when server activates"""
        super().server_activate()
        self.broker.start()
        logger.info(f"Broker server listening on {self.server_address}")
    
    def server_close(self):
        """Stop the broker when server closes"""
        logger.info("Shutting down broker server...")
        if hasattr(self, 'broker'):
            self.broker.stop()
        super().server_close()


def create_default_topics(broker: MessageBroker):
    """Create some default topics for testing"""
    default_topics = [
        ('events', 2),
        ('logs', 1),
        ('notifications', 3),
    ]
    
    for topic_name, partitions in default_topics:
        try:
            broker.create_topic(topic_name, partitions)
            logger.info(f"Created default topic: {topic_name} with {partitions} partitions")
        except Exception as e:
            logger.warning(f"Failed to create default topic {topic_name}: {e}")


def signal_handler(signum, frame):
    """Handle shutdown signals"""
    logger.info(f"Received signal {signum}, shutting down...")
    sys.exit(0)


def main():
    """Main server entry point"""
    # Initialize configuration
    config = initialize_config()
    
    # Setup logging level from config
    log_level = getattr(logging, config.get('logging.level', 'INFO').upper())
    logging.getLogger().setLevel(log_level)
    
    # Setup signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Get server configuration
    host = config.get('server.host')
    port = config.get('server.port')
    
    logger.info(f"Starting message broker server on {host}:{port}")
    logger.info(f"Data directory: {config.get('storage.data_dir')}")
    logger.info(f"Snapshot directory: {config.get('storage.snapshot_dir')}")
    
    # Create and start server
    server = BrokerServer((host, port), BrokerRequestHandler)
    
    try:
        # Create default topics
        create_default_topics(server.broker)
        
        # Start serving
        logger.info("Message broker server started successfully")
        logger.info("Press Ctrl+C to stop the server")
        
        server.serve_forever()
        
    except KeyboardInterrupt:
        logger.info("Received keyboard interrupt")
    except Exception as e:
        logger.error(f"Server error: {e}")
    finally:
        server.server_close()
        logger.info("Server stopped")


if __name__ == '__main__':
    main()
