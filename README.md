# Message Broker

A high-performance, in-memory message broker with disk persistence, built from scratch in Python. This broker provides topic-based messaging with multiple partitions, supporting both point-to-point and publish-subscribe messaging patterns.

## Features

- **Topic-based Architecture**: Messages are organized into topics with configurable partitions
- **Multiple Messaging Patterns**: Support for point-to-point and pub/sub messaging
- **Custom TCP Protocol**: High-performance binary protocol for fast communication
- **Persistence**: Hybrid in-memory + disk storage with append-only logs
- **Message Delivery Guarantees**: Configurable delivery semantics (at-most-once, at-least-once)
- **Multi-client Support**: Handle multiple concurrent connections
- **Partitioning**: Distribute messages across partitions for scalability
- **Heartbeat & Connection Management**: Automatic connection health monitoring

## Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│     Client      │────│   TCP Server    │────│  Message Broker │
└─────────────────┘    └─────────────────┘    └─────────────────┘
                                                        │
                              ┌─────────────────────────┼─────────────────────────┐
                              │                         │                         │
                    ┌─────────▼─────────┐    ┌─────────▼─────────┐    ┌─────────▼─────────┐
                    │  Topic Manager    │    │ Storage Manager   │    │ Protocol Handler  │
                    └───────────────────┘    └───────────────────┘    └───────────────────┘
                              │                         │
                    ┌─────────▼─────────┐    ┌─────────▼─────────┐
                    │   Subscriptions   │    │   Append Logs     │
                    └───────────────────┘    └───────────────────┘
```

## Quick Start

### 1. Start the Broker Server

```bash
python server.py
```

The server will start on `127.0.0.1:9999` by default and create some default topics.

### 2. Create a Topic

```bash
python client.py create-topic my_topic --partitions 2
```

### 3. Publish Messages

```bash
python client.py publish my_topic "Hello, World!"
python client.py publish my_topic "Another message" --partition 1
```

### 4. Subscribe to Messages

```bash
python client.py subscribe my_topic --partition 0 --offset 0
```

## Configuration

Configuration can be provided via:
1. JSON file: `config/broker.json`
2. Environment variables (prefixed with `QB_`)

### Key Configuration Options

```json
{
  "server": {
    "host": "127.0.0.1",
    "port": 9999,
    "max_connections": 100,
    "heartbeat_interval": 30
  },
  "storage": {
    "data_dir": "./storage/data",
    "snapshot_interval": 60
  },
  "topics": {
    "default_partitions": 1,
    "max_partitions": 16,
    "retention_hours": 24
  }
}
```

### Environment Variables

- `QB_SERVER_HOST`: Server host address
- `QB_SERVER_PORT`: Server port
- `QB_DATA_DIR`: Data storage directory
- `QB_LOG_LEVEL`: Logging level (INFO, DEBUG, etc.)

## Message Format

Messages use a custom binary format for efficiency:

```
[4 bytes: total_length][4 bytes: message_type][8 bytes: sequence_number]
[8 bytes: timestamp][4 bytes: properties_length][properties_data]
[4 bytes: body_length][body_data]
```

## Client API

### Python Client Example

```python
from client import BrokerClient

# Connect to broker
with BrokerClient(host='127.0.0.1', port=9999, client_id='my_client') as client:
    
    # Create topic
    client.create_topic('events', partitions=3)
    
    # Publish message
    client.publish_text('events', 'Hello World!', partition=0)
    
    # Subscribe to messages
    def handle_message(message):
        print(f"Received: {message.body.decode('utf-8')}")
    
    client.subscribe('events', partition=0, handler=handle_message)
    
    # Keep connection alive
    time.sleep(10)
```

## Storage

The broker uses a hybrid storage approach:

1. **In-Memory**: Fast access to recent messages
2. **Append Logs**: Persistent storage on disk
3. **Snapshots**: Periodic state snapshots for recovery

### Storage Layout

```
storage/
├── data/
│   ├── events_p0.log      # Topic 'events', partition 0
│   ├── events_p0.idx      # Index file
│   ├── events_p1.log      # Topic 'events', partition 1
│   └── events_p1.idx
└── snapshots/
    └── snapshot_20250818_120000.json
```

## Testing

Run the test suite:

```bash
python test_broker.py
```

This will test:
- Basic topic creation and message publishing
- Publish/subscribe functionality
- Multiple concurrent clients

## Protocol Details

### Message Types

- `DATA`: User data messages
- `ACK`: Acknowledgment messages
- `HEARTBEAT`: Connection health checks
- `CREATE_TOPIC`: Topic creation requests
- `SUBSCRIBE`: Subscription requests
- `UNSUBSCRIBE`: Unsubscription requests
- `PUBLISH`: Message publishing requests

### Connection Lifecycle

1. Client connects to server
2. TCP connection established
3. Client sends requests (create topic, subscribe, publish)
4. Server responds with ACK messages
5. Heartbeats maintain connection health
6. Automatic cleanup on disconnection

## Production Considerations

This broker is designed for testing and development. For production use, consider:

- **Clustering**: Implement distributed consensus (Raft/PBFT)
- **Replication**: Data replication across nodes
- **Security**: Authentication and authorization
- **Monitoring**: Metrics and health checks
- **Performance**: Optimizations for high throughput

## Docker Support

Create multiple broker instances for testing distributed scenarios:

```yaml
# docker-compose.yml
version: '3.8'
services:
  broker1:
    build: .
    ports:
      - "9999:9999"
    environment:
      - QB_SERVER_PORT=9999
      - QB_DATA_DIR=/data/broker1
    volumes:
      - ./storage/broker1:/data/broker1

  broker2:
    build: .
    ports:
      - "10000:9999"
    environment:
      - QB_SERVER_PORT=9999
      - QB_DATA_DIR=/data/broker2
    volumes:
      - ./storage/broker2:/data/broker2
```

## License

MIT License - see LICENSE file for details.
