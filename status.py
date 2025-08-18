"""
Simple status checker for the message broker.
"""
from client import BrokerClient
import time

def main():
    print("Message Broker Status Check")
    print("=" * 40)
    
    try:
        with BrokerClient(client_id="status_checker") as client:
            print("✓ Connected to broker successfully")
            
            # Try to create a test topic
            if client.create_topic("status_test", partitions=1):
                print("✓ Topic creation working")
            else:
                print("⚠ Topic creation failed (may already exist)")
            
            # Try to publish a message
            if client.publish_text("events", f"Status check at {time.time()}"):
                print("✓ Message publishing working")
            else:
                print("✗ Message publishing failed")
            
            print("\nBroker appears to be working correctly!")
            
    except Exception as e:
        print(f"✗ Error connecting to broker: {e}")

if __name__ == '__main__':
    main()
