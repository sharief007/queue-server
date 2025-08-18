"""
Simple test script to verify the message broker functionality.
"""
import time
import threading
from client import BrokerClient


def test_basic_functionality():
    """Test basic broker functionality"""
    print("Testing basic message broker functionality...")
    
    # Test with one client
    with BrokerClient(client_id="test_client_1") as client:
        
        # Create a test topic
        print("1. Creating topic 'test_topic'...")
        success = client.create_topic("test_topic", partitions=2)
        print(f"   Topic creation: {'SUCCESS' if success else 'FAILED'}")
        
        # Publish some messages
        print("2. Publishing messages...")
        for i in range(5):
            message = f"Test message {i+1}"
            success = client.publish_text("test_topic", message, partition=i % 2)
            print(f"   Published message {i+1}: {'SUCCESS' if success else 'FAILED'}")
        
        time.sleep(1)  # Give some time for processing


def test_pub_sub():
    """Test publish/subscribe functionality"""
    print("\nTesting publish/subscribe...")
    
    received_messages = []
    
    def message_handler(message):
        body = message.body.decode('utf-8')
        received_messages.append(body)
        print(f"   Received: {body}")
    
    # Start subscriber in a separate thread
    def subscriber_thread():
        with BrokerClient(client_id="subscriber_1") as sub_client:
            # Subscribe to topic
            success = sub_client.subscribe("test_topic", partition=0, offset=0, handler=message_handler)
            if success:
                print("   Subscribed to test_topic:0")
                time.sleep(5)  # Listen for 5 seconds
            else:
                print("   Failed to subscribe")
    
    # Start subscriber
    sub_thread = threading.Thread(target=subscriber_thread)
    sub_thread.start()
    
    time.sleep(1)  # Give subscriber time to connect
    
    # Publisher
    with BrokerClient(client_id="publisher_1") as pub_client:
        print("   Publishing messages to subscribed topic...")
        for i in range(3):
            message = f"PubSub message {i+1}"
            success = pub_client.publish_text("test_topic", message, partition=0)
            print(f"   Published: {message} ({'SUCCESS' if success else 'FAILED'})")
            time.sleep(0.5)
    
    # Wait for subscriber to finish
    sub_thread.join()
    
    print(f"   Total messages received by subscriber: {len(received_messages)}")


def test_multiple_clients():
    """Test multiple clients"""
    print("\nTesting multiple clients...")
    
    def client_worker(client_id, message_count):
        with BrokerClient(client_id=client_id) as client:
            for i in range(message_count):
                message = f"Message from {client_id} #{i+1}"
                success = client.publish_text("test_topic", message)
                print(f"   {client_id}: Published message {i+1} ({'SUCCESS' if success else 'FAILED'})")
                time.sleep(0.1)
    
    # Start multiple client threads
    threads = []
    for i in range(3):
        thread = threading.Thread(
            target=client_worker, 
            args=(f"client_{i+1}", 2)
        )
        threads.append(thread)
        thread.start()
    
    # Wait for all to complete
    for thread in threads:
        thread.join()
    
    print("   Multiple client test completed")


def main():
    """Run all tests"""
    print("=" * 50)
    print("MESSAGE BROKER TEST SUITE")
    print("=" * 50)
    
    try:
        test_basic_functionality()
        test_pub_sub()
        test_multiple_clients()
        
        print("\n" + "=" * 50)
        print("ALL TESTS COMPLETED")
        print("=" * 50)
        
    except Exception as e:
        print(f"\nTest failed with error: {e}")
        import traceback
        traceback.print_exc()


if __name__ == '__main__':
    main()
