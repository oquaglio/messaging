import argparse
import os
import time

from solace.messaging.messaging_service import MessagingService
from solace.messaging.receiver.message_receiver import InboundMessage, MessageHandler
from solace.messaging.resources.queue import Queue
from solace.messaging.resources.topic_subscription import TopicSubscription

# Default configuration
DEFAULT_BROKER = "tcp://localhost:55555"
DEFAULT_VPN = "default"
DEFAULT_USERNAME = "default"
DEFAULT_PASSWORD = "default"
DEFAULT_TOPIC = "solace/loadtest/topic"
DEFAULT_QUEUE = None  # Set to queue name for queue consumption


class SimpleMessageHandler(MessageHandler):
    def on_message(self, message: InboundMessage):
        message_id = message.get_application_message_id() or "N/A"
        payload = message.get_payload_as_string() or "N/A"
        print(
            f"Received message (ID: {message_id}, Payload size: {len(payload.encode('utf-8')) / 1024:.2f} KB)"
        )


def main(broker: str, vpn: str, username: str, password: str, topic: str, queue: str):
    # Log connection parameters
    print(f"Using Solace host: {broker}")
    print(f"Using VPN name: {vpn}")
    print(f"Using username: {username}")
    print(f"Subscribing to: {f'topic {topic}' if topic else f'queue {queue}'}")

    # Configure connection properties
    properties = {
        "solace.messaging.transport.host": broker,
        "solace.messaging.service.vpn-name": vpn,
        "solace.messaging.authentication.scheme.basic.username": username,
        "solace.messaging.authentication.scheme.basic.password": password,
    }
    print(f"Connection properties: {properties}")

    # Build and connect messaging service
    messaging_service = MessagingService.builder().from_properties(properties).build()
    try:
        messaging_service.connect()
        print(f"Connected to Solace broker at {broker}")
    except Exception as e:
        print(f"Connection failed: {str(e)}")
        print("Suggestions:")
        print("- Verify Solace broker is running and accessible")
        print(f"- Check if {broker} is reachable: 'telnet {broker.split('://')[1]}'")
        print("- Ensure port 55555 (or 55443 for TLS) is open")
        print("- Verify broker, VPN, username, and password are correct")
        return

    try:
        # Create receiver
        if queue:
            # Consume from queue
            receiver = (
                messaging_service.create_persistent_message_receiver_builder().build(
                    Queue.durable_exclusive_queue(queue)
                )
            )
        else:
            # Subscribe to topic
            receiver = (
                messaging_service.create_direct_message_receiver_builder()
                .with_subscriptions(TopicSubscription.of(topic))
                .build()
            )

        # Start receiver and attach handler
        receiver.start()
        print(f"Receiver started for {'queue ' + queue if queue else 'topic ' + topic}")
        receiver.receive_async(SimpleMessageHandler())

        # Keep running until interrupted
        print("Running. Press Ctrl+C to stop.")
        try:
            time.sleep(3600)  # Run for 1 hour or until interrupted
        except KeyboardInterrupt:
            print("Stopping receiver...")
    finally:
        # Clean up
        receiver.terminate()
        messaging_service.disconnect()
        print("Disconnected")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Solace JSON Load Test Subscriber")
    parser.add_argument(
        "--broker",
        type=str,
        default=os.environ.get("SOLACE_HOST", DEFAULT_BROKER),
        help="Broker host and port (e.g., tcp://<host>:55555)",
    )
    parser.add_argument(
        "--vpn",
        type=str,
        default=os.environ.get("SOLACE_VPN", DEFAULT_VPN),
        help="Message VPN name (default: default)",
    )
    parser.add_argument(
        "--username",
        type=str,
        default=os.environ.get("SOLACE_USERNAME", DEFAULT_USERNAME),
        help="Broker username (default: default)",
    )
    parser.add_argument(
        "--password",
        type=str,
        default=os.environ.get("SOLACE_PASSWORD", DEFAULT_PASSWORD),
        help="Broker password (default: default)",
    )
    parser.add_argument(
        "--topic",
        type=str,
        default=DEFAULT_TOPIC,
        help="Topic name to subscribe to (default: solace/loadtest/topic)",
    )
    parser.add_argument(
        "--queue",
        type=str,
        default=DEFAULT_QUEUE,
        help="Queue name to consume from (default: None, uses topic if not set)",
    )
    args = parser.parse_args()
    main(args.broker, args.vpn, args.username, args.password, args.topic, args.queue)
