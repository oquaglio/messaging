import argparse
import json
import os
import random
import string
import time

from solace.messaging.messaging_service import MessagingService
from solace.messaging.resources.topic import Topic

# Default configuration
DEFAULT_BROKER = "tcp://localhost:55555"
DEFAULT_VPN = "default"
DEFAULT_USERNAME = "default"
DEFAULT_PASSWORD = "default"

# Default load test config
DEFAULT_TOPIC = "solace/loadtest/topic"
DEFAULT_VOLUME = 1000
DEFAULT_DELAY = 0.001
MESSAGE_PROPERTIES = {"app_id": "loadtest"}  # Optional custom SMF properties


def generate_random_json(target_size_kb: int) -> str:
    """Generate a random JSON string approximately matching the target size in KB."""
    target_size_bytes = target_size_kb * 1024  # Convert KB to bytes
    base_data = {
        "id": str(random.randint(1, 1000000)),
        "timestamp": time.time(),
        "data": [],
    }

    # Generate random strings to fill the data array
    chars = string.ascii_letters + string.digits
    item_size = 100  # Approx size of each array item in bytes
    num_items = (
        target_size_bytes - 100
    ) // item_size  # Reserve ~100 bytes for base structure

    for _ in range(num_items):
        random_string = "".join(
            random.choice(chars) for _ in range(80)
        )  # ~80 bytes per string
        base_data["data"].append(
            {
                "value": random_string,
                "index": random.randint(1, 1000),
                "flag": random.choice([True, False]),
            }
        )

    # Serialize to JSON
    json_str = json.dumps(base_data)

    # Adjust size by trimming or padding
    current_size = len(json_str.encode("utf-8"))
    if current_size < target_size_bytes:
        padding = " " * (target_size_bytes - current_size - 50)  # Conservative padding
        base_data["padding"] = padding
        json_str = json.dumps(base_data)
    elif current_size > target_size_bytes:
        excess_items = (current_size - target_size_bytes) // item_size + 1
        base_data["data"] = base_data["data"][:-excess_items]
        json_str = json.dumps(base_data)

    final_size = len(json_str.encode("utf-8")) / 1024  # Size in KB
    print(f"Generated JSON payload of ~{final_size:.2f} KB")
    return json_str


def main(
    payload_size_kb: int,
    num_messages: int,
    topic_name: str,
    delay: float,
    broker: str,
    vpn: str,
    username: str,
    password: str,
):
    # Step 1: Log connection parameters
    print(f"Using Solace host: {broker}")
    print(f"Using VPN name: {vpn}")
    print(f"Using username: {username}")
    print(
        f"Publishing {num_messages} messages to topic '{topic_name}' with {delay} sec delay"
    )

    # Step 2: Configure connection properties as a dictionary
    properties = {
        "solace.messaging.transport.host": broker,
        "solace.messaging.service.vpn-name": vpn,
        "solace.messaging.authentication.scheme.basic.username": username,
        "solace.messaging.authentication.scheme.basic.password": password,
    }
    print(f"Connection properties: {properties}")

    # Step 3: Build and connect the messaging service
    messaging_service = MessagingService.builder().from_properties(properties).build()
    try:
        messaging_service.connect()
        print(f"Connected to Solace broker at {broker}")
    except Exception as e:
        print(f"Connection failed: {str(e)}")
        print(f"Error details: {type(e).__name__}")
        print("Suggestions:")
        print("- Verify Solace broker is running and accessible")
        print(f"- Check if {broker} is reachable: 'telnet {broker.split('://')[1]}'")
        print("- Ensure port 55555 (or 55443 for TLS) is open")
        print("- Verify broker, VPN, username, and password are correct")
        return

    # Step 4: Create and start the direct message publisher
    publisher = (
        messaging_service.create_direct_message_publisher_builder()
        .on_back_pressure_reject(1000)
        .build()
    )
    publisher.start()
    print("Publisher started")

    # Step 5: Prepare reusable message builder
    message_builder = messaging_service.message_builder()

    # Step 6: Publish volume messages with random JSON
    topic_obj = Topic.of(topic_name)
    start_time = time.time()
    for i in range(num_messages):
        # Generate random JSON payload
        json_payload = generate_random_json(payload_size_kb)

        # Build outbound message with JSON content type
        outbound_message = (
            message_builder.with_application_message_id(f"loadtest-json-{i}")
            .with_property("content_type", "application/json")
            .from_properties(MESSAGE_PROPERTIES)
            .build(json_payload)
        )

        # Publish to topic and log
        publisher.publish(outbound_message, topic_obj)
        print(
            f"Published message {i + 1}/{num_messages} (ID: loadtest-json-{i}, ~{payload_size_kb} KB)"
        )

        # Optional delay to control rate
        if delay > 0:
            time.sleep(delay)

    end_time = time.time()
    print(
        f"Published {num_messages} messages of ~{payload_size_kb} KB to topic '{topic_name}' "
        f"in {end_time - start_time:.2f} seconds "
        f"({num_messages / (end_time - start_time):.2f} msgs/sec)"
    )

    # Step 7: Clean up
    publisher.terminate()
    messaging_service.disconnect()
    print("Disconnected")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Solace JSON Load Test Publisher")
    parser.add_argument(
        "--size",
        type=int,
        default=1,
        help="Payload size in KB (e.g., 1, 10, 100, 1000)",
    )
    parser.add_argument(
        "--messages",
        type=int,
        default=DEFAULT_VOLUME,
        help="Number of messages to publish (default: 1000)",
    )
    parser.add_argument(
        "--topic",
        type=str,
        default=DEFAULT_TOPIC,
        help="Topic name to publish to (default: solace/loadtest/topic)",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=DEFAULT_DELAY,
        help="Delay between messages in seconds (default: 0.001)",
    )
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
    args = parser.parse_args()
    main(
        args.size,
        args.messages,
        args.topic,
        args.delay,
        args.broker,
        args.vpn,
        args.username,
        args.password,
    )
