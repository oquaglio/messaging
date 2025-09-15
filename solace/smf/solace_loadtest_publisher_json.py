import argparse
import json
import os
import random
import string
import time

from solace.messaging.messaging_service import MessagingService
from solace.messaging.resources.topic import Topic

# Configuration - Set these via env vars or edit directly
BROKER_HOST = os.environ.get("SOLACE_HOST", "tcp://localhost:55555")
VPN_NAME = os.environ.get("SOLACE_VPN", "default")
USERNAME = os.environ.get("SOLACE_USERNAME", "default")
PASSWORD = os.environ.get("SOLACE_PASSWORD", "default")

# Load test config
TOPIC = "solace/loadtest/topic"  # Change to your queue-bound topic if needed
VOLUME = 1000  # Number of messages to publish
DELAY_BETWEEN_MSGS = (
    0.001  # Seconds between messages (0 for max speed; adjust to avoid backpressure)
)
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


def main(payload_size_kb: int):
    # Step 1: Log host and VPN name
    print(f"Using Solace host: {BROKER_HOST}")
    print(f"Using VPN name: {VPN_NAME}")

    # Step 2: Configure connection properties as a dictionary
    properties = {
        "solace.messaging.transport.host": BROKER_HOST,
        "solace.messaging.service.vpn-name": VPN_NAME,
        "solace.messaging.authentication.scheme.basic.username": USERNAME,
        "solace.messaging.authentication.scheme.basic.password": PASSWORD,
    }

    # Step 3: Build and connect the messaging service
    messaging_service = MessagingService.builder().from_properties(properties).build()
    try:
        messaging_service.connect()
        print(f"Connected to Solace broker at {BROKER_HOST}")
    except Exception as e:
        print(f"Connection failed: {e}")
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
    topic_obj = Topic.of(TOPIC)
    start_time = time.time()
    for i in range(VOLUME):
        # Generate random JSON payload
        json_payload = generate_random_json(payload_size_kb)

        # Build outbound message with JSON content type
        outbound_message = (
            message_builder.with_application_message_id(f"loadtest-json-{i}")
            .with_property("content_type", "application/json")
            .from_properties(MESSAGE_PROPERTIES)
            .build(json_payload)
        )

        # Publish to topic (or queue-bound topic)
        publisher.publish(outbound_message, topic_obj)

        # Optional delay to control rate
        time.sleep(DELAY_BETWEEN_MSGS)

    end_time = time.time()
    print(
        f"Published {VOLUME} messages of ~{payload_size_kb} KB to topic '{TOPIC}' "
        f"in {end_time - start_time:.2f} seconds "
        f"({VOLUME / (end_time - start_time):.2f} msgs/sec)"
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
    args = parser.parse_args()
    main(args.size)
