import os
import time

from solace.messaging.config.integration import SolaceProperties
from solace.messaging.messaging_service import MessagingService
from solace.messaging.publisher.direct_message_publisher import DirectMessagePublisher
from solace.messaging.resources.topic import Topic

# Configuration - Set these via env vars or edit directly
BROKER_HOST = os.environ.get("SOLACE_HOST", "tcp://localhost:55555")
VPN_NAME = os.environ.get("SOLACE_VPN", "default")
USERNAME = os.environ.get("SOLACE_USERNAME", "default")
PASSWORD = os.environ.get("SOLACE_PASSWORD", "default")

# Load test config
TOPIC = "solace/loadtest/topic"  # Change to your queue-bound topic if needed
VOLUME = 1000  # Number of messages to publish
MESSAGE_BODY = "Load test message payload"  # Can be str or bytes for SMF binary
DELAY_BETWEEN_MSGS = (
    0.001  # Seconds between messages (0 for max speed; adjust to avoid backpressure)
)
MESSAGE_PROPERTIES = {"app_id": "loadtest"}  # Optional custom SMF properties


def main():
    # Step 1: Configure connection properties
    properties: SolaceProperties = SolaceProperties()
    properties.host = BROKER_HOST
    properties.vpn_name = VPN_NAME
    properties.authentication_scheme_basic_username = USERNAME
    properties.authentication_scheme_basic_password = PASSWORD

    # Step 2: Build and connect the messaging service
    messaging_service = MessagingService.builder().from_properties(properties).build()
    try:
        messaging_service.connect()
        print(f"Connected to Solace broker at {BROKER_HOST}")
    except Exception as e:
        print(f"Connection failed: {e}")
        return

    # Step 3: Create and start the direct message publisher
    # Configures backpressure to reject if buffer hits 1000 (prevents overload)
    publisher: DirectMessagePublisher = (
        messaging_service.create_direct_message_publisher_builder()
        .on_back_pressure_reject(1000)
        .build()
    )
    publisher.start()
    print("Publisher started")

    # Step 4: Prepare reusable message builder for efficiency
    message_builder = messaging_service.message_builder()

    # Step 5: Publish volume messages
    topic_obj = Topic.of(TOPIC)
    start_time = time.time()
    for i in range(VOLUME):
        # Build outbound message (supports SMF properties and binary payloads)
        outbound_message = (
            message_builder.with_application_message_id(f"loadtest-msg-{i}")
            .from_properties(MESSAGE_PROPERTIES)
            .build(MESSAGE_BODY)
        )

        # Publish to topic (or queue-bound topic)
        publisher.publish(outbound_message, topic_obj)

        # Optional delay to control rate (remove for burst publishing)
        time.sleep(DELAY_BETWEEN_MSGS)

    end_time = time.time()
    print(
        f"Published {VOLUME} messages to topic '{TOPIC}' in {end_time - start_time:.2f} seconds "
        f"({VOLUME / (end_time - start_time):.2f} msgs/sec)"
    )

    # Step 6: Clean up
    publisher.terminate()
    messaging_service.disconnect()
    print("Disconnected")


if __name__ == "__main__":
    main()
