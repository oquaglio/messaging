import os
import logging
from datetime import datetime
import boto3
import json
from solace.messaging.messaging_service import (
    MessagingService,
    ReconnectionListener,
    ReconnectionAttemptListener,
    ServiceInterruptionListener,
)
from solace.messaging.receiver.message_receiver import InboundMessage
from solace.messaging.resources.queue import Queue
from solace.messaging.receiver.persistent_message_receiver import PersistentMessageReceiver
import time


# Set the environment variable LOG_LEVEL to 10 (= DEBUG) during development.
log_level = int(os.getenv("LOG_LEVEL", logging.INFO))

logging.basicConfig()
logging.getLogger().setLevel(log_level)


class ServiceEventHandler(ReconnectionAttemptListener, ReconnectionListener, ServiceInterruptionListener):
    def on_reconnected(self, e: "ServiceEvent"):
        logging.info(f"Error cause: {e.get_cause()}")
        logging.info(f"Message: {e.get_message()}")

    def on_reconnecting(self, e: "ServiceEvent"):
        logging.info(f"Error cause: {e.get_cause()}")
        logging.info(f"Message: {e.get_message()}")

    def on_service_interrupted(self, e: "ServiceEvent"):
        logging.info(f"Error cause: {e.get_cause()}")
        logging.info(f"Message: {e.get_message()}")


# This method connects to Secrets Manager and gets secret.
def get_sm_secret(secret_id):
    secrets = {}
    sm_client = boto3.client("secretsmanager")
    sm_response = sm_client.get_secret_value(SecretId=secret_id)
    secretDict = json.loads(sm_response["SecretString"])
    # secrets["sm_token"] = secretDict[sm_access_token]0
    return secretDict


# This method connects to Solace Queue and extracts data.
def get_messages_from_queue(
    solace_host_1,
    solace_host_2,
    solace_vpn,
    solace_usr_name,
    solace_pswd,
    solace_queue,
    batch_string,
    start_time,
    max_runtime_seconds,
):
    try:  # broker 1
        broker_prop = {
            "solace.messaging.transport.host": solace_host_1,
            "solace.messaging.service.vpn-name": solace_vpn,
            "solace.messaging.authentication.scheme.basic.username": solace_usr_name,
            "solace.messaging.authentication.scheme.basic.password": solace_pswd,
        }
        messaging_service = MessagingService.builder().from_properties(broker_prop).build()
        MessagingService.set_core_messaging_log_level(level="DEBUG")
        messaging_service.set_core_messaging_log_level(level="CRITICAL")
        logging.info(f"[LAMBDA LOG] - Connecting to Solace messaging service on broker 1 ({solace_host_1})...")
        messaging_service.connect()
        if messaging_service.is_connected:
            logging.info(f"[LAMBDA LOG] - Connected to broker 1 OK")
    except Exception as e:
        try:  # broker 2
            logging.info(f"[LAMBDA LOG] - Failed to connect to Solace messaging service on broker 1")
            broker_prop = {
                "solace.messaging.transport.host": solace_host_2,
                "solace.messaging.service.vpn-name": solace_vpn,
                "solace.messaging.authentication.scheme.basic.username": solace_usr_name,
                "solace.messaging.authentication.scheme.basic.password": solace_pswd,
            }
            messaging_service = MessagingService.builder().from_properties(broker_prop).build()
            logging.info(f"[LAMBDA LOG] - Connecting to Solace messaging service on broker 2 ({solace_host_2})...")
            messaging_service.connect()
            if messaging_service.is_connected:
                logging.info(f"[LAMBDA LOG] - Connected to broker 2 OK")

        except Exception as e2:
            pass

    if not messaging_service.is_connected:
        logging.info(
            f"[LAMBDA LOG] - Failed to connect to Solace messaging service on broker 1 or 2. Aborting pipeline run."
        )
        return

    # Set up handlers for Solace connection we are initiating.
    service_handler = ServiceEventHandler()
    messaging_service.add_reconnection_listener(service_handler)
    messaging_service.add_reconnection_attempt_listener(service_handler)
    messaging_service.add_service_interruption_listener(service_handler)

    durable_queue = Queue.durable_exclusive_queue(solace_queue)
    direct_receiver = None
    s3_client = boto3.client("s3")
    message_number = 0
    messages_processed = 0

    try:
        direct_receiver: PersistentMessageReceiver = (
            messaging_service.create_persistent_message_receiver_builder()
            # .with_message_auto_acknowledgement()
            .build(durable_queue)
        )
        direct_receiver.start()

        logging.info(f"[LAMBDA LOG] - Source queue: {durable_queue.get_name()}")
        bucket_name = os.getenv("BUCKET_NAME")
        logging.info(f"[LAMBDA LOG] - Target bucket: {bucket_name}")
        logging.info(
            f"[LAMBDA LOG] - Max pipeline run time: {max_runtime_seconds} second(s) ({round(max_runtime_seconds/60, 1)} minute(s))"
        )

        logging.info(f"[LAMBDA LOG] - Listening for new messages on queue...")
        while True:
            if (time.time() - start_time) > max_runtime_seconds:
                logging.info(
                    (f"[LAMBDA LOG] - Stopped listening for new messages (prevent data loss from Lambda timeout)")
                )
                break

            try:
                message: InboundMessage = direct_receiver.receive_message(10000)  # timeout in ms
                if message != None:  # there was a message to receive
                    message_number += 1
                    logging.info(f"[LAMBDA LOG] - Received message #{message_number} from queue")
                    try:
                        upload_message_to_s3(bucket_name, message, s3_client, batch_string, message_number)
                        try:
                            direct_receiver.ack(message)
                            messages_processed += 1  # messages successfully uploaded to s3 and removed from queue
                        except Exception as e:
                            logging.error(
                                f"[LAMBDA LOG] - Exception occurred acknowledging message #{message_number}: {e}"
                            )
                    except Exception as e:
                        logging.error(
                            f"[LAMBDA LOG] - Exception occurred uploading message #{message_number} to s3: {e}"
                        )

            except Exception as e:
                logging.error(f"[LAMBDA LOG] - Exception occurred getting message #{message_number} from queue: {e}")

    except Exception as e:
        logging.error(f"[LAMBDA LOG] - Exception occurred getting message #{message_number} from queue: {e}")

    finally:
        logging.info(f"[LAMBDA LOG] - Total messages consumed during this pipeline run: {messages_processed}")
        logging.info(f"[LAMBDA LOG] - Runtime for this pipeline: {round((time.time() - start_time) / 60, 1)} minute(s)")
        if direct_receiver is not None:
            direct_receiver.terminate()
        logging.info(f"[LAMBDA LOG] - Disconnecting from Solace message service...")
        messaging_service.disconnect()
        logging.info(f"[LAMBDA LOG] - Disconnected.")


# This method processes payloads from the Solace Queue.
def upload_message_to_s3(bucket_name, message, s3_client, batch_string, message_number):
    message_error = False
    message_data = ""
    file_obj_name = ""

    try:
        data = message.get_payload_as_bytes().decode("utf-8")
    except:
        data = message.get_payload_as_bytes()
        logging.error(f"[LAMBDA LOG] - Could not decode the message: {e}")
        message_error = True

    batch_string_date = batch_string.split("/")
    batch_string_timestamp = batch_string_date[-1]  # e.g. 152726
    batch_string_date = batch_string_date[:-1]
    batch_string_date = "".join(batch_string_date)  # e.g. 20231204

    # build s3 obj path using message header details
    if not message_error:
        try:
            data_dict = json.loads(data)
            message_data = data_dict["RailTrackInspectionData"]["Data"]["Survey and Localization Information"]
            message_id = data_dict["RailTrackInspectionData"]["Headers"]["TransactionIdentity"]["MessageID"]
            record_id = data_dict["RailTrackInspectionData"]["Headers"]["TransactionIdentity"]["RecordID"]
            file_obj_name = (
                f"landing/{batch_string_date}/incremental/{batch_string_timestamp}/{message_id}_{record_id}.json"
            )
        except KeyError as e:
            logging.error(f"[LAMBDA LOG] - Failed to extract key from message: {e}")
            message_error = True
        except Exception as e:
            logging.error(f"[LAMBDA LOG] - Error parsing message header: {e}")
            message_error = True

    # build path in case of error parsing message
    if message_error:
        message_data = data
        file_obj_name = f"error/{batch_string_date}/incremental/{batch_string_timestamp}/{message_number}.json"
        logging.info(f"[LAMBDA LOG] - Uploading message to S3 with key: {file_obj_name}")

    s3_client.put_object(
        Body=json.dumps(message_data), Bucket=bucket_name, Key=file_obj_name, ServerSideEncryption="AES256"
    )


def lambda_handler(event, context):
    """
    The variables below are passed from Airflow as strings to Lambda as events payload:
    event['batch_string'] - the batch time for each publication events
    """

    start_time = time.time()

    logging.info(f"[LAMBDA LOG] - Event used - {event}")
    logging.info(context)

    batch_string = event["batch_string"]
    secret_id = event["secret_id"]
    sm_host_1 = event["sm_host_1"]
    sm_host_2 = event["sm_host_2"]
    sm_vpn = event["sm_vpn"]
    sm_username = event["sm_username"]
    sm_pswd = event["sm_pswd"]

    solace_queue = os.getenv("SOLACE_QUEUE")
    max_runtime_seconds = os.getenv("MAX_RUNTIME_SECONDS")
    # solace_queue = event["solace_queue"]

    logging.info("[LAMBDA LOG] - Parameters successfully read from DAG")

    sm_secret = get_sm_secret(secret_id)
    solace_host_1 = sm_secret[sm_host_1]
    solace_host_2 = sm_secret[sm_host_2]
    solace_vpn = sm_secret[sm_vpn]
    solace_usr_name = sm_secret[sm_username]
    solace_pswd = sm_secret[sm_pswd]

    logging.info("[LAMBDA LOG] - Secrets successfully retrieved")

    logging.info(f"[LAMBDA LOG] - Monitoring data in Solace queue")
    get_messages_from_queue(
        solace_host_1,
        solace_host_2,
        solace_vpn,
        solace_usr_name,
        solace_pswd,
        solace_queue,
        batch_string,
        start_time,
        max_runtime_seconds,
    )

    return "Successful Invokation of Lambda"


# USE FOR LOCAL TESTING
if __name__ == "__main__":
    start_time = time.time()
    datetime_obj = datetime.now()
    batch_string = datetime_obj.strftime("%Y/%m/%d/%H%M%S")
    print(batch_string)
    solace_host_1 = os.getenv("SOLACE_HOST_1", "localhost:55554")  # Solace Messaging Format (SMF) protocol, port 55555
    solace_host_2 = os.getenv("SOLACE_HOST_2", "localhost:56554")
    solace_vpn = os.getenv("SOLACE_VPN", "default")  # WAIO-PERTH-01-TEST, WAIO-PERTH-01-QA
    solace_usr_name = os.getenv("SOLACE_USER", "admin")
    solace_pswd = os.getenv("SOLACE_PASSWORD", "admin")
    solace_queue = os.getenv("SOLACE_QUEUE", "q/minaus/waio/sf/cam/trackmeasurement/recorded/v1")
    max_runtime_seconds = os.getenv("MAX_RUNTIME_SECONDS", 30)

    get_messages_from_queue(
        solace_host_1,
        solace_host_2,
        solace_vpn,
        solace_usr_name,
        solace_pswd,
        solace_queue,
        batch_string,
        start_time,
        max_runtime_seconds,
    )

    # lambda_handler(lambda_event, None)
