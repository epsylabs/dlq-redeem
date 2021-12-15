import json
import os
import time
from decimal import Decimal
from pprint import pformat
from typing import Generator, Optional, Set

import click

from dlq_redeem import logger

LONG_POLL_MAX_VISIBILITY_TIMEOUT = 600  # seconds
LONG_POLL_MAX_POLL_INTERVAL = 20  # Must be >= 0 and <= 20 (seconds)


def read_messages(client, queue_url: str, long_poll_duration: Optional[int] = None) -> Generator[dict, None, None]:
    already_seen: Set[str] = set()

    params = dict(QueueUrl=queue_url, MaxNumberOfMessages=10, WaitTimeSeconds=1, MessageAttributeNames=["All"])
    if long_poll_duration:
        params.update(
            VisibilityTimeout=min(LONG_POLL_MAX_VISIBILITY_TIMEOUT, long_poll_duration),
            WaitTimeSeconds=min(LONG_POLL_MAX_POLL_INTERVAL, long_poll_duration),
        )

    time_start = time.time()
    while not long_poll_duration or time.time() < time_start + long_poll_duration:
        logger.info(click.style("\nPolling queue for messages...", fg="magenta", bold=True))
        response = client.receive_message(**params)

        message_count = len(response.get("Messages", []))
        logger.debug(f"Received {message_count} messages.")
        if not long_poll_duration and message_count < 1:
            logger.debug("Stopped polling: no more visible messages in queue.")
            break

        for message in response.get("Messages", []):
            if message["MessageId"] in already_seen:
                logger.debug(f"Skipping message {message['MessageId']}: already seen")
                continue
            already_seen.add(message["MessageId"])

            logger.info(
                click.style("\nProcessing message ", fg="magenta") + click.style(message["MessageId"], fg="cyan")
            )

            try:
                logger.debug(pformat({k: json.loads(v) if k == "Body" else v for k, v in message.items()}, indent=2))
            except (ValueError, TypeError):
                pass

            yield message

    logger.info(click.style("\nNo more visible messages in queue.", fg="magenta", bold=True))


def backup_message(backup_dir: str, message: dict):
    backup_dir = os.path.realpath(backup_dir)
    os.makedirs(backup_dir, exist_ok=True)

    file_path = os.path.join(backup_dir, message["MessageId"] + ".json")
    with open(file_path, "w") as fp:
        json.dump(message, fp, indent=2)

    logger.debug(f"Saved message contents to {file_path}")


def delete_message(client, queue_url: str, receipt_handle: str, dry_run: bool = False):
    if not dry_run:
        try:
            response = client.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt_handle)
            logger.debug(f"Response from SQS: {response}")
            logger.info(click.style("Message has been deleted from the queue.", fg="green"))
        except Exception as error:
            logger.error(click.style(f"Failed to delete message from the queue: {error}", fg="red", bold=True))
    else:
        logger.info("Message remains in the queue (dry-run).")


def get_queue_url(client, queue_name: str) -> str:
    try:
        return client.get_queue_url(QueueName=queue_name).get("QueueUrl")
    except client.exceptions.QueueDoesNotExist:
        raise click.BadParameter(f'Could not find SQS queue by name "{queue_name}".')


def get_message_attributes(message: dict) -> dict:
    def _parse_value(value: dict):
        if value["DataType"] == "String":
            return value["StringValue"]
        elif value["DataType"] == "Number":
            return Decimal(value["StringValue"])
        elif value["DataType"] == "Binary":
            return value["BinaryValue"]
        else:
            raise NotImplementedError(f'Unhandled message attribute type: "{value["DataType"]}"')

    return {k: _parse_value(v) for k, v in message.get("MessageAttributes", {}).items()}
