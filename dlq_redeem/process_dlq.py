import json
from base64 import b64decode
from dataclasses import dataclass
from enum import Enum, auto
from typing import Dict, Optional, Tuple

import click

from dlq_redeem import logger

MAX_MESSAGE_REQUEUE_DELAY = 60  # seconds


class Action(Enum):
    PASS = auto()
    REPROCESS = auto()
    IGNORE = auto()


recurring_actions: Dict[Tuple[Optional[str], Optional[str], Optional[str]], Action] = {}


@dataclass
class Task:
    action: Action
    payload: Optional[dict] = None
    target: Optional[str] = None

    @classmethod
    def from_request_context(cls, action: Action, message_body: dict, message_attributes: dict) -> "Task":
        if function_arn := message_attributes.get("TARGET_ARN"):
            return cls(action, message_body, function_arn)

        try:
            return cls(action, message_body["requestPayload"], message_body["requestContext"]["functionArn"])
        except KeyError as error:
            logger.error(f"Unexpected message body structure: {error} not found.")
            return cls(Action.PASS)


def check_for_validation_error(message_body: dict, message_attributes: dict) -> Task:
    if message_body.get("responsePayload", {}).get("errorType") == "ValidationError":
        return Task.from_request_context(Action.REPROCESS, message_body, message_attributes)
    else:
        return Task(Action.PASS)


def check_for_timeout(message_body: dict, message_attributes: dict) -> Task:
    response_payload = message_body.get("responsePayload") or {}
    if not response_payload.get("errorType") and "Task timed out" in response_payload.get("errorMessage", ""):
        return Task.from_request_context(Action.REPROCESS, message_body, message_attributes)
    else:
        return Task(Action.PASS)


def check_for_in_progress(message_body: dict, message_attributes: dict) -> Task:
    if (
        message_body.get("responsePayload", {}).get("errorType") == "IdempotencyAlreadyInProgressError"
        or message_attributes.get("ERROR_CODE") == "IDEMPOTENCY_ALREADY_IN_PROGRESS_ERROR"
    ):
        return Task(Action.IGNORE)
    else:
        return Task(Action.PASS)


def check_for_event_age_exceeded(message_body: dict, message_attributes: dict) -> Task:
    if message_body.get("requestContext", {}).get("condition") == "EventAgeExceeded":
        return Task.from_request_context(Action.REPROCESS, message_body, message_attributes)
    else:
        return Task(Action.PASS)


def check_for_any_unhandled_error(message_body: dict, message_attributes: dict) -> Task:
    if message_body.get("responseContext", {}).get("functionError") == "Unhandled":
        return Task.from_request_context(Action.REPROCESS, message_body, message_attributes)
    else:
        return Task(Action.PASS)


def ask_for_action(message_body: dict, message_attributes: dict) -> Task:
    payload = message_body.get("requestPayload") or message_body
    source = payload.get("source")
    event_type = payload.get("detail-type")
    error_type = (
        message_attributes["ERROR_CODE"]
        if "ERROR_CODE" in message_attributes
        else message_body.get("responsePayload", {}).get("errorType")
    )
    choice_key = (source, event_type, error_type)

    if choice_key in recurring_actions:
        return Task.from_request_context(recurring_actions[choice_key], message_body, message_attributes)

    logger.info(click.style("Message body:", fg="blue"))
    logger.info(json.dumps(message_body, indent=2) + "\n")
    logger.info(click.style("Message attributes:", fg="blue"))
    logger.info(json.dumps(message_attributes, indent=2) + "\n")

    bool_params = dict(type=click.types.BoolParamType(), default=False, show_default=False, show_choices=False)

    action = Action.PASS
    if click.prompt("Do you want to attempt to re-process this? [y/N]", **bool_params):
        action = Action.REPROCESS
    elif click.prompt("Do you want to ignore this and delete the message from the DLQ? [y/N]", **bool_params):
        action = Action.IGNORE

    def _format(value: Optional[str]) -> str:
        return click.style("unknown", fg="yellow") if value is None else click.style(value, fg="cyan")

    if click.prompt(
        f"Do you want to do the same with all {_format(source)} messages of type {_format(event_type)} "
        f"that failed with {_format(error_type)} error? [y/N]",
        **bool_params,
    ):
        recurring_actions[choice_key] = action

    return (
        Task.from_request_context(action, message_body, message_attributes)
        if action == Action.REPROCESS
        else Task(action)
    )


inspectors = [
    check_for_in_progress,
]
# TODO: Improve the inspectors so they stop catching all failures, or figure out how to integrate them with
#       interactive mode. Now they don't allow fine tuned handling based on event type, they will cause all
#       events to be retried (even if we don't want to retry some of them).


def invoke(client, message_id: str, function_arn: str, payload: dict, dry_run: bool = False) -> bool:
    logger.info(
        click.style("Invoking lambda", fg="magenta")
        + (click.style(" dry-run", fg="yellow") if dry_run else "")
        + click.style(" for message ", fg="magenta")
        + click.style(message_id, fg="cyan", bold=True)
    )

    try:
        response = client.invoke(
            FunctionName=function_arn,
            InvocationType="DryRun" if dry_run else "RequestResponse",
            LogType="Tail",
            Payload=json.dumps(payload),
        )
        logger.debug(f"Response from Lambda: {response}")
        response_payload = response.get("Payload")
        response_payload = response_payload.read() if hasattr(response_payload, "read") else response_payload
        logger.debug(f"Response payload: {response_payload}")

        if response.get("FunctionError"):
            raise Exception(f"({response['FunctionError']}) {response_payload.decode('utf-8')}")
        else:
            logger.info(click.style("Success", fg="green"))

        if "LogResult" in response:
            log_tail = b64decode(response["LogResult"]).decode("utf-8")
            report_index = log_tail.rfind("REPORT")
            logger.info(log_tail[report_index:] if report_index >= 0 else log_tail)

        return True
    except Exception as error:
        logger.error(click.style(f"Function invocation failed: {error}", fg="red", bold=True))
        return False


def requeue(
    client, message_id: str, queue_url: str, payload: dict, delay: Optional[int] = None, dry_run: bool = False
) -> bool:
    logger.info(
        click.style("Sending message ", fg="magenta")
        + click.style(message_id, fg="cyan", bold=True)
        + click.style(" to original SQS queue", fg="magenta")
    )

    if dry_run:
        logger.debug("Dry-run mode enabled, skipped sending message to original SQS queue.")
        return True

    try:
        response = client.send_message(
            QueueUrl=queue_url,
            MessageBody=json.dumps(payload),
            DelaySeconds=min(MAX_MESSAGE_REQUEUE_DELAY, delay),
        )
        logger.debug(f"Response from SQS: {response}")
        logger.info(click.style("Success", fg="green"))
        return True
    except Exception as error:
        logger.error(click.style(f"Failed to send message to SQS queue: {error}", fg="red", bold=True))
        return False
