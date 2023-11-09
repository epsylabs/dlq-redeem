import json
from base64 import b64decode
from dataclasses import dataclass
from enum import Enum, auto
from typing import NamedTuple, Optional

import click

from dlq_redeem import logger

MAX_MESSAGE_REQUEUE_DELAY = 60  # seconds


class Action(Enum):
    PASS = auto()
    REPROCESS = auto()
    IGNORE = auto()


class EventCategory(NamedTuple):
    source: Optional[str] = None
    type: Optional[str] = None
    error: Optional[str] = None


@dataclass
class Task:
    category: EventCategory = EventCategory()
    payload: Optional[dict] = None
    target: Optional[str] = None

    @classmethod
    def _get_error_message(cls, message_body: dict, message_attributes: dict):
        return (
            message_body.get("responsePayload", {}).get("errorMessage", "")
            or message_attributes.get("ErrorMessage", "")
            or message_attributes.get("ERROR_MESSAGE", "")
        )

    @classmethod
    def from_request_context(cls, message_body: dict, message_attributes: dict) -> "Task":
        category, payload, target = EventCategory(), None, None
        try:
            if "EventBusName" in message_body and "Detail" in message_body:
                category = EventCategory(message_body.get("Source"), message_body.get("DetailType"))
                payload = json.loads(message_body["Detail"])
            elif "requestContext" in message_body and "DDBStreamBatchInfo" in message_body:
                payload = {"DDBStreamBatchInfo": message_body["DDBStreamBatchInfo"]}
                target = message_body["requestContext"].get("functionArn")
                category = EventCategory(
                    "DynamodbStream", "DDBStreamBatchInfo", message_body["requestContext"].get("condition")
                )
            elif "requestPayload" in message_body:
                payload = message_body["requestPayload"]
                target = message_body.get("requestContext", {}).get("functionArn")
                category = EventCategory(
                    payload.get("source"),
                    payload.get("detail-type"),
                    message_body.get("responsePayload", {}).get("errorType"),
                )
            else:
                payload = message_body
                target = message_attributes.get("TARGET_ARN")
                category = EventCategory(
                    payload.get("source"), payload.get("detail-type"), message_attributes.get("ERROR_CODE")
                )
        except json.JSONDecodeError as error:
            logger.warning("Invalid JSON: " + str(error))

        if not category.error:
            message = cls._get_error_message(message_body, message_attributes)
            if "Execution already in progress with idempotency key" in message:
                error_type = "IdempotencyAlreadyInProgressError"
            elif "Task timed out" in message:
                error_type = "TaskTimedOut"
            elif "Invocation failed to be made within the retry-able period" in message:
                error_type = "InvocationFailed"
            else:
                error_type = None

            if error_type:
                category = EventCategory(category.source, category.type, error_type)

        return cls(category, payload, target)

    def __bool__(self) -> bool:
        """Returns False if initialised without any arguments, if anything is not None then it returns True."""
        return not all((all(attr is None for attr in self.category), self.payload is None, self.target is None))


class Inspectors:
    @staticmethod
    def reprocess_validation_errors(task: Task, *_) -> Action:
        return Action.REPROCESS if task.category.error == "ValidationError" else Action.PASS

    @staticmethod
    def reprocess_timeouts(task: Task, message_body: dict, *_) -> Action:
        response_payload = message_body.get("responsePayload") or {}
        if not task.category.error and "Task timed out" in response_payload.get("errorMessage", ""):
            return Action.REPROCESS
        else:
            return Action.PASS

    @staticmethod
    def ignore_in_progress(task: Task, *_) -> Action:
        if task.category.error in ("IdempotencyAlreadyInProgressError", "IDEMPOTENCY_ALREADY_IN_PROGRESS_ERROR"):
            return Action.IGNORE
        else:
            return Action.PASS

    @staticmethod
    def reprocess_event_age_exceeded(_, message_body: dict, *__) -> Action:
        if message_body.get("requestContext", {}).get("condition") == "EventAgeExceeded":
            return Action.REPROCESS
        else:
            return Action.PASS

    @staticmethod
    def reprocess_all_unhandled_errors(task: Task, *_) -> Action:
        return Action.REPROCESS if not task.category.error or task.category.error == "Unhandled" else Action.PASS

    @staticmethod
    def reprocess_all_device_registry_events(task: Task, *_) -> Action:
        return Action.REPROCESS if task.category.source == "device-registry" else Action.PASS

    @staticmethod
    def reprocess_all(*_, **__) -> Action:
        return Action.REPROCESS


def invoke(
    client, message_id: str, function_arn: str, payload: dict, dry_run: bool = False, async_invoke: bool = False
) -> bool:
    logger.info(
        click.style(f'Invoking lambda "{function_arn.split(":")[-1]}"', fg="magenta")
        + (click.style(" dry-run", fg="yellow") if dry_run else "")
        + (click.style(" asynchronously", fg="blue") if async_invoke else "")
        + click.style(" for message ", fg="magenta")
        + click.style(message_id, fg="cyan", bold=True)
    )

    invocation_type = "RequestResponse"
    if dry_run:
        invocation_type = "DryRun"
    elif async_invoke:
        invocation_type = "Event"

    try:
        response = client.invoke(
            FunctionName=function_arn,
            InvocationType=invocation_type,
            LogType="Tail",
            Payload=json.dumps(payload),
        )
        logger.debug(f"Response from Lambda: {response}")
        response_payload = response.get("Payload")
        response_payload = response_payload.read() if hasattr(response_payload, "read") else response_payload
        logger.debug(f"Response payload: {response_payload}")

        if response.get("FunctionError"):
            error_details = response_payload.decode("utf-8")
            try:
                result = json.loads(response_payload)
                error_details = (
                    f'({result["errorType"]}) {result["errorMessage"]}\n'
                    f'Stack trace:\n{"".join(result.get("stackTrace", []))}'
                )
            except json.JSONDecodeError as error:
                logger.warning("JSON decode error: " + str(error))
            except KeyError:
                pass
            finally:
                raise Exception(f"({response['FunctionError']}) {error_details}")
        elif not 200 <= response.get("StatusCode", 0) < 300:
            raise Exception(f'{response.get("StatusCode")} response from Lambda')
        else:
            logger.info(click.style("Success", fg="green"))

        if "LogResult" in response:
            log_tail = b64decode(response["LogResult"]).decode("utf-8")
            report_index = log_tail.rfind("REPORT")
            logger.info(str(log_tail[report_index:] if report_index >= 0 else log_tail).strip("\n"))

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
