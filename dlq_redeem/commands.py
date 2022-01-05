import json
import os
import time
from typing import Dict, Optional

import boto3
import click
import click_log

from dlq_redeem import logger
from dlq_redeem.process_dlq import Action, EventCategory, Inspectors, Task, invoke, requeue
from dlq_redeem.queue import backup_message, delete_message, get_message_attributes, get_queue_url, read_messages
from dlq_redeem.utils import DynamodbEncoder

bool_params = dict(type=click.types.BoolParamType(), default=False, show_default=False, show_choices=False)


def ask_for_action(task: Task, message_body: dict, message_attributes: dict) -> Action:
    def _format(value: Optional[str]) -> str:
        return click.style("unknown", fg="yellow") if value is None else click.style(value, fg="cyan")

    logger.info(click.style("Message body:", fg="blue"))
    logger.info(json.dumps(message_body, indent=2) + "\n")
    logger.info(click.style("Message attributes:", fg="blue"))
    logger.info(json.dumps(message_attributes, indent=2, cls=DynamodbEncoder) + "\n")
    logger.info(click.style("Event source: " + _format(task.category.source), fg="blue"))
    logger.info(click.style("Event type: " + _format(task.category.type), fg="blue"))
    logger.info(click.style("Error type: " + _format(task.category.error), fg="blue"))

    action = Action.PASS
    if click.prompt("Do you want to attempt to re-process this? [y/N]", **bool_params):
        action = Action.REPROCESS
    elif click.prompt("Do you want to ignore this and delete the message from the DLQ? [y/N]", **bool_params):
        action = Action.IGNORE

    return action


@click.command()
@click.argument("dlq_name")
@click.option("--backup-dir", default="", help="Path to backup directory, the default is the working directory.")
@click.option(
    "--dry-run/--no-dry-run",
    is_flag=True,
    default=True,
    help="Do not actually re-process or delete messages from the queue.",
)
@click.option("--long-poll", default=0, type=int, help="Keep running and polling the queue for X seconds.")
@click.option(
    "--queue-name", help="Name of the SQS queue whose DLQ is processed. Messages are sent here when reprocessing."
)
@click.option("--interactive", is_flag=True, help="Decide what to do with each unhandled error on the fly.")
@click.option(
    "--reprocess-all-unhandled",
    is_flag=True,
    help="Re-process all failed lambda function invocations where the cause of the failure is unknown "
    "(an exception was returned that is not handled by any inspector defined in this tool).",
)
@click_log.simple_verbosity_option(logger)
def sqs(
    dlq_name: str,
    backup_dir: str,
    dry_run: bool,
    long_poll: int,
    queue_name: Optional[str] = None,
    interactive: bool = False,
    reprocess_all_unhandled: bool = False,
):
    recurring_actions: Dict[EventCategory, Action] = {}
    target_arn: Optional[str] = None

    if interactive and reprocess_all_unhandled:
        raise click.BadParameter("Can't use both --interactive and --reprocess-all-unhandled flags at once.")

    if dry_run:
        logger.warning("Dry run is enabled! Messages won't be re-processed and will remain in the DLQ.")

    sqs_client = boto3.client("sqs")
    lambda_client = boto3.client("lambda")
    dlq_url = get_queue_url(sqs_client, queue_name=dlq_name)
    queue_url = get_queue_url(sqs_client, queue_name=queue_name) if queue_name else None
    backup_dir = os.path.join(backup_dir or ".", "dlq-backup", f"{dlq_name}-{int(time.time())}")

    inspectors = []
    if reprocess_all_unhandled:
        inspectors.append(Inspectors.reprocess_all_unhandled_errors)

    for message in read_messages(client=sqs_client, queue_url=dlq_url, long_poll_duration=long_poll):
        message_body = json.loads(message["Body"])
        message_attributes = get_message_attributes(message)
        task = Task.from_request_context(message_body, message_attributes)

        recurring = False
        action = Action.PASS
        if interactive:
            if task.category in recurring_actions:
                action = recurring_actions[task.category]
                recurring = True
            else:
                action = ask_for_action(task, message_body, message_attributes)
        else:
            for inspector in inspectors:
                action = inspector(task, message_body, message_attributes)
                logger.debug(
                    f"{click.style(inspector.__name__, fg='cyan')} says {click.style(action.name, fg='yellow')}"
                )
                if action != Action.PASS:
                    break

        logger.debug(f"Action: {click.style(action.name, fg='yellow')}")
        backup_message(backup_dir=os.path.join(backup_dir, action.name.lower()), message=message)

        success = False
        if task and action == Action.REPROCESS:
            if not task.target:
                if queue_url:
                    task.target = queue_url
                elif target_arn:
                    task.target = target_arn
                else:
                    while not target_arn:
                        target_arn = click.prompt(
                            "Original target function or queue could not be identified. "
                            "Please enter target function or queue ARN",
                            default=target_arn or "",
                            type=click.STRING,
                        )
                    task.target = target_arn

            if task.target.startswith("arn:aws:lambda:"):
                success = invoke(
                    client=lambda_client,
                    message_id=message["MessageId"],
                    function_arn=task.target,
                    payload=task.payload,
                    dry_run=dry_run,
                )
            elif task.target.startswith("arn:aws:sqs:") or queue_url:
                success = requeue(
                    client=sqs_client,
                    message_id=message["MessageId"],
                    queue_url=get_queue_url(sqs_client, task.target.split(":")[-1])
                    if task.target.startswith("arn:aws:sqs:")
                    else queue_url,
                    payload=task.payload,
                    # Message delivery is delayed to avoid quickly passing it back and forth between queue
                    # and DLQ in case it still fails to process.
                    delay=long_poll,
                    dry_run=dry_run,
                )
            else:
                logger.warning("Message skipped: unhandled original request context and no target SQS queue specified.")

        if not success:
            backup_message(backup_dir=os.path.join(backup_dir, "error"), message=message)

        if success or action == Action.IGNORE:
            delete_message(
                client=sqs_client, queue_url=dlq_url, receipt_handle=message["ReceiptHandle"], dry_run=dry_run
            )

        if (
            interactive
            and task
            and not recurring
            and click.confirm("Do you want to do this with all similar messages", prompt_suffix="? ")
        ):
            recurring_actions[task.category] = action
