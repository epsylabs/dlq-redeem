import json
import os
import time
from typing import Optional

import boto3
import click
import click_log

from dlq_redeem import logger, process_dlq, queue


@click.command()
@click.argument("dlq_name")
@click.option("--backup-dir", default="", help="Path to backup directory, the default is the working directory.")
@click.option("--dry-run", is_flag=True, help="Do not actually re-process or delete messages from the queue.")
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
    if interactive and reprocess_all_unhandled:
        raise click.BadParameter("Can't use both --interactive and --reprocess-all-unhandled flags at once.")

    if dry_run:
        logger.warning("Dry run is enabled! Messages won't be re-processed and will remain in the DLQ.")

    sqs_client = boto3.client("sqs")
    lambda_client = boto3.client("lambda")

    dlq_url = queue.get_queue_url(sqs_client, queue_name=dlq_name)
    queue_url = queue.get_queue_url(sqs_client, queue_name=queue_name) if queue_name else None

    backup_dir = os.path.join(backup_dir or ".", "dlq-backup", f"{dlq_name}-{int(time.time())}")

    inspectors = process_dlq.inspectors
    if interactive:
        inspectors.append(process_dlq.ask_for_action)
    if reprocess_all_unhandled:
        inspectors.append(process_dlq.check_for_any_unhandled_error)

    for message in queue.read_messages(client=sqs_client, queue_url=dlq_url, long_poll_duration=long_poll):
        message_body = json.loads(message["Body"])

        action = process_dlq.Action.PASS
        task: Optional[process_dlq.Task] = None
        for inspector in inspectors:
            task = inspector(message_body, queue.get_message_attributes(message))
            action = task.action
            logger.debug(f"{click.style(inspector.__name__, fg='cyan')} says {click.style(action.name, fg='yellow')}")
            if action != process_dlq.Action.PASS:
                break

        logger.debug(f"Action: {click.style(action.name, fg='yellow')}")
        queue.backup_message(backup_dir=os.path.join(backup_dir, action.name.lower()), message=message)

        success = False
        if task and task.action == process_dlq.Action.REPROCESS:
            if task.target.startswith("arn:aws:lambda:"):
                success = process_dlq.invoke(
                    client=lambda_client,
                    message_id=message["MessageId"],
                    function_arn=task.target,
                    payload=task.payload,
                    dry_run=dry_run,
                )
            elif task.target.startswith("arn:aws:sqs:") or queue_url:
                success = process_dlq.requeue(
                    client=sqs_client,
                    message_id=message["MessageId"],
                    queue_url=task.target if task.target.startswith("arn:aws:sqs:") else queue_url,
                    payload=task.payload,
                    # Message delivery is delayed to avoid quickly passing it back and forth between queue
                    # and DLQ in case it still fails to process.
                    delay=long_poll,
                    dry_run=dry_run,
                )
            else:
                logger.warning("Message skipped: unhandled original request context and no target SQS queue specified.")

        if success or action == process_dlq.Action.IGNORE:
            queue.delete_message(
                client=sqs_client, queue_url=dlq_url, receipt_handle=message["ReceiptHandle"], dry_run=dry_run
            )
