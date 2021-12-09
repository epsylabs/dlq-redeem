import click

from dlq_redeem import commands


@click.group()
def cli():
    pass


# noinspection PyTypeChecker
cli.add_command(commands.sqs)

if __name__ == "__main__":
    cli()
