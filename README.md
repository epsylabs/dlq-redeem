<h2 align="center">dlq-redeem</h2>
<p align="center">
<a href="https://pypi.org/project/dlq-redeem/"><img alt="PyPI" src="https://img.shields.io/pypi/v/dlq-redeem"></a>
<a href="https://pypi.org/project/dlq-redeem/"><img alt="Python" src="https://img.shields.io/pypi/pyversions/dlq-redeem.svg"></a>
<a href="https://github.com/epsylabs/dlq-redeem/blob/master/LICENSE"><img alt="License" src="https://img.shields.io/pypi/l/dlq-redeem.svg"></a>
</p>
AWS SQS dead letter queue (DLQ) message inspection and cleanup automation tool

## Installation
```shell
pip install dlq-redeem
```

## What is it about?
`dlq-redeem` allows you to go through, interactively, DLQ messages and decide if you want te reprocess them.

Based on type of message it will either move it back to its source SQS or invoke lambda function in case message was an EventBridge event.
