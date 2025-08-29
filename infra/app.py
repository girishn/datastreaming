#!/usr/bin/env python3
import aws_cdk as cdk
from stacks.kinesis_stack import KinesisStack
from stacks.producer_stack import ProducerStack
from stacks.s3_stack import S3Stack


app = cdk.App()

account = app.node.try_get_context("account")
region = app.node.try_get_context("region")
env = cdk.Environment(account=account, region=region)

kinesis = KinesisStack(app, "KinesisStack", env=env)

ProducerStack(
    app,
    "ProducerStack",
    stream=kinesis.stream,
    env=env
)

S3Stack(
    app,
    "S3Stack",
    env=env
)

app.synth()
