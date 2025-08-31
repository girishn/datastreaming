#!/usr/bin/env python3
import aws_cdk as cdk
from stacks.kinesis_stack import KinesisStack
from stacks.producer_stack import ProducerStack
from stacks.s3_stack import S3Stack
from stacks.flink_app_stack import FlinkAppStack


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

code_bucket = S3Stack(
    app,
    "S3Stack",
    env=env
)

flink = FlinkAppStack(
    app,
    "FlinkAppStack",
    env=env,
    code_bucket=code_bucket.s3,
    jar_object_key="KinesisS3Sink-1.0-SNAPSHOT.jar",
    source_stream=kinesis.stream,
    app_name="KinesisS3Sink",
    kinesis_initial_position="TRIM_HORIZON",
    runtime_environment="FLINK-1_20",
)

# Ensure FlinkAppStack waits for S3Stack to complete
flink.add_dependency(code_bucket)

app.synth()
