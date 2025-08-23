from aws_cdk import (
    Stack,
    Duration,
    aws_kinesis as kinesis,
    aws_kms as kms,
)
from constructs import Construct

class KinesisStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs):
        super().__init__(scope, construct_id, **kwargs)

        key = kms.Key(
            self, "KinesisKey",
            enable_key_rotation=True,
            alias="alias/kinesis-producer"
        )

        retention_hours = int(self.node.try_get_context("stream_retention_hours") or 24)

        # On-demand is simple and cost-effective to start with
        self.stream = kinesis.Stream(
            self, "DataStream",
            stream_mode=kinesis.StreamMode.ON_DEMAND,
            retention_period=Duration.hours(retention_hours),
            encryption=kinesis.StreamEncryption.KMS,
            encryption_key=key
        )
