from aws_cdk import (
    Stack,
    Duration,
    aws_s3 as s3
)
from constructs import Construct

class S3Stack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs):
        super().__init__(scope, construct_id, **kwargs)

        self.s3 = s3.Bucket(
            self,
            "FlinkS3Bucket",
            bucket_name="gn-flink-bucket",
        )
