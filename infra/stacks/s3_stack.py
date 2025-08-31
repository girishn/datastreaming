import os
from config import S3Config
import aws_cdk as cdk

from aws_cdk import (
    Stack,
    aws_s3 as s3,
    aws_s3_deployment as s3deploy,
)
from constructs import Construct


class S3Stack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs):
        super().__init__(scope, construct_id, **kwargs)

        self.s3 = s3.Bucket(
            self,
            "FlinkS3Bucket",
            bucket_name=S3Config.S3_BUCKET_NAME,
        )

        
        artifacts_dir = os.path.dirname(os.path.join(
            os.path.dirname(__file__), S3Config.JAR_FILE_PATH
        ))

        s3deploy.BucketDeployment(
            self, "DeployJarFile",
            sources=[s3deploy.Source.asset(artifacts_dir)],
            destination_bucket=self.s3,
            destination_key_prefix="",
            prune=False,
            retain_on_delete=True,
            memory_limit=1024,
        )

