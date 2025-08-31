from typing import Optional, Literal

from aws_cdk import (
    Aws,
    Stack,
    aws_iam as iam,
    aws_logs as logs,
    aws_s3 as s3,
    aws_kinesis as kinesis,
    aws_kinesisanalytics as kda,
    custom_resources as cr,
)
from constructs import Construct


KinesisInitPos = Literal["LATEST", "TRIM_HORIZON"]


class FlinkAppStack(Stack):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        *,
        code_bucket: s3.IBucket,
        jar_object_key: str,
        source_stream: kinesis.IStream,
        app_name: str,
        runtime_environment: str = "FLINK-1_20",
        property_group_id: str = "app.runtime",
        kinesis_initial_position: KinesisInitPos = "TRIM_HORIZON",
        **kwargs,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # ---- Service execution role for Kinesis Data Analytics (Apache Flink)
        kda_role = iam.Role(
            self,
            "FlinkServiceRole",
            assumed_by=iam.ServicePrincipal("kinesisanalytics.amazonaws.com"),
            description="Execution role for Kinesis Data Analytics for Apache Flink",
        )

        # Read the JAR from S3 (grants GetObject and KMS decrypt if bucket uses KMS)
        code_bucket.grant_read(kda_role, jar_object_key)

        # Allow reading from the Kinesis source stream
        kda_role.add_to_policy(
            iam.PolicyStatement(
                actions=[
                    "kinesis:DescribeStream",
                    "kinesis:DescribeStreamSummary",
                    "kinesis:GetShardIterator",
                    "kinesis:GetRecords",
                    "kinesis:ListShards",
                ],
                resources=[source_stream.stream_arn],
            )
        )

        # ---- CloudWatch Logs for the application
        log_group = logs.LogGroup(
            self,
            "FlinkLogGroup",
            retention=logs.RetentionDays.ONE_MONTH,
        )
        log_stream = logs.LogStream(
            self,
            "FlinkLogStream",
            log_group=log_group,
            log_stream_name=f"{app_name}-app",
        )
        log_group.grant_write(kda_role)
        
        
        # Build the log stream ARN explicitly (since LogStream may not expose it)
        log_stream_arn = (
            f"arn:{Aws.PARTITION}:logs:{Aws.REGION}:{Aws.ACCOUNT_ID}:"
            f"log-group:{log_group.log_group_name}:log-stream:{log_stream.log_stream_name}"
        )


        # ---- Kinesis Data Analytics (Apache Flink) Application
        app = kda.CfnApplicationV2(
            self,
            "FlinkApplication",
            application_name=app_name,
            runtime_environment=runtime_environment,
            service_execution_role=kda_role.role_arn,
            application_configuration={
                "applicationCodeConfiguration": {
                    "codeContentType": "ZIPFILE",  # use S3 jar/zip
                    "codeContent": {
                        "s3ContentLocation": {
                            "bucketArn": code_bucket.bucket_arn,
                            "fileKey": jar_object_key,
                        }
                    },
                },
                "flinkApplicationConfiguration": {
                    "checkpointConfiguration": {"configurationType": "DEFAULT"},
                    "monitoringConfiguration": {"configurationType": "DEFAULT"},
                    "parallelismConfiguration": {"configurationType": "DEFAULT"},
                },
                "environmentProperties": {
                    "propertyGroups": [
                        {
                            "propertyGroupId": property_group_id,
                            "propertyMap": {
                                "kinesis.stream.arn": source_stream.stream_arn,
                                "aws.region": Stack.of(self).region,
                                "kinesis.source.initial.position": kinesis_initial_position,
                                "s3.bucket.name": code_bucket.bucket_name,
                            },
                        }
                    ]
                },
            },
        )

        # ---- Send KDA application logs to CloudWatch Logs
        cw_logs = kda.CfnApplicationCloudWatchLoggingOptionV2(
            self,
            "FlinkAppCloudWatchLogs",
            application_name=app.ref,  # reference to the created application
            cloud_watch_logging_option=kda.CfnApplicationCloudWatchLoggingOptionV2.CloudWatchLoggingOptionProperty(
                log_stream_arn=log_stream_arn,
            ),
        )
        cw_logs.node.add_dependency(app)

        # ---- Start the Flink application after create (one-time)
        start_on_create = cr.AwsCustomResource(
            self,
            "StartFlinkAppOnCreate",
            on_create=cr.AwsSdkCall(
                service="KinesisAnalyticsV2",
                action="startApplication",
                parameters={
                    "ApplicationName": app.ref,
                    "RunConfiguration": {
                        "ApplicationRestoreConfiguration": {
                            "ApplicationRestoreType": "SKIP_RESTORE_FROM_SNAPSHOT"
                        },
                        "FlinkRunConfiguration": {"AllowNonRestoredState": True},
                    },
                },
                physical_resource_id=cr.PhysicalResourceId.of(f"Start-{app_name}"),
            ),
            policy=cr.AwsCustomResourcePolicy.from_sdk_calls(
                resources=cr.AwsCustomResourcePolicy.ANY_RESOURCE
            ),
        )
        start_on_create.node.add_dependency(cw_logs)
