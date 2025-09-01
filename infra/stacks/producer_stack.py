# infra/stacks/producer_stack.py
import os
from aws_cdk import (
    Stack,
    Duration,
    aws_lambda as _lambda,
    aws_iam as iam,
    aws_events as events,
    aws_events_targets as targets,
    aws_ecr_assets as ecr_assets,
    aws_secretsmanager as secretsmanager,
)
from constructs import Construct

class ProducerStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, stream, **kwargs):
        super().__init__(scope, construct_id, **kwargs)

        alpaca_secret = secretsmanager.Secret.from_secret_name_v2(
            self,
            id="AlpacaApiSecret",
            secret_name="AlpacaApiSecret"
        )

        lambda_fn = _lambda.DockerImageFunction(
            self, "ProducerFunction",
            code=_lambda.DockerImageCode.from_image_asset(
                directory=os.path.join("..", "src", "producer_lambda_image"),
                file="Dockerfile",
                platform=ecr_assets.Platform.LINUX_AMD64,
            ),
            architecture=_lambda.Architecture.X86_64,
            timeout=Duration.seconds(60),
            memory_size=1024,
            environment={
                "STREAM_NAME": stream.stream_name,
                "TICKER": "MSFT",
                "PERIOD": "1d",
                "INTERVAL": "1m",
                "DRY_RUN": "false",
            },
            description="Fetches ticker data (yfinance) and writes to Kinesis",
        )

        stream.grant_write(lambda_fn)
        alpaca_secret.grant_read(lambda_fn)

        lambda_fn.add_to_role_policy(iam.PolicyStatement(
            actions=["logs:CreateLogGroup", 
                     "logs:CreateLogStream", 
                     "logs:PutLogEvents", 
                     "secretsmanager:GetSecretValue", 
                     "secretsmanager:DescribeSecret", 
                     "secretsmanager:ListSecrets"],
            resources=["*"]
        ))

        schedule_expr = self.node.try_get_context("schedule") or "rate(2 minutes)"
        rule = events.Rule(
            self, "ProducerSchedule",
            schedule=events.Schedule.expression(schedule_expr),
        )
        rule.add_target(targets.LambdaFunction(lambda_fn))

