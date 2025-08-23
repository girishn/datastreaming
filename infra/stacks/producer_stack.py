import os
from aws_cdk import (
    Stack,
    Duration,
    aws_lambda as _lambda,
    aws_iam as iam,
    aws_events as events,
    aws_events_targets as targets,
)
from constructs import Construct

class ProducerStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, stream, **kwargs):
        super().__init__(scope, construct_id, **kwargs)

        lambda_fn = _lambda.Function(
            self, "ProducerFunction",
            runtime=_lambda.Runtime.PYTHON_3_12,
            handler="app.handler",
            code=_lambda.Code.from_asset(
                os.path.join("..", "src", "producer_lambda")
            ),
            timeout=Duration.seconds(15),
            environment={"STREAM_NAME": stream.stream_name},
            description="Emits events to Kinesis on a schedule"
        )

        # Least privilege: only write to the specific stream
        stream.grant_write(lambda_fn)

        # CloudWatch Logs permissions (basic)
        lambda_fn.add_to_role_policy(iam.PolicyStatement(
            actions=["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"],
            resources=["*"]
        ))

        # EventBridge schedule from context
        schedule_expr = self.node.try_get_context("schedule") or "rate(5 minutes)"
        rule = events.Rule(
            self, "ProducerSchedule",
            schedule=events.Schedule.expression(schedule_expr),
            description=f"Triggers ProducerFunction on {schedule_expr}"
        )
        rule.add_target(targets.LambdaFunction(lambda_fn))
