from constructs import Construct
from aws_cdk import RemovalPolicy
from aws_cdk import (
    Duration,
    Stack,
    aws_sqs as sqs,
    aws_lambda as lambda_,
    aws_lambda_event_sources as lambda_event_sources,
    aws_s3 as s3,
    aws_s3_notifications as s3_notifications,
    aws_iam as iam,
    aws_dynamodb as dynamodb
)


class LambdaSqsStackStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Create SQS Queue
        queue = sqs.Queue(
            self, "LambdaSqsStackQueue",
            visibility_timeout=Duration.seconds(300),
        )

        # Create S3 Bucket
        bucket = s3.Bucket(self, "MyUploadBucket",
                           removal_policy=RemovalPolicy.DESTROY)

        # Create DynamoDB Table
        table = dynamodb.Table(self, "ProcessedDataTable",
                               partition_key=dynamodb.Attribute(name="user_id", type=dynamodb.AttributeType.STRING),
                               removal_policy=RemovalPolicy.DESTROY
                               # Only for development
                               )

        # Reference the Lambda Layer by ARN
        pandas_layer = lambda_.LayerVersion.from_layer_version_arn(
            self, "PandasLayer",
            layer_version_arn="arn:aws:lambda:us-east-1:336392948345:layer:AWSSDKPandas-Python310:26"
        )

        # Create Lambda Function
        sqs_lambda = lambda_.Function(self, "SQSLambda",
                                      handler='lambda_handler.handler',
                                      runtime=lambda_.Runtime.PYTHON_3_10,
                                      code=lambda_.Code.from_asset('lambda'),
                                      layers=[pandas_layer],  # Attach the Layer
                                      environment={  # Add environment variable for DynamoDB table
                                          'DYNAMODB_TABLE': table.table_name
                                      }
                                      )

        # Grant Lambda permission to read/write to DynamoDB
        table.grant_read_write_data(sqs_lambda)

        # Add Lambda permissions to allow S3 to invoke it
        bucket.grant_read(sqs_lambda)

        # Create event source (S3 to Lambda)
        bucket.add_event_notification(
            s3.EventType.OBJECT_CREATED,
            s3_notifications.LambdaDestination(sqs_lambda)
        )

        # Create SQS Event Source
        sqs_event_source = lambda_event_sources.SqsEventSource(queue)

        # Add SQS event source to Lambda
        sqs_lambda.add_event_source(sqs_event_source)
