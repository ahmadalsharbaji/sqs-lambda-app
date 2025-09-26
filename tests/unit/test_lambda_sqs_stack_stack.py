import aws_cdk as core
from aws_cdk import assertions
from lambda_sqs_stack.lambda_sqs_stack_stack import LambdaSqsStackStack


def test_sqs_queue_created():
    app = core.App()
    stack = LambdaSqsStackStack(app, "lambda-sqs-stack")
    template = assertions.Template.from_stack(stack)

    template.has_resource_properties("AWS::SQS::Queue", {
        "VisibilityTimeout": 300
    })


def test_lambda_created():
    app = core.App()
    stack = LambdaSqsStackStack(app, "lambda-sqs-stack")
    template = assertions.Template.from_stack(stack)

    template.has_resource_properties("AWS::Lambda::Function", {
        "Runtime": "python3.10",
        "Handler": "lambda_handler.handler",
    })

    template.has_resource_properties("AWS::Lambda::Function", {
        "Environment": {
            "Variables": {
                "DYNAMODB_TABLE": "ProcessedDataTable"
            }
        }
    })


def test_s3_bucket_created():
    app = core.App()
    stack = LambdaSqsStackStack(app, "lambda-sqs-stack")
    template = assertions.Template.from_stack(stack)

    template.has_resource_properties("AWS::S3::Bucket", {
        "DeletionPolicy": "Delete"
    })


def test_dynamodb_table_created():
    app = core.App()
    stack = LambdaSqsStackStack(app, "lambda-sqs-stack")
    template = assertions.Template.from_stack(stack)

    template.has_resource_properties("AWS::DynamoDB::Table", {
        "KeySchema": [{
            "AttributeName": "user_id",
            "KeyType": "HASH"
        }]
    })


def test_lambda_dynamodb_permissions():
    app = core.App()
    stack = LambdaSqsStackStack(app, "lambda-sqs-stack")
    template = assertions.Template.from_stack(stack)

    template.has_resource_properties("AWS::IAM::Role", {
        "PolicyDocument": {
            "Statement": [{
                "Action": [
                    "dynamodb:PutItem",
                    "dynamodb:BatchWriteItem",
                    "dynamodb:UpdateItem"
                ],
                "Effect": "Allow",
                "Resource": "*"
            }]
        }
    })


def test_sqs_event_source():
    app = core.App()
    stack = LambdaSqsStackStack(app, "lambda-sqs-stack")
    template = assertions.Template.from_stack(stack)

    template.has_resource_properties("AWS::Lambda::EventSourceMapping", {
        "EventSourceArn": {
            "Fn::GetAtt": ["LambdaSqsStackQueue", "Arn"]
        },
        "FunctionName": {
            "Ref": "SQSLambda"
        },
        "BatchSize": 5
    })


def test_s3_event_notification():
    app = core.App()
    stack = LambdaSqsStackStack(app, "lambda-sqs-stack")
    template = assertions.Template.from_stack(stack)

    template.has_resource_properties("AWS::S3::Bucket", {
        "NotificationConfiguration": {
            "LambdaConfigurations": [{
                "Event": "s3:ObjectCreated:*",
                "Function": {
                    "Ref": "SQSLambda"
                }
            }]
        }
    })
