#!/usr/bin/env python3

import aws_cdk as cdk

from lambda_sqs_stack.lambda_sqs_stack_stack import LambdaSqsStackStack


app = cdk.App()
LambdaSqsStackStack(app, "lambda-sqs-stack")

app.synth()
