# cdk/app.py
#!/usr/bin/env python3
import os
import aws_cdk as cdk
from stacks.vpc_stack import VpcStack
from stacks.database_stack import DatabaseStack
from stacks.api_stack import ApiStack

app = cdk.App()

# Get environment configuration
environment = app.node.try_get_context("environment") or "development"
config = app.node.try_get_context(environment) or {}

# AWS environment
env = cdk.Environment(
    account=os.environ.get("CDK_DEFAULT_ACCOUNT"),
    region=os.environ.get("CDK_DEFAULT_REGION", "us-east-1")
)

# Stack naming
stack_name_prefix = f"financial-pipeline-{environment}"

# VPC Stack
vpc_stack = VpcStack(
    app, 
    f"{stack_name_prefix}-vpc",
    env=env,
    config=config
)

# Database Stack
database_stack = DatabaseStack(
    app, 
    f"{stack_name_prefix}-database",
    vpc=vpc_stack.vpc,
    env=env,
    config=config
)

# API Stack
api_stack = ApiStack(
    app, 
    f"{stack_name_prefix}-api",
    vpc=vpc_stack.vpc,
    database=database_stack.database,
    env=env,
    config=config
)

# Add dependencies
database_stack.add_dependency(vpc_stack)
api_stack.add_dependency(database_stack)

app.synth()