# cdk/stacks/vpc_stack.py
from aws_cdk import (
    Stack,
    aws_ec2 as ec2,
    CfnOutput
)
from constructs import Construct

class VpcStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, config: dict, **kwargs):
        super().__init__(scope, construct_id, **kwargs)
        
        # Create VPC
        self.vpc = ec2.Vpc(
            self, "FinancialPipelineVpc",
            vpc_name=f"{construct_id}-vpc",
            ip_addresses=ec2.IpAddresses.cidr("10.0.0.0/16"),
            max_azs=2,
            subnet_configuration=[
                # Public subnets for ALB
                ec2.SubnetConfiguration(
                    name="PublicSubnet",
                    subnet_type=ec2.SubnetType.PUBLIC,
                    cidr_mask=24
                ),
                # Private subnets for ECS tasks
                ec2.SubnetConfiguration(
                    name="PrivateSubnet",
                    subnet_type=ec2.SubnetType.PRIVATE_WITH_EGRESS,
                    cidr_mask=24
                ),
                # Isolated subnets for database
                ec2.SubnetConfiguration(
                    name="DatabaseSubnet",
                    subnet_type=ec2.SubnetType.PRIVATE_ISOLATED,
                    cidr_mask=24
                )
            ],
            enable_dns_hostnames=True,
            enable_dns_support=True
        )
        
        # VPC Flow Logs (for monitoring and security)
        flow_log_role = ec2.FlowLogDestination.to_cloud_watch_logs()
        self.vpc.add_flow_log(
            "FlowLog",
            destination=flow_log_role
        )
        
        # Outputs
        CfnOutput(
            self, "VpcId",
            value=self.vpc.vpc_id,
            description="VPC ID"
        )
        
        CfnOutput(
            self, "VpcCidr",
            value=self.vpc.vpc_cidr_block,
            description="VPC CIDR Block"
        )