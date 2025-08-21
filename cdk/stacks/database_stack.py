# cdk/stacks/database_stack.py
from aws_cdk import (
    Stack,
    aws_rds as rds,
    aws_ec2 as ec2,
    aws_secretsmanager as secretsmanager,
    RemovalPolicy,
    Duration,
    CfnOutput
)
from constructs import Construct

class DatabaseStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, vpc: ec2.Vpc, config: dict, **kwargs):
        super().__init__(scope, construct_id, **kwargs)
        
        environment = config.get("environment", "development")
        
        # Database credentials secret
        self.db_secret = secretsmanager.Secret(
            self, "DatabaseSecret",
            description=f"Database credentials for {construct_id}",
            generate_secret_string=secretsmanager.SecretStringGenerator(
                secret_string_template='{"username": "pipeline_admin"}',
                generate_string_key="password",
                exclude_characters=" %+~`#$&*()|[]{}:;<>?!'/@\"\\",
                password_length=32
            )
        )
        
        db_subnet_group = rds.SubnetGroup(
            self, "DatabaseSubnetGroup",
            description="Subnet group for Financial Pipeline database",
            vpc=vpc,
            vpc_subnets=ec2.SubnetSelection(
                subnet_type=ec2.SubnetType.PRIVATE_ISOLATED
            )
        )
        
        # Database security group
        db_security_group = ec2.SecurityGroup(
            self, "DatabaseSecurityGroup",
            vpc=vpc,
            description="Security group for Financial Pipeline database",
            allow_all_outbound=False
        )
        
        # Allow inbound connections from VPC CIDR (for ECS tasks)
        db_security_group.add_ingress_rule(
            peer=ec2.Peer.ipv4(vpc.vpc_cidr_block),
            connection=ec2.Port.tcp(5432),
            description="Allow connections from VPC"
        )
        
        # Database parameter group
        db_parameter_group = rds.ParameterGroup(
            self, "DatabaseParameterGroup",
            engine=rds.DatabaseInstanceEngine.postgres(
                version=rds.PostgresEngineVersion.VER_15_13
            ),
            parameters={
                "shared_preload_libraries": "pg_stat_statements",
                "log_statement": "all",
                "log_min_duration_statement": "1000",  # Log queries > 1 second
                "max_connections": "200"
            }
        )
        
        # Database instance
        instance_class = ec2.InstanceType.of(
            ec2.InstanceClass.T3,
            ec2.InstanceSize.MICRO if environment == "development" else ec2.InstanceSize.MEDIUM
        )
        
        self.database = rds.DatabaseInstance(
            self, "Database",
            instance_identifier=f"{construct_id}-postgres",
            engine=rds.DatabaseInstanceEngine.postgres(
                version=rds.PostgresEngineVersion.VER_15_13
            ),
            instance_type=instance_class,
            credentials=rds.Credentials.from_secret(self.db_secret),
            database_name="financial_pipeline",
            vpc=vpc,
            subnet_group=db_subnet_group,
            security_groups=[db_security_group],
            parameter_group=db_parameter_group,
            
            # Storage configuration
            storage_type=rds.StorageType.GP3,
            allocated_storage=20,
            max_allocated_storage=100 if environment == "production" else 50,
            
            # Backup configuration - FIXED PARAMETER NAMES
            backup_retention=Duration.days(7),
            preferred_backup_window="03:00-04:00",
            preferred_maintenance_window="sun:04:00-sun:05:00",
            
            # High availability for production
            multi_az=environment == "production",
            
            # Security
            storage_encrypted=True,
            
            # Monitoring
            monitoring_interval=Duration.seconds(60),
            enable_performance_insights=True,
            performance_insight_retention=rds.PerformanceInsightRetention.DEFAULT,
            
            # Deletion protection for production
            deletion_protection=environment == "production",
            removal_policy=RemovalPolicy.SNAPSHOT if environment == "production" else RemovalPolicy.DESTROY
        )
        
        # Allow connections from ECS tasks (will be added by API stack)
        self.db_security_group = db_security_group
        
        # Outputs
        CfnOutput(
            self, "DatabaseEndpoint",
            value=self.database.instance_endpoint.hostname,
            description="Database endpoint"
        )
        
        CfnOutput(
            self, "DatabaseSecretArn",
            value=self.db_secret.secret_arn,
            description="Database credentials secret ARN"
        )