# cdk/stacks/api_stack.py
from aws_cdk import (
    Stack,
    aws_ecs as ecs,
    aws_ecs_patterns as ecs_patterns,
    aws_ec2 as ec2,
    aws_rds as rds,
    aws_elasticloadbalancingv2 as elbv2,
    aws_logs as logs,
    aws_iam as iam,
    aws_secretsmanager as secretsmanager,
    Duration,
    CfnOutput
)
from constructs import Construct

class ApiStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, vpc: ec2.Vpc, 
                 database: rds.DatabaseInstance, config: dict, **kwargs):
        super().__init__(scope, construct_id, **kwargs)
        
        environment = config.get("environment", "development")
        
        # ECS Cluster
        cluster = ecs.Cluster(
            self, "ApiCluster",
            cluster_name=f"{construct_id}-cluster",
            vpc=vpc,
            enable_fargate_capacity_providers=True
        )
        
        # Task execution role
        execution_role = iam.Role(
            self, "TaskExecutionRole",
            assumed_by=iam.ServicePrincipal("ecs-tasks.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AmazonECSTaskExecutionRolePolicy")
            ]
        )
        
        # Task role (for application permissions)
        task_role = iam.Role(
            self, "TaskRole",
            assumed_by=iam.ServicePrincipal("ecs-tasks.amazonaws.com")
        )
        
        # Grant permissions to read database secret
        database.secret.grant_read(task_role)
        database.secret.grant_read(execution_role)
        
        # Log group for container logs
        log_group = logs.LogGroup(
            self, "ApiLogGroup",
            log_group_name=f"/ecs/{construct_id}-api",
            retention=logs.RetentionDays.ONE_WEEK if environment == "development" else logs.RetentionDays.ONE_MONTH
        )
        
        # Fargate task definition
        task_definition = ecs.FargateTaskDefinition(
            self, "ApiTaskDefinition",
            family=f"{construct_id}-api",
            cpu=512,  # 0.5 vCPU
            memory_limit_mib=1024,  # 1 GB
            execution_role=execution_role,
            task_role=task_role
        )
        
        # Container definition
        container = task_definition.add_container(
            "ApiContainer",
            image=ecs.ContainerImage.from_registry("nginx:latest"),
            port_mappings=[
                ecs.PortMapping(
                    container_port=8000,
                    protocol=ecs.Protocol.TCP
                )
            ],
            environment={
                "ENVIRONMENT": environment,
                "AWS_DEFAULT_REGION": self.region
                # ← Remove all DATABASE_* environment variables
            },
            secrets={
                # Use ONLY secrets - no environment variables with same names
                "DATABASE_USERNAME": ecs.Secret.from_secrets_manager(
                    database.secret,
                    field="username"
                ),
                "DATABASE_PASSWORD": ecs.Secret.from_secrets_manager(
                    database.secret,
                    field="password"
                ),
                "DATABASE_HOST": ecs.Secret.from_secrets_manager(
                    database.secret,
                    field="host"
                ),
                "DATABASE_PORT": ecs.Secret.from_secrets_manager(
                    database.secret,
                    field="port"
                ),
                "DATABASE_NAME": ecs.Secret.from_secrets_manager(
                    database.secret,
                    field="dbname"
                )
            },
            logging=ecs.LogDrivers.aws_logs(
                stream_prefix="api",
                log_group=log_group
            ),
            health_check=ecs.HealthCheck(
                command=["CMD-SHELL", "curl -f http://localhost:8000/health || exit 1"],
                interval=Duration.seconds(30),
                timeout=Duration.seconds(5),
                retries=3,
                start_period=Duration.seconds(60)
            )
        )
        
        # Security group for ECS tasks
        ecs_security_group = ec2.SecurityGroup(
            self, "EcsSecurityGroup",
            vpc=vpc,
            description="Security group for Financial Pipeline API",
            allow_all_outbound=True  # This allows outbound to database
        )
        
        # Fargate service with Application Load Balancer
        self.fargate_service = ecs_patterns.ApplicationLoadBalancedFargateService(
            self, "ApiService",
            cluster=cluster,
            task_definition=task_definition,
            service_name=f"{construct_id}-api-service",
            
            # Load balancer configuration
            public_load_balancer=True,
            listener_port=8000,
            protocol=elbv2.ApplicationProtocol.HTTP,
            
            # Service configuration
            desired_count=1 if environment == "development" else 2,
            
            # Networking
            assign_public_ip=False,  # Tasks run in private subnets
            
            # Health check configuration
            health_check_grace_period=Duration.seconds(60)
        )
        
        # Configure target group health check - FIXED PARAMETER NAMES
        self.fargate_service.target_group.configure_health_check(
            path="/health",
            healthy_http_codes="200",
            interval=Duration.seconds(30),  # Fixed: was health_check_interval
            timeout=Duration.seconds(5),    # Fixed: was health_check_timeout
            healthy_threshold_count=2,
            unhealthy_threshold_count=3
        )
        
        # Auto scaling configuration
        scaling = self.fargate_service.service.auto_scale_task_count(
            max_capacity=10 if environment == "production" else 3,
            min_capacity=1
        )
        
        # CPU-based scaling
        scaling.scale_on_cpu_utilization(
            "CpuScaling",
            target_utilization_percent=70,
            scale_in_cooldown=Duration.seconds(300),
            scale_out_cooldown=Duration.seconds(60)
        )
        
        # Memory-based scaling
        scaling.scale_on_memory_utilization(
            "MemoryScaling",
            target_utilization_percent=80,
            scale_in_cooldown=Duration.seconds(300),
            scale_out_cooldown=Duration.seconds(60)
        )
        
        # Outputs
        CfnOutput(
            self, "LoadBalancerDNS",
            value=self.fargate_service.load_balancer.load_balancer_dns_name,
            description="Application Load Balancer DNS name"
        )
        
        CfnOutput(
            self, "ServiceName",
            value=self.fargate_service.service.service_name,
            description="ECS Service name"
        )
        
        CfnOutput(
            self, "ClusterName",
            value=cluster.cluster_name,
            description="ECS Cluster name"
        )
        
        CfnOutput(
            self, "ApiUrl",
            value=f"http://{self.fargate_service.load_balancer.load_balancer_dns_name}",
            description="API base URL"
        )