#!/bin/bash
# scripts/build-and-push.sh

set -e

ENVIRONMENT=${1:-development}
AWS_REGION=${AWS_REGION:-us-east-1}
ECR_REPOSITORY_NAME="financial-pipeline-api"

# Get AWS account ID
AWS_ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
ECR_URI="$AWS_ACCOUNT_ID.dkr.ecr.$AWS_REGION.amazonaws.com/$ECR_REPOSITORY_NAME"

echo "🐳 Building and pushing Docker image for $ENVIRONMENT"

# Login to ECR
echo "Logging in to Amazon ECR..."
aws ecr get-login-password --region $AWS_REGION | docker login --username AWS --password-stdin $ECR_URI

# Build the image
echo "Building Docker image..."
docker build -f docker/Dockerfile -t $ECR_REPOSITORY_NAME:latest .

# Tag for ECR
docker tag $ECR_REPOSITORY_NAME:latest $ECR_URI:latest
docker tag $ECR_REPOSITORY_NAME:latest $ECR_URI:$ENVIRONMENT

# Push to ECR
echo "Pushing image to ECR..."
docker push $ECR_URI:latest
docker push $ECR_URI:$ENVIRONMENT

echo "✅ Docker image pushed successfully"
echo "📦 Image URI: $ECR_URI:latest"

# Update ECS service to use new image
if [ "$ENVIRONMENT" != "local" ]; then
    echo "🔄 Updating ECS service..."
    CLUSTER_NAME="financial-pipeline-$ENVIRONMENT-cluster"
    SERVICE_NAME="financial-pipeline-$ENVIRONMENT-api-service"
    
    # Check if service exists
    if aws ecs describe-services --cluster $CLUSTER_NAME --services $SERVICE_NAME --region $AWS_REGION &> /dev/null; then
        aws ecs update-service \
            --cluster $CLUSTER_NAME \
            --service $SERVICE_NAME \
            --force-new-deployment \
            --region $AWS_REGION
        echo "✅ ECS service updated"
    else
        echo "ℹ️  ECS service not found, skipping update"
    fi
fi