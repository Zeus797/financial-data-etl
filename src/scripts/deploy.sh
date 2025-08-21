#!/bin/bash
# scripts/deploy.sh

set -e

# Configuration
ENVIRONMENT=${1:-development}
AWS_REGION=${AWS_REGION:-us-east-1}
ECR_REPOSITORY_NAME="financial-pipeline-api"

echo "🚀 Deploying Financial Pipeline API to $ENVIRONMENT environment"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Check prerequisites
print_status "Checking prerequisites..."

# Check if AWS CLI is installed and configured
if ! command -v aws &> /dev/null; then
    print_error "AWS CLI is not installed"
    exit 1
fi

# Check if CDK is installed
if ! command -v cdk &> /dev/null; then
    print_error "AWS CDK is not installed. Run: npm install -g aws-cdk"
    exit 1
fi

# Check if Docker is running
if ! docker info &> /dev/null; then
    print_error "Docker is not running"
    exit 1
fi

# Get AWS account ID
AWS_ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
print_status "AWS Account ID: $AWS_ACCOUNT_ID"

# Create ECR repository if it doesn't exist
print_status "Creating ECR repository if it doesn't exist..."
aws ecr describe-repositories --repository-names $ECR_REPOSITORY_NAME --region $AWS_REGION &> /dev/null || {
    print_status "Creating ECR repository: $ECR_REPOSITORY_NAME"
    aws ecr create-repository --repository-name $ECR_REPOSITORY_NAME --region $AWS_REGION
}

# Build and push Docker image
print_status "Building and pushing Docker image..."
./src/scripts/build-and-push.sh $ENVIRONMENT

# Install CDK dependencies
print_status "Installing CDK dependencies..."
cd cdk
pip install -r requirements.txt

# Bootstrap CDK (if needed)
print_status "Bootstrapping CDK..."
cdk bootstrap --context environment=$ENVIRONMENT

# Deploy infrastructure
print_status "Deploying CDK stacks..."
cdk deploy --all --require-approval never --context environment=$ENVIRONMENT

# Get stack outputs
print_status "Getting deployment outputs..."
LOAD_BALANCER_DNS=$(aws cloudformation describe-stacks \
    --stack-name "financial-pipeline-$ENVIRONMENT-api" \
    --query 'Stacks[0].Outputs[?OutputKey==`LoadBalancerDNS`].OutputValue' \
    --output text \
    --region $AWS_REGION)

print_status "✅ Deployment completed successfully!"
echo ""
echo "🌐 API URL: http://$LOAD_BALANCER_DNS"
echo "📊 Health Check: http://$LOAD_BALANCER_DNS/health"
echo "📖 API Docs: http://$LOAD_BALANCER_DNS/docs"
echo ""
print_warning "Note: It may take a few minutes for the load balancer to become fully available"