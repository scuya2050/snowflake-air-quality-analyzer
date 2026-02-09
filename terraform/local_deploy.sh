#!/bin/bash

# Navigate to script directory
cd "$(dirname "$0")"

# Load environment variables from .env file
if [ -f .env ]; then
    echo "🔧 Loading environment variables from .env..."
    set -a
    source .env
    set +a
    echo "✅ Environment variables loaded"
else
    echo "❌ Error: .env file not found!"
    echo "Please copy .env.example to .env and fill in your credentials."
    exit 1
fi

# # Verify required variables are set
# if [ -z "$TF_VAR_sf_org_name" ] || [ -z "$TF_VAR_sf_account_name" ]; then
#     echo "❌ Error: Required variables not set in .env file"
#     echo "Required: TF_VAR_sf_org_name, TF_VAR_sf_account_name"
#     exit 1
# fi

echo ""
echo "🚀 Starting Terraform deployment..."
echo ""

# Initialize Terraform
echo "📦 Initializing Terraform..."
terraform init
if [ $? -ne 0 ]; then
    echo "❌ Terraform init failed!"
    exit 1
fi

# Validate configuration
echo ""
echo "✅ Validating configuration..."
terraform validate
if [ $? -ne 0 ]; then
    echo "❌ Validation failed!"
    exit 1
fi

# Format check
echo ""
echo "📝 Checking formatting..."
terraform fmt -check -recursive
if [ $? -ne 0 ]; then
    echo "⚠️  Code needs formatting. Auto-formatting..."
    terraform fmt -recursive
fi

# Plan
echo ""
echo "📋 Running Terraform plan..."
terraform plan -var-file="environments/dev.tfvars"
if [ $? -ne 0 ]; then
    echo "❌ Plan failed!"
    exit 1
fi

# Prompt for apply
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "Review the plan above."
read -p "Do you want to APPLY these changes? (yes/no): " apply_response
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

if [ "$apply_response" = "yes" ]; then
    echo ""
    echo "🚀 Applying changes..."
    terraform apply -var-file="environments/dev.tfvars"
    
    if [ $? -eq 0 ]; then
        echo ""
        echo "✅ Apply successful!"
        echo ""
        echo "📊 Outputs:"
        terraform output
    else
        echo "❌ Apply failed!"
        exit 1
    fi
else
    echo "❌ Apply cancelled."
    exit 0
fi

# Optional: Ask about showing state
echo ""
read -p "Do you want to view the state? (yes/no): " state_response
if [ "$state_response" = "yes" ]; then
    terraform show
fi

echo ""
echo "✅ Deployment complete!"