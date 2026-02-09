#!/bin/bash

# Navigate to script directory
cd "$(dirname "$0")"

# Load environment variables
if [ -f .env ]; then
    echo "🔧 Loading environment variables from .env..."
    set -a
    source .env
    set +a
else
    echo "❌ Error: .env file not found!"
    exit 1
fi

echo ""
echo "⚠️  WARNING: This will DESTROY all Terraform-managed resources!"
echo ""
terraform plan -destroy -var-file="environments/dev.tfvars"

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
read -p "Are you SURE you want to DESTROY? Type 'yes' to confirm: " destroy_response
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

if [ "$destroy_response" = "yes" ]; then
    terraform destroy -var-file="environments/dev.tfvars"
    echo "✅ Resources destroyed"
else
    echo "❌ Destroy cancelled"
fi
