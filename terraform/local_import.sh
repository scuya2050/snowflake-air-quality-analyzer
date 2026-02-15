#!/bin/bash
# ============================================================================
# Terraform State Import Script for Snowflake Resources
# ============================================================================
# This script imports existing Snowflake resources into Terraform state
# Run this after state corruption or when adopting existing infrastructure
# 
# Prerequisites:
# - Existing Snowflake resources must match Terraform configuration
# - Snowflake environment variables must be set (SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD)
# - Run from the terraform directory
# ============================================================================

# Don't exit on error - we want to try importing all resources
set +e

echo "========================================"
echo "Terraform State Import for Snowflake"
echo "========================================"
echo ""

# Load environment variables from .env file if it exists
if [ -f ".env" ]; then
    echo "Loading environment variables from .env file..."
    set -a
    source .env
    set +a
    echo "✓ Environment variables loaded"
    echo ""
else
    echo "⚠ No .env file found. Ensure SNOWFLAKE_* environment variables are set."
    echo ""
fi

# Read environment from tfvars file or use default
ENVIRONMENT="dev"
TFVARS_FILE="environments/dev.tfvars"

if [ -f "$TFVARS_FILE" ]; then
    echo "Using tfvars file: $TFVARS_FILE"
    # Parse database name from tfvars
    DATABASE_NAME=$(grep -oP 'database_name\s*=\s*"\K[^"]+' "$TFVARS_FILE" 2>/dev/null || echo "DEV_DB")
else
    DATABASE_NAME="DEV_DB"
    echo "Using default database name: $DATABASE_NAME"
fi

echo "Database: $DATABASE_NAME"
echo ""

# Initialize Terraform
echo "Step 1: Initializing Terraform..."
terraform init
if [ $? -ne 0 ]; then
    echo "✗ Failed to initialize Terraform"
    exit 1
fi
echo "✓ Terraform initialized"
echo ""

# Function to import a resource with error handling
import_resource() {
    local resource_address=$1
    local resource_id=$2
    local description=$3
    
    echo "Importing: $description"
    echo "  Address: $resource_address"
    echo "  ID: $resource_id"
    
    # Capture output to check for "already managed" message
    output=$(terraform import -var-file="$TFVARS_FILE" "$resource_address" "$resource_id" 2>&1)
    exit_code=$?
    
    if [ $exit_code -eq 0 ]; then
        echo "  ✓ Successfully imported"
        return 0
    elif echo "$output" | grep -q "already being managed"; then
        echo "  ℹ Already in state (skipped)"
        return 0
    else
        echo "  ⚠ Failed to import"
        echo "  Error: $output"
        return 1
    fi
    echo ""
}

echo "Step 2: Importing resources..."
echo ""

SUCCESS_COUNT=0
FAIL_COUNT=0
SKIP_COUNT=0

# Import Database
if import_resource "snowflake_database.main" "$DATABASE_NAME" "Database: $DATABASE_NAME"; then
    ((SUCCESS_COUNT++))
else
    ((FAIL_COUNT++))
fi

# Import Schemas
declare -a schemas=(
    "STAGE_SCH:snowflake_schema.stage_sch:Stage Schema"
    "CLEAN_SCH:snowflake_schema.clean_sch:Clean Schema"
    "CONSUMPTION_SCH:snowflake_schema.consumption_sch:Consumption Schema"
    "PUBLISH_SCH:snowflake_schema.publish_sch:Publish Schema"
)

for schema_entry in "${schemas[@]}"; do
    IFS=':' read -r schema_name address desc <<< "$schema_entry"
    schema_id="$DATABASE_NAME.$schema_name"
    if import_resource "$address" "$schema_id" "$desc"; then
        ((SUCCESS_COUNT++))
    else
        ((FAIL_COUNT++))
    fi
done

# Import Warehouses
declare -a warehouses=(
    "LOAD_WH:snowflake_warehouse.load_wh:Load Warehouse"
    "TRANSFORM_WH:snowflake_warehouse.transform_wh:Transform Warehouse"
    "STREAMLIT_WH:snowflake_warehouse.streamlit_wh:Streamlit Warehouse"
    "ADHOC_WH:snowflake_warehouse.adhoc_wh:Ad-hoc Warehouse"
)

for wh_entry in "${warehouses[@]}"; do
    IFS=':' read -r wh_name address desc <<< "$wh_entry"
    if import_resource "$address" "$wh_name" "$desc"; then
        ((SUCCESS_COUNT++))
    else
        ((FAIL_COUNT++))
    fi
done

# Import File Format
file_format_id="$DATABASE_NAME.STAGE_SCH.JSON_FILE_FORMAT"
if import_resource "snowflake_file_format.json_file_format" "$file_format_id" "JSON File Format"; then
    ((SUCCESS_COUNT++))
else
    ((FAIL_COUNT++))
fi

# Import Stage
stage_id="$DATABASE_NAME.STAGE_SCH.RAW_STG"
if import_resource "snowflake_stage.raw_stg" "$stage_id" "Raw Stage"; then
    ((SUCCESS_COUNT++))
else
    ((FAIL_COUNT++))
fi

# Summary
echo ""
echo "========================================"
echo "Import Summary"
echo "========================================"
echo "✓ Successfully imported: $SUCCESS_COUNT"
echo "⚠ Failed imports: $FAIL_COUNT"
echo ""

# Verify state
echo "Step 3: Verifying state..."
terraform state list
echo ""

echo "Step 4: Validating configuration..."
terraform plan -var-file="$TFVARS_FILE"

echo ""
echo "========================================"
echo "Import process complete!"
echo "========================================"
echo ""
echo "Next steps:"
echo "1. Review the terraform plan output above"
echo "2. If you see unwanted changes, adjust your .tf files to match existing resources"
echo "3. Run 'terraform plan' again to verify no changes"
echo "4. Your state is now synchronized with existing infrastructure!"
echo ""