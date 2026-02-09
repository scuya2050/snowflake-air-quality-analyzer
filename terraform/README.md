# Snowflake Infrastructure - Terraform

This directory contains Terraform configurations to deploy the complete Snowflake infrastructure for the Air Quality Data Pipeline project.

## 📁 Structure

```
terraform/
├── main.tf                      # Main infrastructure definitions
├── variables.tf                 # Variable declarations
├── outputs.tf                   # Output values
├── terraform.tfvars.example     # Example variables file
├── environments/
│   ├── dev.tfvars              # Development environment config
│   └── prod.tfvars             # Production environment config
└── README.md                    # This file
```

## 🚀 Quick Start

### Prerequisites

1. **Install Terraform** (>= 1.5.0)
   ```powershell
   # Using Chocolatey
   choco install terraform
   
   # Or download from https://www.terraform.io/downloads
   ```

2. **Set Snowflake Credentials**
   ```powershell
   $env:SNOWFLAKE_ACCOUNT = "your-account.region"
   $env:SNOWFLAKE_USER = "your-username"
   $env:SNOWFLAKE_PASSWORD = "your-password"
   ```

   Or create a `~/.snowflake/config` file:
   ```ini
   [default]
   account = your-account.region
   user = your-username
   password = your-password
   ```

### Deploy Infrastructure

#### 1. Initialize Terraform
```powershell
cd terraform
terraform init
```

#### 2. Deploy Development Environment
```powershell
# Review changes
terraform plan -var-file="environments/dev.tfvars"

# Apply changes
terraform apply -var-file="environments/dev.tfvars"
```

#### 3. Deploy Production Environment
```powershell
terraform plan -var-file="environments/prod.tfvars"
terraform apply -var-file="environments/prod.tfvars"
```

## 📦 Resources Created

### Infrastructure
- **1 Database** (`DEV_DB` or `PROD_DB`)
- **4 Schemas**:
  - `STAGE_SCH` - Raw data ingestion
  - `CLEAN_SCH` - Transformed data
  - `CONSUMPTION_SCH` - Analytics layer
  - `PUBLISH_SCH` - Data sharing layer

### Compute
- **4 Warehouses**:
  - `LOAD_WH` - Data loading (Medium/X-Small)
  - `TRANSFORM_WH` - ETL transformations (Small/X-Small)
  - `STREAMLIT_WH` - Streamlit apps (Small/X-Small)
  - `ADHOC_WH` - Development queries (X-Small)

### Storage
- **1 Internal Stage** (`RAW_STG`) - For JSON file storage
- **1 File Format** (`JSON_FILE_FORMAT`) - JSON parsing configuration
- **1 Table** (`RAW_AQI`) - Raw data landing table

### Processing
- **2 Dynamic Tables**:
  - `CLEAN_AQI_DT` - Deduplicated and flattened data
  - `CLEAN_FLATTEN_AQI_DT` - Pollutants transposed to columns
- **1 Task** (`COPY_AIR_QUALITY_DATA`) - Scheduled data ingestion

## ⚙️ Configuration

### Environment Variables

The configuration supports environment-specific settings:

| Variable | Dev | Prod | Description |
|----------|-----|------|-------------|
| `database_name` | DEV_DB | PROD_DB | Database name |
| `load_wh` size | X-SMALL | MEDIUM | Load warehouse size |
| `transform_wh` size | X-SMALL | SMALL | Transform warehouse size |
| `enable_tasks` | false | true | Enable scheduled tasks |

### Customize Variables

Edit `environments/dev.tfvars` or `environments/prod.tfvars`:

```hcl
warehouse_sizes = {
  load_wh      = "LARGE"      # Increase for heavy loads
  transform_wh = "MEDIUM"     # Increase for complex transforms
  streamlit_wh = "SMALL"
  adhoc_wh     = "X-SMALL"
}

task_schedule = "USING CRON 0 */2 * * * America/Lima"  # Every 2 hours
enable_tasks  = true
```

## 🔄 Common Operations

### View Current State
```powershell
terraform show
```

### Update Infrastructure
```powershell
terraform plan -var-file="environments/dev.tfvars"
terraform apply -var-file="environments/dev.tfvars"
```

### Destroy Infrastructure
```powershell
# ⚠️ WARNING: This will delete all resources
terraform destroy -var-file="environments/dev.tfvars"
```

### Enable/Resume Tasks
```powershell
# Set enable_tasks = true in tfvars file, then:
terraform apply -var-file="environments/dev.tfvars"
```

### View Outputs
```powershell
terraform output
terraform output -json
```

## 🔐 Security Best Practices

1. **Never commit credentials**
   - Use environment variables
   - Use Snowflake key-pair authentication
   - Use AWS Secrets Manager or similar

2. **Use remote state**
   - Uncomment backend configuration in `main.tf`
   - Store state in S3/Azure Blob/GCS

3. **Least privilege access**
   - Use appropriate Snowflake roles
   - Grant only necessary permissions

## 🛠️ Troubleshooting

### Authentication Issues
```powershell
# Verify environment variables
echo $env:SNOWFLAKE_ACCOUNT
echo $env:SNOWFLAKE_USER

# Test connection
snowsql -a $env:SNOWFLAKE_ACCOUNT -u $env:SNOWFLAKE_USER
```

### Provider Version Issues
```powershell
# Update provider
terraform init -upgrade
```

### State Lock Issues
```powershell
# Force unlock (use carefully!)
terraform force-unlock LOCK_ID
```

## 📚 Next Steps

After deploying infrastructure:

1. **Upload data to stage**:
   ```sql
   PUT file://data/raw/*.json @DEV_DB.STAGE_SCH.RAW_STG/country=India/city=Delhi/;
   ```

2. **Resume tasks** (if not auto-enabled):
   ```sql
   ALTER TASK DEV_DB.STAGE_SCH.COPY_AIR_QUALITY_DATA RESUME;
   ```

3. **Deploy Streamlit apps** using the connection info from outputs

4. **Set up dbt** for additional transformations in consumption layer

## 📖 References

- [Snowflake Terraform Provider Documentation](https://registry.terraform.io/providers/Snowflake-Labs/snowflake/latest/docs)
- [Terraform Best Practices](https://www.terraform.io/docs/cloud/guides/recommended-practices/index.html)
- [Snowflake Documentation](https://docs.snowflake.com/)
