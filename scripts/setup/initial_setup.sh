#!/bin/bash

###############################################################################
# Initial Setup Script for Snowflake Air Quality Analytics Project
# 
# This script automates the initial setup process for the project.
# Run this script after cloning the repository.
#
# Usage:
#   ./scripts/setup/initial_setup.sh
#
# For Windows users, run in Git Bash or WSL
###############################################################################

set -e  # Exit on error

echo "========================================="
echo "Snowflake Air Quality Analytics"
echo "Initial Setup Script"
echo "========================================="
echo ""

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Function to print colored output
print_success() {
    echo -e "${GREEN}✓ $1${NC}"
}

print_error() {
    echo -e "${RED}✗ $1${NC}"
}

print_warning() {
    echo -e "${YELLOW}⚠ $1${NC}"
}

print_info() {
    echo -e "$1"
}

# Check Python installation
echo "Checking Python installation..."
if command -v python3 &> /dev/null; then
    PYTHON_CMD=python3
    print_success "Python 3 found: $(python3 --version)"
elif command -v python &> /dev/null; then
    PYTHON_VERSION=$(python --version 2>&1 | grep -oP '(?<=Python )\d')
    if [ "$PYTHON_VERSION" -ge 3 ]; then
        PYTHON_CMD=python
        print_success "Python found: $(python --version)"
    else
        print_error "Python 3.8+ is required"
        exit 1
    fi
else
    print_error "Python is not installed"
    exit 1
fi

# Check pip installation
echo ""
echo "Checking pip installation..."
if command -v pip3 &> /dev/null; then
    PIP_CMD=pip3
    print_success "pip3 found"
elif command -v pip &> /dev/null; then
    PIP_CMD=pip
    print_success "pip found"
else
    print_error "pip is not installed"
    exit 1
fi

# Create virtual environment
echo ""
echo "Creating virtual environment..."
if [ ! -d "venv" ]; then
    $PYTHON_CMD -m venv venv
    print_success "Virtual environment created"
else
    print_warning "Virtual environment already exists"
fi

# Activate virtual environment
echo ""
echo "Activating virtual environment..."
if [[ "$OSTYPE" == "msys" || "$OSTYPE" == "win32" ]]; then
    # Windows (Git Bash)
    source venv/Scripts/activate
else
    # Linux/macOS
    source venv/bin/activate
fi
print_success "Virtual environment activated"

# Upgrade pip
echo ""
echo "Upgrading pip..."
$PIP_CMD install --upgrade pip
print_success "pip upgraded"

# Install requirements
echo ""
echo "Installing Python dependencies..."
$PIP_CMD install -r requirements.txt
print_success "Core dependencies installed"

echo ""
echo "Installing Streamlit dependencies..."
$PIP_CMD install -r streamlit/requirements.txt
print_success "Streamlit dependencies installed"

# Create credentials file
echo ""
echo "Setting up configuration files..."
if [ ! -f "config/credentials.yaml" ]; then
    cp config/credentials.yaml.template config/credentials.yaml
    print_warning "Created config/credentials.yaml - Please edit with your credentials"
else
    print_info "config/credentials.yaml already exists"
fi

# Create log directory
echo ""
echo "Creating log directory..."
mkdir -p logs
print_success "Log directory created"

# Create .env file if it doesn't exist
echo ""
echo "Checking .env file..."
if [ ! -f ".env" ]; then
    cat > .env << EOF
# Snowflake Credentials
SNOWFLAKE_ACCOUNT=your-account.region
SNOWFLAKE_USER=your-username
SNOWFLAKE_PASSWORD=your-password
SNOWFLAKE_ROLE=SYSADMIN
SNOWFLAKE_WAREHOUSE=LOAD_WH
SNOWFLAKE_DATABASE=DEV_DB
SNOWFLAKE_SCHEMA=STAGE_SCH

# API Keys
AIR_QUALITY_API_KEY=your-api-key

# Environment
ENVIRONMENT=dev
LOG_LEVEL=INFO
EOF
    print_warning "Created .env file - Please edit with your credentials"
else
    print_info ".env file already exists"
fi

# Test Snowflake connection (optional)
echo ""
read -p "Do you want to test Snowflake connection? (y/n) " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    echo "Testing Snowflake connection..."
    $PYTHON_CMD python/src/utils/snowflake_connector.py
    if [ $? -eq 0 ]; then
        print_success "Connection test passed"
    else
        print_error "Connection test failed - check your credentials"
    fi
fi

# Summary
echo ""
echo "========================================="
echo "Setup Complete!"
echo "========================================="
echo ""
print_info "Next steps:"
echo "1. Edit config/credentials.yaml with your Snowflake credentials"
echo "2. Or set environment variables in .env file"
echo "3. Run DDL scripts in Snowflake (see docs/setup-guide.md)"
echo "4. Test data ingestion: python python/src/ingestion/ingest-api-data.py"
echo "5. Launch Streamlit: cd streamlit && streamlit run pages/01-air-quality-trend-city-day-level.py"
echo ""
print_info "For detailed setup instructions, see: docs/setup-guide.md"
echo ""
