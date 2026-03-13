#!/bin/bash

#########################################################################################################
# Debezium PostgreSQL Source Connector - Creation Script
# Purpose: Create/Recreate the PostgreSQL CDC source connector for MSK Connect
# Usage: ./create-postgres-source-connector.sh [--delete-existing]
#########################################################################################################

set -e  # Exit on error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Connector Configuration (Static - you can change these)
CONNECTOR_NAME="PostgresSourceConnectorDebezium"
PROPERTIES_FILE="pg-debezium-source-connector-config.properties"

# AWS Configuration - Will be auto-discovered
AWS_REGION=""
AWS_ACCOUNT_ID=""
MSK_CLUSTER_ARN=""
MSK_BOOTSTRAP_SERVERS=""
SUBNET_1=""
SUBNET_2=""
SECURITY_GROUP=""
SERVICE_EXECUTION_ROLE_ARN=""
CUSTOM_PLUGIN_ARN=""
PLUGIN_REVISION="1"

# Kafka Connect Version
KAFKA_CONNECT_VERSION="2.7.1"

# Capacity Configuration (Auto-scaling)
MCU_COUNT="1"
MIN_WORKER_COUNT="1"
MAX_WORKER_COUNT="2"
SCALE_IN_CPU_THRESHOLD="20"
SCALE_OUT_CPU_THRESHOLD="80"

# CloudWatch Log Group
LOG_GROUP="/aws/msk-connect/${CONNECTOR_NAME}"
USE_LOG_DELIVERY="true"

#########################################################################################################
# Functions
#########################################################################################################

# Function to auto-discover AWS configuration
discover_aws_config() {
    print_info "Auto-discovering AWS configuration..."
    echo ""
    
    # 1. Get AWS Region from CLI config or default
    print_info "  [1/7] Detecting AWS Region..."
    AWS_REGION=$(aws configure get region 2>/dev/null || echo "us-east-1")
    print_success "    Region: ${AWS_REGION}"
    
    # 2. Get AWS Account ID from STS
    print_info "  [2/7] Getting AWS Account ID..."
    AWS_ACCOUNT_ID=$(aws sts get-caller-identity --query 'Account' --output text 2>/dev/null)
    if [[ -z "${AWS_ACCOUNT_ID}" ]]; then
        print_error "    Failed to get AWS Account ID. Check AWS credentials."
        exit 1
    fi
    print_success "    Account ID: ${AWS_ACCOUNT_ID}"
    
    # 3. Find MSK Cluster (look for cluster with 'cdc' or 'kafka' in name)
    print_info "  [3/7] Finding MSK Cluster..."
    local clusters=$(aws kafka list-clusters-v2 --region ${AWS_REGION} --query 'ClusterInfoList[?State==`ACTIVE`].{Name:ClusterName,Arn:ClusterArn}' --output json 2>/dev/null)
    
    if [[ -z "${clusters}" ]] || [[ "${clusters}" == "[]" ]]; then
        print_error "    No active MSK clusters found in ${AWS_REGION}"
        exit 1
    fi
    
    local cluster_count=$(echo "$clusters" | jq 'length')
    
    if [[ $cluster_count -eq 1 ]]; then
        # Only one cluster found, use it
        MSK_CLUSTER_ARN=$(echo "$clusters" | jq -r '.[0].Arn')
        local cluster_name=$(echo "$clusters" | jq -r '.[0].Name')
        print_success "    Found cluster: ${cluster_name}"
    else
        # Multiple clusters found, prompt user to select
        print_warning "    Found ${cluster_count} MSK clusters. Please select one:"
        echo ""
        
        local i=1
        while read -r name; do
            echo "      [$i] $name"
            i=$((i+1))
        done < <(echo "$clusters" | jq -r '.[].Name')
        
        echo ""
        local selection=0
        while [[ $selection -lt 1 ]] || [[ $selection -gt $cluster_count ]]; do
            read -p "      Enter selection (1-${cluster_count}): " selection
            if ! [[ "$selection" =~ ^[0-9]+$ ]]; then
                selection=0
            fi
        done
        
        MSK_CLUSTER_ARN=$(echo "$clusters" | jq -r ".[$((selection-1))].Arn")
        local cluster_name=$(echo "$clusters" | jq -r ".[$((selection-1))].Name")
        print_success "    Selected: ${cluster_name}"
    fi
    
    print_info "    ARN: ${MSK_CLUSTER_ARN}"
    
    # 4. Get MSK Bootstrap Servers
    print_info "  [4/7] Getting MSK Bootstrap Servers..."
    MSK_BOOTSTRAP_SERVERS=$(aws kafka get-bootstrap-brokers --cluster-arn "${MSK_CLUSTER_ARN}" --region ${AWS_REGION} --query 'BootstrapBrokerStringSaslIam' --output text 2>/dev/null)
    
    # Try TLS if SASL/IAM not available
    if [[ -z "${MSK_BOOTSTRAP_SERVERS}" ]] || [[ "${MSK_BOOTSTRAP_SERVERS}" == "None" ]]; then
        MSK_BOOTSTRAP_SERVERS=$(aws kafka get-bootstrap-brokers --cluster-arn "${MSK_CLUSTER_ARN}" --region ${AWS_REGION} --query 'BootstrapBrokerStringTls' --output text 2>/dev/null)
    fi
    
    if [[ -z "${MSK_BOOTSTRAP_SERVERS}" ]] || [[ "${MSK_BOOTSTRAP_SERVERS}" == "None" ]]; then
        print_error "    Failed to get bootstrap servers for cluster"
        exit 1
    fi
    print_success "    Bootstrap Servers: ${MSK_BOOTSTRAP_SERVERS}"
    
    # 5. Get Network Configuration from existing connector or MSK cluster
    print_info "  [5/7] Getting Network Configuration (VPC, Subnets, Security Groups)..."
    
    # First try to get from existing connector (any MSK Connect connector)
    local existing_connectors=$(aws kafkaconnect list-connectors --region ${AWS_REGION} --query 'connectors[?connectorState==`RUNNING`].connectorArn' --output text 2>/dev/null | head -1)
    
    if [[ -n "${existing_connectors}" ]]; then
        print_info "    Found existing connector, using its network config..."
        local connector_info=$(aws kafkaconnect describe-connector --connector-arn "${existing_connectors}" --region ${AWS_REGION} --query 'kafkaCluster.apacheKafkaCluster.vpc' --output json 2>/dev/null)
        
        SUBNET_1=$(echo "$connector_info" | jq -r '.subnets[0]' 2>/dev/null)
        SUBNET_2=$(echo "$connector_info" | jq -r '.subnets[1]' 2>/dev/null)
        SECURITY_GROUP=$(echo "$connector_info" | jq -r '.securityGroups[0]' 2>/dev/null)
    fi
    
    # If still empty, get from MSK cluster VPC
    if [[ -z "${SUBNET_1}" ]] || [[ -z "${SECURITY_GROUP}" ]]; then
        print_info "    Getting network config from MSK cluster..."
        local cluster_info=$(aws kafka describe-cluster-v2 --cluster-arn "${MSK_CLUSTER_ARN}" --region ${AWS_REGION} --output json 2>/dev/null)
        
        local vpc_id=$(echo "$cluster_info" | jq -r '.ClusterInfo.Provisioned.BrokerNodeGroupInfo.ClientSubnets[0]' | xargs -I {} aws ec2 describe-subnets --subnet-ids {} --region ${AWS_REGION} --query 'Subnets[0].VpcId' --output text 2>/dev/null)
        
        if [[ -n "${vpc_id}" ]]; then
            # Get subnets from MSK cluster
            SUBNET_1=$(echo "$cluster_info" | jq -r '.ClusterInfo.Provisioned.BrokerNodeGroupInfo.ClientSubnets[0]')
            SUBNET_2=$(echo "$cluster_info" | jq -r '.ClusterInfo.Provisioned.BrokerNodeGroupInfo.ClientSubnets[1]')
            
            # Find security group for MSK Connect (look for one with MSK or Connect in name)
            local sg_list=$(aws ec2 describe-security-groups --filters "Name=vpc-id,Values=${vpc_id}" --region ${AWS_REGION} --query 'SecurityGroups[?contains(GroupName, `MSK`) || contains(GroupName, `Connect`) || contains(GroupName, `Kafka`)].GroupId' --output text 2>/dev/null | head -1)
            
            if [[ -n "${sg_list}" ]]; then
                SECURITY_GROUP="${sg_list}"
            else
                # If no MSK-specific SG found, get default SG
                SECURITY_GROUP=$(aws ec2 describe-security-groups --filters "Name=vpc-id,Values=${vpc_id}" "Name=group-name,Values=default" --region ${AWS_REGION} --query 'SecurityGroups[0].GroupId' --output text 2>/dev/null)
            fi
        fi
    fi
    
    if [[ -z "${SUBNET_1}" ]] || [[ -z "${SECURITY_GROUP}" ]]; then
        print_error "    Failed to discover network configuration"
        print_warning "    Please ensure MSK cluster exists and has proper network setup"
        exit 1
    fi
    
    print_success "    Subnet 1: ${SUBNET_1}"
    print_success "    Subnet 2: ${SUBNET_2}"
    print_success "    Security Group: ${SECURITY_GROUP}"
    
    # 6. Find IAM Role for MSK Connect
    print_info "  [6/7] Finding IAM Role for MSK Connect..."
    
    # Try to get from existing connector first
    if [[ -n "${existing_connectors}" ]]; then
        SERVICE_EXECUTION_ROLE_ARN=$(aws kafkaconnect describe-connector --connector-arn "${existing_connectors}" --region ${AWS_REGION} --query 'serviceExecutionRoleArn' --output text 2>/dev/null)
    fi
    
    # If not found, search for role with MSKConnect in name
    if [[ -z "${SERVICE_EXECUTION_ROLE_ARN}" ]] || [[ "${SERVICE_EXECUTION_ROLE_ARN}" == "None" ]]; then
        local roles=$(aws iam list-roles --query 'Roles[?contains(RoleName, `MSK`)].RoleName' --output json 2>/dev/null)
        local role_count=$(echo "$roles" | jq -r '. | length')
        
        if [[ $role_count -eq 0 ]]; then
            print_error "    No IAM roles found with 'MSK' in name"
            print_warning "    Please create an IAM role for MSK Connect"
            exit 1
        elif [[ $role_count -eq 1 ]]; then
            local role_name=$(echo "$roles" | jq -r '.[0]')
            SERVICE_EXECUTION_ROLE_ARN="arn:aws:iam::${AWS_ACCOUNT_ID}:role/${role_name}"
        else
            # Multiple roles found, prompt user
            print_warning "    Found ${role_count} IAM roles with 'MSK' in name. Please select one:"
            echo ""
            
            local i=1
            while read -r rname; do
                echo "      [$i] $rname"
                i=$((i+1))
            done < <(echo "$roles" | jq -r '.[]')
            
            echo ""
            local selection=0
            while [[ $selection -lt 1 ]] || [[ $selection -gt $role_count ]]; do
                read -p "      Enter selection (1-${role_count}): " selection
                if ! [[ "$selection" =~ ^[0-9]+$ ]]; then
                    selection=0
                fi
            done
            
            local role_name=$(echo "$roles" | jq -r ".[$((selection-1))]")
            SERVICE_EXECUTION_ROLE_ARN="arn:aws:iam::${AWS_ACCOUNT_ID}:role/${role_name}"
        fi
    fi
    
    if [[ -z "${SERVICE_EXECUTION_ROLE_ARN}" ]] || [[ "${SERVICE_EXECUTION_ROLE_ARN}" == "None" ]]; then
        print_error "    Failed to find MSK Connect IAM role"
        print_warning "    Please create an IAM role with MSK Connect trust policy"
        exit 1
    fi
    
    local role_name=$(echo "${SERVICE_EXECUTION_ROLE_ARN}" | awk -F'/' '{print $NF}')
    print_success "    Role: ${role_name}"
    print_info "    ARN: ${SERVICE_EXECUTION_ROLE_ARN}"
    
    # 7. Find Debezium Plugin
    print_info "  [7/7] Finding Debezium PostgreSQL Plugin..."
    
    local plugins=$(aws kafkaconnect list-custom-plugins --region ${AWS_REGION} --query 'customPlugins[?contains(name, `Debezium`) || contains(name, `debezium`) || contains(name, `Postgres`) || contains(name, `postgres`)].{Name:name,Arn:customPluginArn,Revision:latestRevision.revision}' --output json 2>/dev/null)
    
    if [[ -z "${plugins}" ]] || [[ "${plugins}" == "[]" ]]; then
        print_error "    No Debezium PostgreSQL plugin found"
        print_warning "    Please create a custom plugin first"
        print_info "    See: msk-connect-plugin-setup.sh"
        exit 1
    fi
    
    local plugin_count=$(echo "$plugins" | jq 'length')
    
    if [[ $plugin_count -eq 1 ]]; then
        # Only one plugin found
        CUSTOM_PLUGIN_ARN=$(echo "$plugins" | jq -r '.[0].Arn')
        PLUGIN_REVISION=$(echo "$plugins" | jq -r '.[0].Revision')
        local plugin_name=$(echo "$plugins" | jq -r '.[0].Name')
        print_success "    Found plugin: ${plugin_name}"
    else
        # Multiple plugins found, prompt user
        print_warning "    Found ${plugin_count} Debezium plugins. Please select one:"
        echo ""
        
        local i=1
        while read -r line; do
            local pname=$(echo "$line" | jq -r '.Name')
            local prev=$(echo "$line" | jq -r '.Revision')
            echo "      [$i] $pname (revision: $prev)"
            i=$((i+1))
        done < <(echo "$plugins" | jq -c '.[]')
        
        echo ""
        local selection=0
        while [[ $selection -lt 1 ]] || [[ $selection -gt $plugin_count ]]; do
            read -p "      Enter selection (1-${plugin_count}): " selection
            if ! [[ "$selection" =~ ^[0-9]+$ ]]; then
                selection=0
            fi
        done
        
        CUSTOM_PLUGIN_ARN=$(echo "$plugins" | jq -r ".[$((selection-1))].Arn")
        PLUGIN_REVISION=$(echo "$plugins" | jq -r ".[$((selection-1))].Revision")
        local plugin_name=$(echo "$plugins" | jq -r ".[$((selection-1))].Name")
        print_success "    Selected: ${plugin_name}"
    fi
    
    print_info "    ARN: ${CUSTOM_PLUGIN_ARN}"
    print_info "    Revision: ${PLUGIN_REVISION}"
    
    echo ""
    print_success "Configuration discovery completed successfully!"
    echo ""
}

# Function to ensure CloudWatch log group exists
ensure_log_group_exists() {
    local log_group=$1
    
    print_info "Checking CloudWatch log group: ${log_group}"
    
    # Check if log group exists
    local exists=$(aws logs describe-log-groups --log-group-name-prefix "${log_group}" --region ${AWS_REGION} --query "logGroups[?logGroupName=='${log_group}'].logGroupName" --output text 2>/dev/null)
    
    if [[ -z "${exists}" ]]; then
        print_warning "Log group doesn't exist. Creating it..."
        
        aws logs create-log-group \
            --log-group-name "${log_group}" \
            --region ${AWS_REGION} 2>/dev/null
        
        if [[ $? -eq 0 ]]; then
            # Set retention to 7 days to save costs
            aws logs put-retention-policy \
                --log-group-name "${log_group}" \
                --retention-in-days 7 \
                --region ${AWS_REGION} 2>/dev/null
            
            print_success "Log group created with 7-day retention"
        else
            print_error "Failed to create log group"
            return 1
        fi
    else
        print_success "Log group exists"
    fi
    
    return 0
}

print_header() {
    echo -e "${BLUE}========================================${NC}"
    echo -e "${BLUE}$1${NC}"
    echo -e "${BLUE}========================================${NC}"
}

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
    echo -e "${BLUE}ℹ $1${NC}"
}

# Function to check if connector exists
check_connector_exists() {
    local connector_name=$1
    aws kafkaconnect list-connectors --region ${AWS_REGION} \
        --query "connectors[?connectorName=='${connector_name}'].connectorArn" \
        --output text 2>/dev/null || echo ""
}

# Function to delete existing connector
delete_existing_connector() {
    local connector_arn=$1
    print_warning "Deleting existing connector..."
    
    aws kafkaconnect delete-connector \
        --connector-arn "${connector_arn}" \
        --region ${AWS_REGION}
    
    print_info "Waiting for connector deletion to complete (this may take 2-3 minutes)..."
    
    # Wait for deletion
    while true; do
        local state=$(aws kafkaconnect describe-connector \
            --connector-arn "${connector_arn}" \
            --region ${AWS_REGION} \
            --query 'connectorState' \
            --output text 2>/dev/null || echo "DELETED")
        
        if [[ "${state}" == "DELETED" ]] || [[ -z "${state}" ]]; then
            print_success "Connector deleted successfully"
            break
        fi
        
        echo -n "."
        sleep 10
    done
    echo ""
}

# Function to convert properties file to JSON connector configuration
generate_connector_config() {
    local props_file=$1
    
    if [[ ! -f "${props_file}" ]]; then
        print_error "Properties file not found: ${props_file}"
        exit 1
    fi
    
    # Read properties and convert to JSON format
    # Skip empty lines and comments
    local config_json="{"
    local first=true
    
    while IFS='=' read -r key value || [[ -n "$key" ]]; do
        # Skip comments and empty lines
        [[ "$key" =~ ^#.*$ ]] && continue
        [[ -z "$key" ]] && continue
        
        # Remove leading/trailing whitespace
        key=$(echo "$key" | xargs)
        value=$(echo "$value" | xargs)
        
        # Skip empty values
        [[ -z "$value" ]] && continue
        
        if [[ "$first" == true ]]; then
            first=false
        else
            config_json+=","
        fi
        
        # Escape special characters in value
        value=$(echo "$value" | sed 's/"/\\"/g')
        config_json+="\"${key}\":\"${value}\""
        
    done < "${props_file}"
    
    config_json+="}"
    
    echo "$config_json"
}

# Function to create connector
create_connector() {
    local config_json=$1
    
    print_info "Creating connector with name: ${CONNECTOR_NAME}"
    print_info "Using properties from: ${PROPERTIES_FILE}"
    echo ""
    
    # Write config to temp file to avoid quoting issues
    local temp_config="/tmp/connector-config-$$.json"
    echo "$config_json" > "$temp_config"
    
    # Build command parameters
    local kafka_cluster=$(cat <<EOF
{
  "apacheKafkaCluster": {
    "bootstrapServers": "${MSK_BOOTSTRAP_SERVERS}",
    "vpc": {
      "subnets": ["${SUBNET_1}", "${SUBNET_2}"],
      "securityGroups": ["${SECURITY_GROUP}"]
    }
  }
}
EOF
)
    
    local plugins=$(cat <<EOF
[{
  "customPlugin": {
    "customPluginArn": "${CUSTOM_PLUGIN_ARN}",
    "revision": ${PLUGIN_REVISION}
  }
}]
EOF
)
    
    local capacity=$(cat <<EOF
{
  "autoScaling": {
    "mcuCount": ${MCU_COUNT},
    "minWorkerCount": ${MIN_WORKER_COUNT},
    "maxWorkerCount": ${MAX_WORKER_COUNT},
    "scaleInPolicy": {
      "cpuUtilizationPercentage": ${SCALE_IN_CPU_THRESHOLD}
    },
    "scaleOutPolicy": {
      "cpuUtilizationPercentage": ${SCALE_OUT_CPU_THRESHOLD}
    }
  }
}
EOF
)
    
    # Create connector
    print_info "Sending request to AWS..."
    
    # Determine authentication type based on bootstrap server port
    local auth_type='{"authenticationType":"NONE"}'
    if [[ "${MSK_BOOTSTRAP_SERVERS}" == *":9098"* ]]; then
        auth_type='{"authenticationType":"IAM"}'
        print_info "Using IAM authentication (detected SASL/IAM port 9098)"
    else
        print_info "Using no authentication (TLS only)"
    fi
    
    if [[ "${USE_LOG_DELIVERY}" == "true" ]]; then
        local response=$(aws kafkaconnect create-connector \
            --connector-name "${CONNECTOR_NAME}" \
            --kafka-cluster "${kafka_cluster}" \
            --kafka-cluster-client-authentication "${auth_type}" \
            --kafka-cluster-encryption-in-transit '{"encryptionType":"TLS"}' \
            --kafka-connect-version "${KAFKA_CONNECT_VERSION}" \
            --plugins "${plugins}" \
            --capacity "${capacity}" \
            --connector-configuration "file://${temp_config}" \
            --service-execution-role-arn "${SERVICE_EXECUTION_ROLE_ARN}" \
            --log-delivery "{\"workerLogDelivery\":{\"cloudWatchLogs\":{\"enabled\":true,\"logGroup\":\"${LOG_GROUP}\"}}}" \
            --region ${AWS_REGION} 2>&1)
    else
        local response=$(aws kafkaconnect create-connector \
            --connector-name "${CONNECTOR_NAME}" \
            --kafka-cluster "${kafka_cluster}" \
            --kafka-cluster-client-authentication "${auth_type}" \
            --kafka-cluster-encryption-in-transit '{"encryptionType":"TLS"}' \
            --kafka-connect-version "${KAFKA_CONNECT_VERSION}" \
            --plugins "${plugins}" \
            --capacity "${capacity}" \
            --connector-configuration "file://${temp_config}" \
            --service-execution-role-arn "${SERVICE_EXECUTION_ROLE_ARN}" \
            --region ${AWS_REGION} 2>&1)
    fi
    
    local exit_code=$?
    
    # Clean up temp file
    rm -f "$temp_config"
    
    if [[ $exit_code -eq 0 ]]; then
        local connector_arn=$(echo "$response" | jq -r '.connectorArn' 2>/dev/null)
        if [[ -z "${connector_arn}" ]] || [[ "${connector_arn}" == "null" ]]; then
            connector_arn=$(echo "$response" | grep -o 'arn:aws:kafkaconnect[^"]*' | head -1)
        fi
        
        print_success "Connector creation initiated!"
        print_info "Connector ARN: ${connector_arn}"
        echo ""
        return 0
    else
        print_error "Failed to create connector"
        echo ""
        echo "$response"
        return 1
    fi
}

# Function to monitor connector status
monitor_connector_status() {
    local connector_name=$1
    local connector_arn=$(check_connector_exists "${connector_name}")
    
    if [[ -z "${connector_arn}" ]]; then
        print_error "Connector not found: ${connector_name}"
        return 1
    fi
    
    print_info "Monitoring connector status..."
    echo ""
    
    local max_attempts=60  # 10 minutes (60 * 10 seconds)
    local attempt=0
    
    while [[ $attempt -lt $max_attempts ]]; do
        local status=$(aws kafkaconnect describe-connector \
            --connector-arn "${connector_arn}" \
            --region ${AWS_REGION} \
            --query '{State:connectorState,Workers:capacity.autoScaling.minWorkerCount}' \
            --output json 2>/dev/null)
        
        local state=$(echo "$status" | jq -r '.State')
        
        echo -ne "\r${BLUE}Status: ${state}${NC} (attempt $((attempt+1))/${max_attempts})   "
        
        if [[ "${state}" == "RUNNING" ]]; then
            echo ""
            print_success "Connector is RUNNING!"
            
            # Show snapshot status
            print_info "Checking snapshot status (this may take a few minutes)..."
            sleep 10
            
            # Check CloudWatch logs for snapshot completion
            print_info "Monitor logs with:"
            echo "  aws logs tail ${LOG_GROUP} --follow --region ${AWS_REGION}"
            echo ""
            
            return 0
        elif [[ "${state}" == "FAILED" ]]; then
            echo ""
            print_error "Connector creation FAILED!"
            
            # Get failure details
            local state_desc=$(aws kafkaconnect describe-connector \
                --connector-arn "${connector_arn}" \
                --region ${AWS_REGION} \
                --query 'stateDescription' \
                --output text 2>/dev/null)
            
            if [[ -n "${state_desc}" ]]; then
                print_error "Reason: ${state_desc}"
            fi
            
            return 1
        fi
        
        attempt=$((attempt+1))
        sleep 10
    done
    
    echo ""
    print_warning "Timeout waiting for connector to reach RUNNING state"
    print_info "Check status manually with:"
    echo "  aws kafkaconnect describe-connector --connector-arn ${connector_arn} --region ${AWS_REGION}"
    return 1
}

#########################################################################################################
# Main Execution
#########################################################################################################

main() {
    print_header "Debezium PostgreSQL Source Connector Setup"
    echo ""
    
    print_info "Note: Debezium will auto-create the replication slot if it doesn't exist"
    print_info "Slot name from config: $(grep '^slot.name=' ${PROPERTIES_FILE} 2>/dev/null | cut -d'=' -f2)"
    echo ""
    
    # Auto-discover AWS configuration
    discover_aws_config
    
    # Check if properties file exists
    if [[ ! -f "${PROPERTIES_FILE}" ]]; then
        print_error "Properties file not found: ${PROPERTIES_FILE}"
        print_info "Make sure you're running this script from the project directory"
        exit 1
    fi
    
    # Check for existing connector
    print_info "Checking for existing connector..."
    local existing_arn=$(check_connector_exists "${CONNECTOR_NAME}")
    
    if [[ -n "${existing_arn}" ]]; then
        print_warning "Connector already exists: ${CONNECTOR_NAME}"
        print_info "ARN: ${existing_arn}"
        echo ""
        
        # Check for --delete-existing flag
        if [[ "$1" == "--delete-existing" ]] || [[ "$1" == "-d" ]]; then
            delete_existing_connector "${existing_arn}"
            echo ""
            sleep 5  # Wait a bit before recreating
        else
            print_warning "Use '--delete-existing' flag to delete and recreate"
            print_info "Example: ./create-postgres-source-connector.sh --delete-existing"
            echo ""
            read -p "Delete existing connector and continue? (y/N): " -n 1 -r
            echo ""
            if [[ $REPLY =~ ^[Yy]$ ]]; then
                delete_existing_connector "${existing_arn}"
                echo ""
                sleep 5
            else
                print_info "Exiting without changes"
                exit 0
            fi
        fi
    else
        print_success "No existing connector found with name: ${CONNECTOR_NAME}"
        echo ""
    fi
    
    # Generate connector configuration from properties file
    print_info "Reading connector configuration from ${PROPERTIES_FILE}..."
    local connector_config=$(generate_connector_config "${PROPERTIES_FILE}")
    
    if [[ -z "${connector_config}" ]] || [[ "${connector_config}" == "{}" ]]; then
        print_error "Failed to read connector configuration"
        exit 1
    fi
    
    print_success "Configuration loaded successfully"
    echo ""
    
    # Ensure CloudWatch log group exists
    print_header "CloudWatch Logs Setup"
    echo ""
    
    if ! ensure_log_group_exists "${LOG_GROUP}"; then
        print_warning "Continuing without CloudWatch logs..."
        # Disable log delivery if log group creation failed
        USE_LOG_DELIVERY="false"
    else
        USE_LOG_DELIVERY="true"
    fi
    echo ""
    
    # Create the connector
    print_header "Creating Connector"
    echo ""
    
    if create_connector "${connector_config}"; then
        echo ""
        print_header "Monitoring Connector Status"
        echo ""
        
        # Wait a bit for AWS to initialize
        sleep 5
        
        # Monitor status
        if monitor_connector_status "${CONNECTOR_NAME}"; then
            echo ""
            print_header "Setup Complete!"
            echo ""
            print_success "Connector is now running and capturing CDC events"
            echo ""
            print_info "Next steps:"
            echo "  1. Verify Kafka topics are receiving data:"
            echo "     aws kafka list-topics --cluster-arn <your-msk-cluster-arn>"
            echo ""
            echo "  2. Check connector logs:"
            echo "     aws logs tail ${LOG_GROUP} --follow --region ${AWS_REGION}"
            echo ""
            echo "  3. Verify snapshot completion (check for 'Snapshot completed' in logs)"
            echo ""
        else
            print_error "Connector creation completed but status check failed"
            print_info "Check connector status manually in AWS Console or CLI"
        fi
    else
        print_error "Connector creation failed"
        exit 1
    fi
}

# Run main function
main "$@"

