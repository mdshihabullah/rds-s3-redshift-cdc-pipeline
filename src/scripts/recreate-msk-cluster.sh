#!/bin/bash

#########################################################################################################
# Recreate MSK Provisioned Cluster from Backup
# Purpose: Recreate MSK cluster using saved configuration
# Usage: ./recreate-msk-cluster.sh [config-file]
#########################################################################################################

set -e

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

AWS_REGION=$(aws configure get region 2>/dev/null || echo "us-east-1")
CONFIG_FILE="${1:-msk-cluster-backup-config.json}"

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

# Check config file
check_config() {
    if [[ ! -f "${CONFIG_FILE}" ]]; then
        print_error "Configuration file not found: ${CONFIG_FILE}"
        print_info "Please run ./backup-msk-cluster-config.sh first"
        exit 1
    fi
    
    # Validate config structure
    if ! jq -e '.cluster_config' "${CONFIG_FILE}" > /dev/null 2>&1; then
        print_error "Invalid configuration file format"
        exit 1
    fi
    
    print_success "Configuration file loaded: ${CONFIG_FILE}"
    
    # Display config summary
    local cluster_name=$(jq -r '.cluster_config.cluster_name' "${CONFIG_FILE}")
    local instance_type=$(jq -r '.cluster_config.broker_node_group.instance_type' "${CONFIG_FILE}")
    local broker_count=$(jq -r '.cluster_config.broker_node_group.broker_count' "${CONFIG_FILE}")
    
    echo ""
    print_info "Cluster Configuration:"
    echo "  Name: ${cluster_name}"
    echo "  Instance Type: ${instance_type}"
    echo "  Broker Count: ${broker_count}"
    echo ""
}

# Check if cluster already exists
check_existing() {
    local cluster_name=$(jq -r '.cluster_config.cluster_name' "${CONFIG_FILE}")
    
    print_info "Checking for existing cluster: ${cluster_name}"
    
    local existing=$(aws kafka list-clusters-v2 --region ${AWS_REGION} \
        --query "ClusterInfoList[?ClusterName=='${cluster_name}'].{ARN:ClusterArn,State:State}" \
        --output json 2>/dev/null)
    
    if [[ -n "${existing}" ]] && [[ "${existing}" != "[]" ]]; then
        local state=$(echo "$existing" | jq -r '.[0].State')
        local arn=$(echo "$existing" | jq -r '.[0].ARN')
        
        if [[ "${state}" == "ACTIVE" ]] || [[ "${state}" == "CREATING" ]]; then
            print_warning "Cluster already exists: ${cluster_name}"
            print_info "State: ${state}"
            print_info "ARN: ${arn}"
            echo ""
            read -p "Delete existing cluster and recreate? (y/N): " -n 1 -r
            echo ""
            if [[ $REPLY =~ ^[Yy]$ ]]; then
                print_info "Deleting existing cluster..."
                aws kafka delete-cluster-v2 --cluster-arn "${arn}" --region ${AWS_REGION} > /dev/null 2>&1
                
                print_info "Waiting for deletion (this may take 10-15 minutes)..."
                while true; do
                    local status=$(aws kafka describe-cluster-v2 \
                        --cluster-arn "${arn}" \
                        --region ${AWS_REGION} \
                        --query 'ClusterInfo.State' \
                        --output text 2>/dev/null || echo "DELETED")
                    
                    if [[ "${status}" == "DELETED" ]] || [[ -z "${status}" ]]; then
                        print_success "Cluster deleted"
                        break
                    fi
                    
                    echo -n "."
                    sleep 30
                done
                echo ""
            else
                print_info "Cancelled"
                exit 0
            fi
        fi
    fi
}

# Recreate cluster
recreate_cluster() {
    print_header "Recreating MSK Cluster"
    echo ""
    
    local cluster_name=$(jq -r '.cluster_config.cluster_name' "${CONFIG_FILE}")
    local kafka_version=$(jq -r '.cluster_config.kafka_version' "${CONFIG_FILE}")
    local instance_type=$(jq -r '.cluster_config.broker_node_group.instance_type' "${CONFIG_FILE}")
    local enhanced_monitoring=$(jq -r '.cluster_config.enhanced_monitoring' "${CONFIG_FILE}")
    
    # Get subnets and security groups
    local subnets=$(jq -r '.cluster_config.broker_node_group.client_subnets | join(",")' "${CONFIG_FILE}")
    local security_groups=$(jq -r '.cluster_config.broker_node_group.security_groups | join(",")' "${CONFIG_FILE}")
    
    # Get storage configuration
    local storage_info=$(jq '.cluster_config.broker_node_group.storage_info' "${CONFIG_FILE}")
    local volume_size=$(jq -r '.EBSStorageInfo.VolumeSize // 1000' <<< "$storage_info")
    local provisioned_throughput=$(jq '.EBSStorageInfo.ProvisionedThroughput' <<< "$storage_info")
    
    # Get encryption configuration
    local encryption_info=$(jq '.cluster_config.encryption' "${CONFIG_FILE}")
    
    # Get client authentication
    local client_auth=$(jq '.cluster_config.client_authentication' "${CONFIG_FILE}")
    
    # Get logging configuration
    local logging_info=$(jq '.cluster_config.logging' "${CONFIG_FILE}")
    
    print_info "Cluster Name: ${cluster_name}"
    print_info "Kafka Version: ${kafka_version}"
    print_info "Instance Type: ${instance_type}"
    print_info "Subnets: ${subnets}"
    print_info "Security Groups: ${security_groups}"
    print_info "Volume Size: ${volume_size} GB"
    echo ""
    
    # Build broker node group JSON
    local broker_node_group=$(jq -n \
        --arg instance_type "${instance_type}" \
        --arg subnets "${subnets}" \
        --arg security_groups "${security_groups}" \
        --argjson storage_info "${storage_info}" \
        "{
            InstanceType: \$instance_type,
            ClientSubnets: (\$subnets | split(\",\")),
            SecurityGroups: (\$security_groups | split(\",\")),
            StorageInfo: \$storage_info
        }")
    
    # Build provisioned configuration
    local provisioned_config=$(jq -n \
        --argjson broker_node_group "${broker_node_group}" \
        --arg kafka_version "${kafka_version}" \
        --arg enhanced_monitoring "${enhanced_monitoring}" \
        --argjson client_auth "${client_auth}" \
        "{
            BrokerNodeGroupInfo: \$broker_node_group,
            KafkaVersion: \$kafka_version,
            EnhancedMonitoring: \$enhanced_monitoring,
            ClientAuthentication: \$client_auth
        }")
    
    # Build encryption info
    local encryption_config=$(jq -n \
        --argjson encryption_info "${encryption_info}" \
        "\$encryption_info")
    
    # Build logging config
    local logging_config=$(jq -n \
        --argjson logging_info "${logging_info}" \
        "\$logging_info")
    
    print_info "Creating cluster (this takes 10-15 minutes)..."
    echo ""
    
    # Create cluster
    local response=$(aws kafka create-cluster-v2 \
        --cluster-name "${cluster_name}" \
        --cluster-type PROVISIONED \
        --provisioned "${provisioned_config}" \
        --encryption-info "${encryption_config}" \
        --logging-info "${logging_config}" \
        --region ${AWS_REGION} 2>&1)
    
    local exit_code=$?
    
    if [[ $exit_code -eq 0 ]]; then
        local cluster_arn=$(echo "$response" | jq -r '.ClusterArn' 2>/dev/null)
        
        if [[ -z "${cluster_arn}" ]] || [[ "${cluster_arn}" == "null" ]]; then
            cluster_arn=$(echo "$response" | grep -o 'arn:aws:kafka:[^"]*' | head -1)
        fi
        
        if [[ -n "${cluster_arn}" ]]; then
            print_success "Cluster creation initiated!"
            print_info "Cluster ARN: ${cluster_arn}"
            
            echo ""
            print_info "Waiting for cluster to become ACTIVE..."
            monitor_cluster_status "${cluster_arn}"
            
            return 0
        else
            print_error "Failed to extract cluster ARN"
            echo "$response"
            return 1
        fi
    else
        print_error "Failed to create cluster"
        echo "$response"
        return 1
    fi
}

# Monitor cluster status
monitor_cluster_status() {
    local cluster_arn=$1
    
    local max_attempts=60  # 15 minutes (60 * 15 seconds)
    local attempt=0
    
    while [[ $attempt -lt $max_attempts ]]; do
        local status=$(aws kafka describe-cluster-v2 \
            --cluster-arn "${cluster_arn}" \
            --region ${AWS_REGION} \
            --query 'ClusterInfo.State' \
            --output text 2>/dev/null)
        
        echo -ne "\r${BLUE}Status: ${status} (attempt $((attempt+1))/${max_attempts})${NC}   "
        
        if [[ "${status}" == "ACTIVE" ]]; then
            echo ""
            print_success "Cluster is now ACTIVE!"
            
            # Get bootstrap servers
            get_bootstrap_servers "${cluster_arn}"
            
            return 0
        elif [[ "${status}" == "FAILED" ]]; then
            echo ""
            print_error "Cluster creation FAILED!"
            return 1
        fi
        
        attempt=$((attempt+1))
        sleep 15
    done
    
    echo ""
    print_warning "Timeout waiting for cluster to become ACTIVE"
    print_info "Check status manually:"
    echo "  aws kafka describe-cluster-v2 --cluster-arn ${cluster_arn} --region ${AWS_REGION}"
    return 1
}

# Get bootstrap servers
get_bootstrap_servers() {
    local cluster_arn=$1
    
    print_info "Getting bootstrap servers..."
    
    local bootstrap=$(aws kafka get-bootstrap-brokers \
        --cluster-arn "${cluster_arn}" \
        --region ${AWS_REGION} \
        --query 'BootstrapBrokerStringSaslIam' \
        --output text 2>/dev/null)
    
    if [[ -n "${bootstrap}" ]] && [[ "${bootstrap}" != "None" ]]; then
        print_success "Bootstrap Servers: ${bootstrap}"
        echo ""
        print_info "Save these bootstrap servers for connector configuration"
    fi
}

# Main execution
main() {
    print_header "MSK Cluster Recreation from Backup"
    echo ""
    
    check_config
    check_existing
    
    echo ""
    print_warning "This will create a new MSK cluster with the same configuration"
    print_info "Estimated time: 10-15 minutes"
    echo ""
    
    read -p "Continue? (y/N): " -n 1 -r
    echo ""
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        print_info "Cancelled"
        exit 0
    fi
    
    echo ""
    
    if recreate_cluster; then
        echo ""
        print_header "Recreation Complete"
        echo ""
        print_success "Cluster has been recreated successfully!"
        echo ""
        print_info "Next steps:"
        echo "  1. Recreate topics (if needed)"
        echo "  2. Update connectors to use new cluster bootstrap servers"
        echo "  3. Test pipeline"
        echo ""
    else
        print_error "Cluster recreation failed"
        exit 1
    fi
}

main "$@"


