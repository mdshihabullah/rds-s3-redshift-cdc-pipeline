#!/bin/bash

#########################################################################################################
# Create Topics on MSK Serverless Cluster
# Purpose: Create required topics on the Serverless cluster
# Usage: ./create-topics-serverless.sh
# Note: This requires Kafka client tools (kafka-topics.sh) or AWS Console
#########################################################################################################

set -e

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

AWS_REGION=$(aws configure get region 2>/dev/null || echo "us-east-1")
CONFIG_FILE="msk-migration-config.json"

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

# Check if config file exists
check_config() {
    if [[ ! -f "${CONFIG_FILE}" ]]; then
        print_error "Configuration file not found: ${CONFIG_FILE}"
        exit 1
    fi
    
    local bootstrap=$(jq -r '.serverless_cluster.bootstrap_servers' "${CONFIG_FILE}")
    
    if [[ -z "${bootstrap}" ]] || [[ "${bootstrap}" == "null" ]]; then
        print_error "Bootstrap servers not found in config"
        print_info "Please run ./create-msk-serverless-cluster.sh first"
        exit 1
    fi
    
    print_success "Configuration loaded"
}

# Check for Kafka client tools
check_kafka_tools() {
    if command -v kafka-topics.sh &> /dev/null; then
        print_success "Kafka client tools found"
        return 0
    elif command -v kafka-topics &> /dev/null; then
        print_success "Kafka client tools found (kafka-topics)"
        return 0
    else
        print_warning "Kafka client tools not found in PATH"
        print_info "You can:"
        echo "  1. Install Kafka client tools"
        echo "  2. Use AWS Console to create topics"
        echo "  3. Use AWS SDK/CLI (if available)"
        echo ""
        return 1
    fi
}

# Create topics using Kafka client tools
create_topics_with_kafka() {
    local bootstrap=$(jq -r '.serverless_cluster.bootstrap_servers' "${CONFIG_FILE}")
    local topics=$(jq -r '.topics[]' "${CONFIG_FILE}")
    
    print_header "Creating Topics on Serverless Cluster"
    echo ""
    print_info "Bootstrap Servers: ${bootstrap}"
    echo ""
    
    # Determine which kafka-topics command to use
    local kafka_cmd=""
    if command -v kafka-topics.sh &> /dev/null; then
        kafka_cmd="kafka-topics.sh"
    elif command -v kafka-topics &> /dev/null; then
        kafka_cmd="kafka-topics"
    fi
    
    # MSK Serverless uses IAM authentication
    # You'll need to set up IAM credentials in your Kafka client config
    print_warning "Note: MSK Serverless requires IAM authentication"
    print_info "Ensure your Kafka client is configured with IAM credentials"
    echo ""
    
    for topic in $topics; do
        print_info "Creating topic: ${topic}"
        
        # Try to create topic
        if ${kafka_cmd} --bootstrap-server "${bootstrap}" \
            --command-config /tmp/msk-iam-config.properties \
            --create \
            --topic "${topic}" \
            --partitions 3 \
            --replication-factor 1 \
            --if-not-exists 2>/dev/null; then
            print_success "Topic created: ${topic}"
        else
            print_warning "Failed to create topic via CLI (may already exist or need IAM config)"
            print_info "Topic: ${topic}"
        fi
    done
    
    echo ""
}

# Generate IAM config file for Kafka client
generate_iam_config() {
    print_info "Generating IAM config for Kafka client..."
    
    cat > /tmp/msk-iam-config.properties <<EOF
security.protocol=SASL_SSL
sasl.mechanism=AWS_MSK_IAM
sasl.jaas.config=software.amazon.msk.auth.iam.IAMLoginModule required;
sasl.client.callback.handler.class=software.amazon.msk.auth.iam.IAMClientCallbackHandler
EOF
    
    print_success "IAM config created at: /tmp/msk-iam-config.properties"
    print_warning "Note: You need AWS MSK IAM auth library in your Kafka client classpath"
}

# Main execution
main() {
    print_header "Create Topics on MSK Serverless"
    echo ""
    
    check_config
    
    # List topics that need to be created
    print_info "Topics to create:"
    jq -r '.topics[]' "${CONFIG_FILE}" | while read -r topic; do
        echo "  - ${topic}"
    done
    echo ""
    
    if check_kafka_tools; then
        generate_iam_config
        echo ""
        print_warning "Creating topics requires:"
        echo "  1. AWS MSK IAM auth library in classpath"
        echo "  2. Proper IAM permissions"
        echo ""
        read -p "Continue with Kafka CLI? (y/N): " -n 1 -r
        echo ""
        if [[ $REPLY =~ ^[Yy]$ ]]; then
            create_topics_with_kafka
        else
            print_info "Skipping CLI topic creation"
        fi
    fi
    
    echo ""
    print_header "Manual Topic Creation Instructions"
    echo ""
    print_info "If Kafka CLI is not available, create topics via AWS Console:"
    echo ""
    echo "1. Go to: https://console.aws.amazon.com/msk/home?region=${AWS_REGION}"
    echo "2. Select your Serverless cluster"
    echo "3. Go to 'Topics' tab"
    echo "4. Click 'Create topic'"
    echo ""
    print_info "Topics to create:"
    jq -r '.topics[]' "${CONFIG_FILE}" | while read -r topic; do
        echo "  - ${topic}"
        echo "    Partitions: 3"
        echo "    Replication: 1 (Serverless handles this automatically)"
    done
    echo ""
    print_info "Or use AWS SDK/CLI if you have the MSK IAM auth library"
    echo ""
}

main "$@"


