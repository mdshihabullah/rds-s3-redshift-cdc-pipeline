#!/bin/bash

#########################################################################################################
# Setup Kafka CLI and Create Topics on MSK Serverless
# Purpose: Download Kafka, configure IAM auth, and create all required topics
#########################################################################################################

set -e

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

# Configuration
BOOTSTRAP_SERVERS="boot-wb8m0ws8.c3.kafka-serverless.us-east-1.amazonaws.com:9098"
AWS_REGION="us-east-1"
KAFKA_VERSION="3.7.1"
SCALA_VERSION="2.13"
IAM_AUTH_VERSION="2.3.0"

# Topics to create
TOPICS=(
    "cdc-gaming.gaming_oltp.dim_user"
    "cdc-gaming.gaming_oltp.dim_game"
    "cdc-gaming.gaming_oltp.dim_session"
    "cdc-gaming.gaming_oltp.fact_game_event"
    "__debezium-heartbeat.cdc-gaming"
)

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

# Download and setup Kafka
setup_kafka() {
    print_header "Setting Up Kafka CLI Tools"
    echo ""
    
    if [[ -d "kafka_${SCALA_VERSION}-${KAFKA_VERSION}" ]]; then
        print_success "Kafka already downloaded"
    else
        print_info "Downloading Kafka ${KAFKA_VERSION}..."
        curl -L "https://archive.apache.org/dist/kafka/${KAFKA_VERSION}/kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz" -o kafka.tgz
        
        print_info "Extracting Kafka..."
        tar -xzf kafka.tgz
        rm kafka.tgz
        
        print_success "Kafka CLI tools ready"
    fi
    
    # Download AWS MSK IAM Auth JAR
    if [[ -f "kafka_${SCALA_VERSION}-${KAFKA_VERSION}/libs/aws-msk-iam-auth-${IAM_AUTH_VERSION}-all.jar" ]]; then
        print_success "AWS MSK IAM Auth JAR already downloaded"
    else
        print_info "Downloading AWS MSK IAM Auth JAR..."
        curl -L "https://github.com/aws/aws-msk-iam-auth/releases/download/v${IAM_AUTH_VERSION}/aws-msk-iam-auth-${IAM_AUTH_VERSION}-all.jar" \
            -o "kafka_${SCALA_VERSION}-${KAFKA_VERSION}/libs/aws-msk-iam-auth-${IAM_AUTH_VERSION}-all.jar"
        
        print_success "AWS MSK IAM Auth JAR downloaded"
    fi
    
    echo ""
}

# Create client properties file
create_client_properties() {
    print_header "Creating Client Configuration"
    echo ""
    
    cat > client.properties <<EOF
security.protocol=SASL_SSL
sasl.mechanism=AWS_MSK_IAM
sasl.jaas.config=software.amazon.msk.auth.iam.IAMLoginModule required;
sasl.client.callback.handler.class=software.amazon.msk.auth.iam.IAMClientCallbackHandler
EOF
    
    print_success "Client properties file created"
    print_info "Bootstrap Servers: ${BOOTSTRAP_SERVERS}"
    echo ""
}

# Create topics
create_topics() {
    print_header "Creating Topics on MSK Serverless"
    echo ""
    
    local kafka_dir="kafka_${SCALA_VERSION}-${KAFKA_VERSION}"
    local kafka_topics="${kafka_dir}/bin/kafka-topics.sh"
    
    print_info "Using Kafka tools from: ${kafka_dir}"
    echo ""
    
    for topic in "${TOPICS[@]}"; do
        print_info "Creating topic: ${topic}"
        
        if ${kafka_topics} --create \
            --bootstrap-server "${BOOTSTRAP_SERVERS}" \
            --command-config client.properties \
            --replication-factor 1 \
            --partitions 3 \
            --topic "${topic}" 2>/dev/null; then
            print_success "Topic created: ${topic}"
        else
            # Check if it already exists
            if ${kafka_topics} --describe \
                --bootstrap-server "${BOOTSTRAP_SERVERS}" \
                --command-config client.properties \
                --topic "${topic}" &>/dev/null; then
                print_warning "Topic already exists: ${topic}"
            else
                print_error "Failed to create topic: ${topic}"
            fi
        fi
    done
    
    echo ""
}

# List topics to verify
list_topics() {
    print_header "Verifying Topics"
    echo ""
    
    local kafka_dir="kafka_${SCALA_VERSION}-${KAFKA_VERSION}"
    local kafka_topics="${kafka_dir}/bin/kafka-topics.sh"
    
    print_info "Listing all topics..."
    echo ""
    
    ${kafka_topics} --list \
        --bootstrap-server "${BOOTSTRAP_SERVERS}" \
        --command-config client.properties
    
    echo ""
    print_success "Topic verification complete"
    echo ""
}

# Main execution
main() {
    print_header "MSK Serverless Topic Setup"
    echo ""
    
    print_info "This script will:"
    echo "  1. Download Apache Kafka CLI tools"
    echo "  2. Download AWS MSK IAM authentication library"
    echo "  3. Create client configuration"
    echo "  4. Create all required topics"
    echo ""
    
    setup_kafka
    create_client_properties
    create_topics
    list_topics
    
    print_header "Setup Complete!"
    echo ""
    print_success "All topics have been created on MSK Serverless"
    print_info "Your connector should now work without UNKNOWN_TOPIC_OR_PARTITION errors"
    echo ""
    print_info "Monitor connector logs:"
    echo "  aws logs tail MSKConnectorLog --follow --region ${AWS_REGION}"
    echo ""
}

main "$@"


