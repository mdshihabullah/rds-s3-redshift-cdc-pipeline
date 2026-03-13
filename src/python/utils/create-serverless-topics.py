#!/usr/bin/env python3
"""
Create topics on MSK Serverless using kafka-python-ng with IAM auth
"""

from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError
from aws_msk_iam_sasl_signer import MSKAuthTokenProvider
import sys

# MSK Serverless bootstrap
BOOTSTRAP_SERVERS = 'boot-wb8m0ws8.c3.kafka-serverless.us-east-1.amazonaws.com:9098'
REGION = 'us-east-1'

# Topics to create
TOPICS = [
    'cdc-gaming.gaming_oltp.dim_user',
    'cdc-gaming.gaming_oltp.dim_game',
    'cdc-gaming.gaming_oltp.dim_session',
    'cdc-gaming.gaming_oltp.fact_game_event',
    '__debezium-heartbeat.cdc-gaming'
]

def get_token_provider():
    """Create MSK IAM token provider"""
    return MSKAuthTokenProvider(region=REGION)

def create_topics():
    """Create topics on MSK Serverless"""
    
    print(f"Connecting to MSK Serverless: {BOOTSTRAP_SERVERS}")
    
    # Create admin client with IAM authentication
    admin_client = KafkaAdminClient(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        security_protocol='SASL_SSL',
        sasl_mechanism='OAUTHBEARER',
        sasl_oauth_token_provider=get_token_provider(),
        client_id='topic-creator'
    )
    
    print("Connected successfully!")
    
    # Create NewTopic objects
    new_topics = [
        NewTopic(
            name=topic,
            num_partitions=3,
            replication_factor=1  # Serverless manages replication
        )
        for topic in TOPICS
    ]
    
    # Create topics
    try:
        result = admin_client.create_topics(new_topics=new_topics, validate_only=False)
        
        for topic, future in result.items():
            try:
                future.result()  # Block until topic is created
                print(f"✓ Created topic: {topic}")
            except TopicAlreadyExistsError:
                print(f"ℹ Topic already exists: {topic}")
            except Exception as e:
                print(f"✗ Failed to create topic {topic}: {e}")
                
    except Exception as e:
        print(f"Error creating topics: {e}")
        return False
    
    print("\n✓ All topics created successfully!")
    
    # List topics to verify
    print("\nVerifying topics...")
    topics = admin_client.list_topics()
    cdc_topics = [t for t in topics if t.startswith('cdc-gaming') or t.startswith('__debezium')]
    print(f"CDC topics in cluster: {len(cdc_topics)}")
    for t in sorted(cdc_topics):
        print(f"  - {t}")
    
    admin_client.close()
    return True

if __name__ == '__main__':
    print("=" * 60)
    print("MSK Serverless Topic Creator")
    print("=" * 60)
    print()
    
    try:
        success = create_topics()
        sys.exit(0 if success else 1)
    except Exception as e:
        print(f"\n✗ Error: {e}")
        print("\nMake sure you have:")
        print("  1. pip install kafka-python-ng aws-msk-iam-sasl-signer-python")
        print("  2. AWS credentials configured")
        print("  3. IAM permissions for kafka-cluster:* actions")
        sys.exit(1)


