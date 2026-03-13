#!/usr/bin/env python3
"""Create topics on MSK Serverless using Python"""

from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError
from aws_msk_iam_sasl_signer import MSKAuthTokenProvider

BOOTSTRAP_SERVERS = 'boot-wb8m0ws8.c3.kafka-serverless.us-east-1.amazonaws.com:9098'
REGION = 'us-east-1'

TOPICS = [
    'cdc-gaming.gaming_oltp.dim_user',
    'cdc-gaming.gaming_oltp.dim_game',
    'cdc-gaming.gaming_oltp.dim_session',
    'cdc-gaming.gaming_oltp.fact_game_event',
    '__debezium-heartbeat.cdc-gaming'
]

class MSKTokenProvider():
    def token(self):
        token, _ = MSKAuthTokenProvider.generate_auth_token(REGION)
        return token

def main():
    print("Connecting to MSK Serverless...")
    
    admin_client = KafkaAdminClient(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        security_protocol='SASL_SSL',
        sasl_mechanism='OAUTHBEARER',
        sasl_oauth_token_provider=MSKTokenProvider(),
        client_id='topic-creator',
    )
    
    print("✓ Connected!")
    
    new_topics = [
        NewTopic(name=topic, num_partitions=3, replication_factor=1)
        for topic in TOPICS
    ]
    
    try:
        result = admin_client.create_topics(new_topics=new_topics, validate_only=False)
        for topic, future in result.items():
            try:
                future.result()
                print(f"✓ Created: {topic}")
            except TopicAlreadyExistsError:
                print(f"ℹ Already exists: {topic}")
            except Exception as e:
                print(f"✗ Failed {topic}: {e}")
    except Exception as e:
        print(f"Error: {e}")
        return False
    
    print("\n✓ Done! Listing topics...")
    topics = admin_client.list_topics()
    cdc_topics = [t for t in topics if 'cdc-gaming' in t or 'debezium' in t]
    for t in sorted(cdc_topics):
        print(f"  - {t}")
    
    admin_client.close()
    return True

if __name__ == '__main__':
    main()


