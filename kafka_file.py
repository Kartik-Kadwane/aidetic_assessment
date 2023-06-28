from kafka import KafkaConsumer
import hbase_file

# Configure Kafka consumer
consumer = KafkaConsumer(
    'clickstream_topic',
    bootstrap_servers='localhost:9092',
    group_id='data_pipeline_group'
)

# Consume messages from Kafka
for message in consumer:
    # Extract necessary fields from the consumed message
    user_id = message.value['user_id']
    timestamp = message.value['timestamp']
    url = message.value['url']
    ip_address = message.value['ip_address']
    user_agent = message.value['user_agent']

    # Store the extracted data in the data store (HBase)
    hbase_file.store_in_hbase(user_id, timestamp, url, ip_address, user_agent)
