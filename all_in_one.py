from kafka import KafkaConsumer
import happybase
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from elasticsearch import Elasticsearch

# Kafka configuration
kafka_bootstrap_servers = 'localhost:9092'
kafka_topic = 'clickstream_topic'

# HBase configuration
hbase_host = 'localhost'
hbase_table_name = 'clickstream_data'
hbase_column_families = {
    'click_data': ['user_id', 'timestamp', 'url'],
    'geo_data': ['country', 'city'],
    'user_agent_data': ['browser', 'os', 'device']
}

# Elasticsearch configuration
es_host = 'localhost'
es_port = 9200
es_index_name = 'processed_clickstream_data'

# Create a Kafka consumer
consumer = KafkaConsumer(
    kafka_topic,
    bootstrap_servers=kafka_bootstrap_servers,
    group_id='data_pipeline_group'
)

# Connect to HBase
connection = happybase.Connection(hbase_host)
table = connection.table(hbase_table_name)

# Create a Spark session
spark = SparkSession.builder.master('local').appName('ClickstreamProcessing').getOrCreate()

# Connect to Elasticsearch
es = Elasticsearch(hosts=[{'host': es_host, 'port': es_port}])

def store_in_hbase(row_key, data):
    table.put(row_key, data)

def process_and_index_data():
    # Read clickstream data from Kafka and store in HBase
    for message in consumer:
        click_data = message.value
        row_key = str(click_data['event_id'])  # Assuming 'event_id' is the unique identifier for each click event

        hbase_data = {}
        for cf, columns in hbase_column_families.items():
            for column in columns:
                hbase_data[f'{cf}:{column}'] = str(click_data.get(column, ''))

        store_in_hbase(row_key, hbase_data)

    # Process clickstream data in Spark
    hbase_rdd = spark.sparkContext.newAPIHadoopRDD(
        'org.apache.hadoop.hbase.mapreduce.TableInputFormat',
        'org.apache.hadoop.hbase.io.ImmutableBytesWritable',
        'org.apache.hadoop.hbase.client.Result',
        conf={'hbase.zookeeper.quorum': hbase_host,
              'hbase.mapreduce.inputtable': hbase_table_name}
    )

    row_key = hbase_rdd.map(lambda x: x[0].row())
    click_data = hbase_rdd.map(lambda x: x[1]).map(lambda x: x['click_data'])
    geo_data = hbase_rdd.map(lambda x: x[1]).map(lambda x: x['geo_data'])

    processed_data = click_data.join(geo_data, 'row_key') \
        .groupBy(click_data.url, geo_data.country) \
        .agg(
            count(click_data.url).alias('clicks'),
            countDistinct(click_data.user_id).alias('unique_users'),
            avg(click_data.timestamp).alias('avg_time_spent')
        )

    # Index processed data into Elasticsearch
    processed_data.write \
        .format('org.elasticsearch.spark.sql') \
        .option('es.nodes', es_host) \
        .option('es.port', es_port) \
        .option('es.resource', es_index_name) \
        .option('es.mapping.id', 'url') \
        .mode('append') \
        .save()

# Run the data pipeline
process_and_index_data()
