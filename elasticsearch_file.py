from elasticsearch import Elasticsearch

# Connect to Elasticsearch
es = Elasticsearch(['localhost:9200'])

# Index the processed data into Elasticsearch
for _, row in aggregated_data.iterrows():
    document = {
        'url': row['url'],
        'country': row['country'],
        'unique_users': row['unique_users'],
        'clicks': row['clicks'],
        'avg_time_spent': row['avg_time_spent']
    }
    es.index(index='clickstream_data', body=document)
