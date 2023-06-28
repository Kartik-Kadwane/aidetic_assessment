import happybase
from datetime import datetime
from uuid import uuid4

# Connect to HBase
connection = happybase.Connection('localhost')
table = connection.table('clickstream_data')

def generate_row_key(timestamp):
    return datetime.now().strftime('%Y-%m-%d_%H:%M:%S.%f')

def store_in_hbase(user_id, timestamp, url, ip_address, user_agent):
    # Generate a unique row key for each click event
    row_key = generate_row_key(timestamp)

    # Create a dictionary of data to be stored in HBase
    data = {
        'click_data:user_id': user_id,
        'click_data:timestamp': timestamp,
        'click_data:url': url,
        'geo_data:ip_address': ip_address,
        'user_agent_data:user_agent': user_agent
    }

    # Store the data in HBase
    table.put(row_key, data)
