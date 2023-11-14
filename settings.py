TOPIC = 'csv-event-data'

SERVER_URL = 'localhost:9092'  # Kafka server URL


# PRODUCER SETTINGS
# Configure the Kafka producer
PRODUCER_CONF = {
    'bootstrap.servers': SERVER_URL,
    'queue.buffering.max.messages': 500000  # Increase the buffer size
}

# Specify the CSV file path
INPUT_CSV_PATH = ['data/data_2019_Oct.csv', 'data/data_2019_Nov.csv']

# Set the batch size
BATCH_SIZE = 5000  # Adjust as needed


# CONSUMER SETTINGS
# Configure the Kafka consumer
CONSUMER_GROUP_ID = 'csv-event-consumer-group'

CONSUMER_CONF = {
    'bootstrap.servers': SERVER_URL,
    'group.id': CONSUMER_GROUP_ID,  # Choose a consumer group ID
    'auto.offset.reset': 'earliest'
}

KAFKA_OUPPUT_FOLDER = 'kafka_output'
