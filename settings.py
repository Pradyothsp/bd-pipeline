TOPIC = 'csv-event-data'

SERVER_URL = 'localhost:9092'  # Kafka server URL


# CONSUMER SETTINGS
# Configure the Kafka consumer
consumer_gropu_id = 'csv-event-consumer-group'

CONSUMER_CONF = {
    'bootstrap.servers': SERVER_URL,
    'group.id': f'{consumer_gropu_id}',  # Choose a consumer group ID
    'auto.offset.reset': 'earliest'
}

KAFKA_OUPPUT_FOLDER = 'kafka_output'


# PRODUCER SETTINGS
# Configure the Kafka producer
PRODUCER_CONF = {
    'bootstrap.servers': SERVER_URL,
    'queue.buffering.max.messages': 500000  # Increase the buffer size
}

# Specify the CSV file path
INPUT_CSV_PATH = 'data/data_2019_Oct.csv'

# Set the batch size
BATCH_SIZE = 5000  # Adjust as needed
