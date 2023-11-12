TOPIC = 'csv-event-data'


# CONSUMER SETTINGS
# Configure the Kafka consumer
CONSUMER_CONF = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'csv-event-consumer-group',  # Choose a consumer group ID
    'auto.offset.reset': 'earliest'
}


# PRODUCER SETTINGS
# Configure the Kafka producer
PRODUCER_CONF = {
    'bootstrap.servers': 'localhost:9092',
    'queue.buffering.max.messages': 500000  # Increase the buffer size
}

# Specify the CSV file path
INPUT_CSV_PATH = 'data/data_2019_Oct.csv'

# Set the batch size
BATCH_SIZE = 5000  # Adjust as needed
