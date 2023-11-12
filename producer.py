import csv
import time

from confluent_kafka import Producer

from log import logger


def delivery_report(err, msg):
    if err is not None:
        logger.error('Message delivery failed: {}'.format(err))
    else:
        logger.info('Message delivered to {} [{}]'.format(
            msg.topic(), msg.partition()))


# Configure the Kafka producer
conf = {
    'bootstrap.servers': 'localhost:9092',
    'queue.buffering.max.messages': 500000  # Increase the buffer size
    # Adjust the maximum time a message can linger in the queue
    # 'queue.buffering.max.ms': 1000
}
producer = Producer(conf)

# Specify the topic
topic = 'quickstart-events'  # Replace with the actual topic name

# Specify the CSV file path
# Replace with the actual path to your CSV file
csv_file_path = 'data/data_2019_Oct.csv'

# Set the batch size
batch_size = 5000  # Adjust as needed

# Read data from the CSV file and produce messages in batches
with open(csv_file_path, 'r') as file:
    reader = csv.reader(file)
    rows = []

    for row in reader:
        rows.append(','.join(row))

        if len(rows) == batch_size:
            # Produce the batch of messages
            messages = '\n'.join(rows)
            producer.produce(topic, value=messages, callback=delivery_report)

            rows = []
            time.sleep(0.07)  # Introduce a slight delay

            producer.flush()

    # Produce any remaining messages in the last batch
    if rows:
        messages = '\n'.join(rows)
        producer.produce(topic, value=messages, callback=delivery_report)

# Wait for any outstanding messages to be delivered and delivery reports to be received
producer.flush()
