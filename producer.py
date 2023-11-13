import csv
import time

from confluent_kafka import Producer

from log import logger
from settings import BATCH_SIZE, INPUT_CSV_PATH, PRODUCER_CONF, TOPIC
from utils import timeit


def delivery_report(err, msg, num_rows, message_count):
    if err is not None:
        logger.error(
            f"Failed to deliver message to {msg.topic()} [{msg.partition()}]: {err} - Message Count: {message_count[0]}")
    else:
        logger.info(
            f"Message successfully delivered to {msg.topic()} [{msg.partition()}] - Offset: {msg.offset()} - Message Length (Rows): {num_rows} - Message Count: {message_count[0]}")

    # Increment the message counter
    message_count[0] += 1


@timeit
def produce_kafka_messages():
    # Configure the Kafka producer
    PRODUCER = Producer(PRODUCER_CONF)

    # Initialize message counter
    message_count = [0]

    # Read data from the CSV file and produce messages in batches
    for csv_file in INPUT_CSV_PATH:
        with open(csv_file, 'r') as file:
            logger.debug(f"Reading data from {csv_file}")
            reader = csv.reader(file)
            rows = []

            for row in reader:
                rows.append(','.join(row))

                if len(rows) == BATCH_SIZE:
                    # Produce the batch of messages
                    messages = '\n'.join(rows)
                    PRODUCER.produce(
                        TOPIC,
                        value=messages,
                        callback=lambda err, msg,
                        rows=len(rows),
                        count=message_count: delivery_report(err, msg, rows, count)
                    )

                    rows = []
                    time.sleep(0.07)  # Introduce a slight delay

                    PRODUCER.flush()

            # Produce any remaining messages in the last batch
            if rows:
                messages = '\n'.join(rows)
                PRODUCER.produce(
                    TOPIC,
                    value=messages,
                    callback=lambda err, msg,
                    rows=len(rows),
                    count=message_count: delivery_report(err, msg, rows, count)
                )

    # Wait for any outstanding messages to be delivered and delivery reports to be received
    PRODUCER.flush()

    # Log the total number of messages sent
    logger.info(f"Total number of messages sent to Kafka: {message_count[0]}")


if __name__ == "__main__":
    try:
        produce_kafka_messages()
    except KeyboardInterrupt:
        pass
