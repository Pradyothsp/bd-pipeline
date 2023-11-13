import io
from uuid import uuid4

import pyarrow as pa
import pyarrow.orc as orc
from confluent_kafka import Consumer, KafkaError

from log import logger
from settings import CONSUMER_CONF, KAFKA_OUPPUT_FOLDER, TOPIC
from utils import timeit


def create_arrow_schema():
    # Create an Arrow schema based on your data
    return pa.schema([
        ('event_time', pa.string()),
        ('event_type', pa.string()),
        ('product_id', pa.int64()),
        ('category_id', pa.int64()),
        ('category_code', pa.string()),
        ('brand', pa.string()),
        ('price', pa.float64()),
        ('user_id', pa.int64()),
        ('user_session', pa.string())
    ])


def process_message_row(line, schema):
    # Process a single line from the message
    row = line.split(',')

    # Check if the third element can be converted to an integer (assuming product_id is an integer)
    try:
        int(row[2])

    except ValueError:
        # If it can't be converted, it's likely a header line, so skip processing
        return None

    # Convert string values to appropriate types based on the schema
    return (
        row[0],  # event_time as string
        row[1],  # event_type as string
        int(row[2]),  # product_id as int
        int(row[3]),  # category_id as int
        row[4],  # category_code as string
        row[5],  # brand as string
        float(row[6]),  # price as float
        int(row[7]),  # user_id as int
        row[8]  # user_session as string
    )


def create_table(columns):
    # Create a pyarrow.Table from the columns dictionary
    return pa.table(columns)


def write_orc_file(table):
    # Write the table to ORC format
    output_file = f'{KAFKA_OUPPUT_FOLDER}/output_{uuid4()}.orc'
    orc.write_table(table, output_file)
    return output_file


@timeit
def process_kafka_messages(consumer):
    schema = create_arrow_schema()

    # Initialize a counter for log messages
    orc_file_counter = 0

    try:
        while True:
            msg = consumer.poll(timeout=10)  # Adjust the timeout as needed

            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event
                    logger.error("Reached end of partition")
                else:
                    logger.error("Error: {}".format(msg.error()))

            else:
                data = msg.value().decode('utf-8').split('\n')

                # Process each line separately
                columns = {field.name: [] for field in schema}

                for line in data:
                    row = process_message_row(line, schema)

                    if row:
                        # Append each value to the corresponding column list

                        for i, value in enumerate(row):
                            columns[schema[i].name].append(value)

                table = create_table(columns)
                output_file = write_orc_file(table)

                # Increment the counter and log the message
                logger.info(
                    f'Wrote ORC file {orc_file_counter} to {output_file}')
                orc_file_counter += 1

    except KeyboardInterrupt:
        pass
    finally:
        # Close the Kafka consumer
        consumer.close()


if __name__ == "__main__":
    try:
        # Configure the Kafka consumer
        CONSUMER = Consumer(CONSUMER_CONF)

        # Subscribe to the Kafka topic
        CONSUMER.subscribe([TOPIC])

        process_kafka_messages(CONSUMER)

    except KafkaError as error:
        logger.error(error)
    finally:
        # Close the Kafka consumer
        if CONSUMER is not None:
            CONSUMER.close()
