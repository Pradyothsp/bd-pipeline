import io
from uuid import uuid4

import pyarrow as pa
import pyarrow.orc as orc
from confluent_kafka import Consumer, KafkaError

from log import logger

# Configure the Kafka consumer
conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'my_consumer_group',  # Choose a consumer group ID
    'auto.offset.reset': 'earliest'
}
consumer = Consumer(conf)

# Subscribe to the Kafka topic
topic = 'quickstart-events'  # Replace with the actual topic name
consumer.subscribe([topic])

# Create an Arrow schema based on your data
schema = pa.schema([
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

# # Create an ORC file writer
# output_stream = io.BytesIO()
# orc_writer = orc.RecordBatchFileWriter(output_stream, schema)


try:
    while True:
        msg = consumer.poll(timeout=1000)  # Adjust the timeout as needed
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

            # # Process each line separately
            # rows = []

            for line in data:

                row = line.split(',')

                # Check if the third element can be converted to an integer (assuming product_id is an integer)
                try:
                    int(row[2])
                except ValueError:
                    # If it can't be converted, it's likely a header line, so skip processing
                    continue

                # Convert string values to appropriate types based on the schema
                row = (
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
                # Append each value to the corresponding column list
                for i, value in enumerate(row):
                    columns[schema[i].name].append(value)

            # Create a pyarrow.Table from the columns dictionary
            table = pa.table(columns)

            uuid = uuid4()

            # Write the table to ORC format
            output_file = f'output_data/output_{uuid}.orc'
            orc.write_table(table, output_file)
            logger.info(f'Wrote ORC file to {output_file}')


except KeyboardInterrupt:
    pass
finally:
    # Close the Kafka consumer
    consumer.close()
