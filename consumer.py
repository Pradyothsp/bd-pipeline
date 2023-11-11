from confluent_kafka import Consumer, KafkaError

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

try:
    while True:
        msg = consumer.poll(timeout=1000)  # Adjust the timeout as needed
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                # End of partition event
                print("Reached end of partition")
            else:
                print("Error: {}".format(msg.error()))
        else:
            # Print the received message value
            print("Received message: {}".format(msg.value().decode('utf-8')))
            print("Message length: {}".format(len(msg.value().decode('utf-8'))))
            break


except KeyboardInterrupt:
    pass
finally:
    # Close the Kafka consumer
    consumer.close()
