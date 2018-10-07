import time
import kafka

# Prepare msg to send to kafka
topic_name = "abc"
payload = b"Hello World"

# Initiate a Producer which send the Hello world message
producer = kafka.KafkaProducer(bootstrap_servers='localhost:29092')
producer.send(topic=topic_name, payload)

# Initiate the Consumer which will get the msg from Kafka
consumer = kafka.KafkaConsumer(topic_name, auto_offset_reset='earliest', bootstrap_servers=['localhost:29092'])

# Print our messages

for msg in consumer:
    print(msg.value) # => b"Hello World"