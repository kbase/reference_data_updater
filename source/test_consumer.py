from kafka import KafkaConsumer
import json
import os

# Setup a consumer for the genome_url topic that connects to the kafka test instance
# for the purposes of this demo, reset the index to pull from the first message in the topic
# and also deserialize the object from its json encoding
kafka_broker = os.environ["KAFKA_BROKER"]
consumer = KafkaConsumer('genome_url', bootstrap_servers=kafka_broker, auto_offset_reset=
                         #'latest',   #IF LATEST IT ONLY DOES NEW ONES, not last oner
                         'earliest',  #Does all
                         value_deserializer=lambda x: json.loads(x.decode('utf-8')))
for message in consumer:
    message = message.value;
    print("DO STUFF for this message")
    print('{}'.format(message))

# Consumer stays open keeps listening. 
