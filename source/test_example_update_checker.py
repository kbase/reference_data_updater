from kafka import KafkaProducer
import json
import requests
import datetime
import os
# Setup a producer that connects to the broke
# and which which automatically encodes a python object into a json bytestream.
# kafka wants a bytestream as the value, and we want structured data so use json
kafka_broker = os.environ["KAFKA_BROKER"]
producer = KafkaProducer(bootstrap_servers=kafka_broker, value_serializer=lambda v: json.dumps(v).encode('utf-8'))

response = requests.get("https://ftp.ncbi.nlm.nih.gov/refseq/release/RELEASE_NUMBER")
lines = response.text.split("\n")
refseq_current_release_number = int(lines[0])
release_number_in_KBase = 206
if refseq_current_release_number > release_number_in_KBase:
    future = producer.send('genome_url',{"source": "ncbi",
                                         "action": "New RefSeq Release Detected: " + str(refseq_current_release_number),
                                         "timestamp": str(datetime.datetime.utcnow())})
    result = future.get(timeout=60)
    result
    
