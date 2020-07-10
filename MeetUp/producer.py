from kafka import KafkaProducer, KafkaClient
import json, requests
url='192.168.1.173:9092'
kafka = KafkaClient(bootstrap_servers=url)
producer = KafkaProducer(bootstrap_servers=["192.168.1.173:9092"])
"""
producer = SimpleProducer(kafka)
producer.send_messages('meetup',line)
"""

from contextlib import closing


r = requests.get("https://stream.meetup.com/2/rsvps", stream=True)
print (type(producer))

for line in r.iter_lines():
    if line:
        producer.send("meetup", line)
        print(line)
        print (type(line))

kafka.close()