from kafka import KafkaProducer
import json
import os
import pandas as pd

class KafkaProducr:
    def __init__(self,location,bootstrap_servers,topic):
        self.location = location
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.producer_client = KafkaProducer(bootstrap_servers=self.bootstrap_servers)

    def read_csv(self):
        try:
            return pd.read_csv(self.location)
        except Exception as ex:
            raise ValueError('Error with read_csv function ', ex)

    def send_message(self, data):
        self.producer_client.send(self.topic, data.encode('utf-8') )
        self.producer_client.flush()

    def kafka_producer(self):
        try:
            df = self.read_csv()
            df.apply(lambda x: self.send_message(json.dumps(x.to_json())), axis=1)
        except Exception as ex:
            raise ValueError('Error with kafka_producer function ', ex)

if __name__ == '__main__':
    # Could be stored in config.json file
    dir_path = os.path.dirname(os.path.realpath(__file__))
    location = dir_path + '/part2.json'
    bootstrap_servers='localhost:9092'
    topic='part_1'
    try:
        kpc = KafkaProducr(location, bootstrap_servers,topic)
        kpc.kafka_producer()
        print('sent')
    except Exception as ex:
        raise ValueError('Error with producer and send to slack/email',ex)

