# -*- coding: utf-8 -*-
from kafka import KafkaProducer

class Kafka(object):

    def __init__(self,servers,zookeeper=None):
        self.zookeeper = zookeeper
        self.producer = KafkaProducer(bootstrap_servers=servers)

    def send(self,topic,data):

        future = self.producer.send(
            topic,
            data
        )
        results = future.get()
        return results
