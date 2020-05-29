from typing import List
import json
import boto3

from worker_bees.spi import Channel


class SnsChannel(Channel):
    def __init__(self, **kwargs):
        self.__sns = boto3.resource('sns')
        topic_arn = kwargs['topic_arn']
        self.__topic = self.__sns.Topic(topic_arn)
        print(f'Topic: {topic_arn}')

    def send(self, job_id: str, chunk_payloads: List[map]):
        [self.__topic.publish(Message=json.dumps(chunk)) for chunk in chunk_payloads]


def chunks(a_list, n):
    for i in range(0, len(a_list), n):
        yield a_list[i:i + n]


impl = SnsChannel
