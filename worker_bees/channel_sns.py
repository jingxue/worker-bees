from typing import List

import boto3
from worker_bees.spi import Channel, CHUNK_ATTR


class SnsChannel(Channel):
    def __init__(self, **kwargs):
        self.__sns = boto3.resource('sns')
        topic = kwargs['topic_name']
        self.__topic = self.__sns.get_queue_by_name(TopicName=topic)
        print(f'Topic: {topic}')

    def send(self, job_id: str, chunk_payloads: List[map]):
        [self.__topic.publish(Message=str(chunk)) for chunk in chunk_payloads]


def chunks(a_list, n):
    for i in range(0, len(a_list), n):
        yield a_list[i:i + n]


impl = SqsChannel
