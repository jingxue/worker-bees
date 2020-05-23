import random
from typing import List

import boto3
from worker_bees.spi import Channel, CHUNK_ATTR


class SqsChannel(Channel):
    def __init__(self, **kwargs):
        self.__sqs = boto3.resource('sqs')
        job_queue = kwargs['queue_name']
        self.__job_queue = self.__sqs.get_queue_by_name(QueueName=job_queue)
        print(f'Job Queue: {job_queue}')
        self.__random_delay = kwargs['random_delay']

    def send(self, job_id: str, chunk_payloads: List[map]):
        delay = random.randrange(self.__random_delay) if self.__random_delay else 0
        messages = [
            {'Id': chunk[CHUNK_ATTR.CHUNK_ID],
             'MessageBody': str(chunk),
             'DelaySeconds': delay}
            for chunk in chunk_payloads]
        for message_batch in chunks(messages, 10):
            self.__job_queue.send_messages(Entries=message_batch)


def chunks(a_list, n):
    for i in range(0, len(a_list), n):
        yield a_list[i:i + n]


impl = SqsChannel
