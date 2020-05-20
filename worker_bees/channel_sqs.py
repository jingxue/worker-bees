import boto3
from worker_bees.spi import Channel


class SqsChannel(Channel):
    def __init__(self, **kwargs):
        self.__sqs = boto3.resource('sqs')
        job_queue = kwargs['job_queue']
        self.__job_queue = self.__sqs.get_queue_by_name(QueueName=job_queue)
        completion_queue = kwargs['completion_queue']
        self.__completion_queue = self.__sqs.get_queue_by_name(QueueName=completion_queue)
        print(f'Job Queue: {job_queue}, Completion Queue: {completion_queue}')

    def send(self):
        pass


impl = SqsChannel
