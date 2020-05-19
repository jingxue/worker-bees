import boto3
from worker_bees.spi import Repo


class DynamoDBRepo(Repo):
    def __init__(self, **kwargs):
        self.__dynamodb = boto3.resource('dynamodb')
        job_table = kwargs['job_table']
        self.__job_table = self.__dynamodb.Table(job_table)
        print(f'Job Table: {job_table}')

    def save(self, job: map):
        self.__job_table.put_item()


impl = DynamoDBRepo
