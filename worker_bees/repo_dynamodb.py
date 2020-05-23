from datetime import datetime
from decimal import Decimal
import boto3
from boto3.dynamodb.conditions import Attr
from botocore.errorfactory import ClientError
from worker_bees.spi import Repo, JOB_ATTR


class DynamoDBRepo(Repo):
    def __init__(self, **kwargs):
        self.__dynamodb = boto3.resource('dynamodb')
        table = kwargs['table_name']
        self.__job_table = self.__dynamodb.Table(table)
        print(f'Job Table: {table}')

    def load(self, job_id: str):
        return self.__job_table.get_item(Key={JOB_ATTR.ID: job_id})['Item']

    def new_job(self, job: map):
        job[JOB_ATTR.LAST_UPDATED] = self._wrap(datetime.now())
        self.__job_table.put_item(Item=job)

    def inc_completed(self, job: map):
        while True:
            print(f'inc: {job}')
            cond = Attr(JOB_ATTR.COMPLETED).eq(job[JOB_ATTR.COMPLETED])
            upd = f'SET {JOB_ATTR.COMPLETED} = {JOB_ATTR.COMPLETED} + :inc, {JOB_ATTR.LAST_UPDATED} = :now'
            args = {
                ':inc': 1,
                ':now': self._wrap(datetime.now())
            }
            try:
                resp = self.__job_table.update_item(
                    Key={JOB_ATTR.ID: job[JOB_ATTR.ID]},
                    UpdateExpression=upd,
                    ConditionExpression=cond,
                    ReturnValues='UPDATED_NEW',
                    ExpressionAttributeValues=args)
                print(f'RESPONSE: {resp}')
                return int(resp['Attributes'][JOB_ATTR.COMPLETED])
            except ClientError as e:
                if e.response['Error']['Code'] == 'ConditionalCheckFailedException':
                    print(f'Optimistic locking failed. Try again:{e.response["Error"]}')
                    job = self.load(job[JOB_ATTR.ID])
                else:
                    raise e

    @staticmethod
    def _wrap(ts: datetime):
        return Decimal(str(ts.timestamp()))


impl = DynamoDBRepo
