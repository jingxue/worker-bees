import collections
from typing import List

JobAttrT = collections.namedtuple('JobAttrT', ['ID', 'TOTAL', 'COMPLETED', 'LAST_UPDATED', 'EXPIRATION'])
JOB_ATTR = JobAttrT('id', 'total', 'completed', 'last_updated', 'expiration')

ChunkAttrT = collections.namedtuple('ChunkAttrT', ['CHUNK_ID', 'PAYLOAD'])
CHUNK_ATTR = ChunkAttrT('chunk_id', 'payload')


class Channel:
    def send(self, job_id: str, chunk_payloads: List[map]):
        pass


class Repo:
    def new_job(self, job: map):
        pass

    def inc_completed(self, job: map):
        pass

    def load(self, job_id: str) -> map:
        pass
