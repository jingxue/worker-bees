import collections
from typing import List

JobAttrT = collections.namedtuple('JobAttrT', ['ID', 'TOTAL', 'COMPLETED'])
JOB_ATTR = JobAttrT('id', 'total', 'completed')

ChunkAttrT = collections.namedtuple('ChunkAttrT', ['CHUNK_ID', 'PAYLOAD'])
CHUNK_ATTR = ChunkAttrT('chunk_id', 'payload')


class Channel:
    def send(self, job_id: str, chunk_payloads: List[map]):
        pass


class Repo:
    def save(self, job: map):
        pass

    def load(self, job_id: str) -> map:
        pass
