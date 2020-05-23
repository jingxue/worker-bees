import unittest
import traceback
from concurrent.futures import ThreadPoolExecutor
import threading
from worker_bees.spi import JOB_ATTR
from worker_bees.job import _create_component


class TestDynamoDBRepo(unittest.TestCase):
    def setUp(self):
        self.CHUNKS = 20
        self.JOB_ID = 'ut-job'
        self.repo = _create_component(type='repo_dynamodb', table_name='swgoh.job.dev')

    def test_complete_job(self):
        job = {JOB_ATTR.ID: self.JOB_ID, JOB_ATTR.COMPLETED: 0, JOB_ATTR.TOTAL: self.CHUNKS}
        self.repo.new_job(job)
        exec = ThreadPoolExecutor(max_workers=4, thread_name_prefix='job-worker-')
        exec.map(self._do_complete, [self.JOB_ID for i in range(self.CHUNKS)])

        exec.shutdown(wait=True)
        job = self.repo.load(self.JOB_ID)
        self.assertEqual(job[JOB_ATTR.COMPLETED], self.CHUNKS)

    def _do_complete(self, job_id: str):
        try:
            job = self.repo.load(self.JOB_ID)
            print(f'{threading.current_thread().name}: {job[JOB_ATTR.COMPLETED]}')
            self.assertIsNotNone(job)
            self.repo.inc_completed(job)
        except:
            traceback.print_exc()
