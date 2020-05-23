import random
import unittest
import worker_bees.job as job

class JobTest(unittest.TestCase):
    def test_typical(self):
        chunks = [str(i) for i in range(0, 10)]
        job_id = job.kick_off({'type': 'channel_mem'}, {'type': 'repo_mem'}, {'type': 'channel_print'}, chunks)
        self.assertIsNotNone(job_id)
        print('job_id', job_id)

        chunk_indices = [i for i in range(0, 10)]
        random.shuffle(chunk_indices)
        [job.complete(f'{job_id}/{i}') for i in chunk_indices]
