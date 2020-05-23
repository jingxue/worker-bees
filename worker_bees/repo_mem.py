import datetime
from worker_bees.spi import Repo, JOB_ATTR


class InMemRepo(Repo):
    def __init__(self, **kwargs):
        self.__store = {}

    def new_job(self, job: map):
        self.__store[job[JOB_ATTR.ID]] = job

    def inc_completed(self, job: map):
        stored_job = self.__store[job[JOB_ATTR.ID]]
        stored_job[JOB_ATTR.COMPLETED] = stored_job[JOB_ATTR.COMPLETED] + 1
        stored_job[JOB_ATTR.LAST_UPDATED] = datetime.now()

    def load(self, job_id: str):
        return self.__store.get(job_id)


impl = InMemRepo
