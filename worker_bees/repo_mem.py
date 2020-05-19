from worker_bees.spi import Repo, JOB_ATTR


class InMemRepo(Repo):
    def __init__(self, **kwargs):
        self.__store = {}

    def save(self, job: map):
        self.__store[job[JOB_ATTR.ID]] = job

    def load(self, job_id: str):
        return self.__store.get(job_id)


impl = InMemRepo
