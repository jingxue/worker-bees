import importlib
import uuid
from worker_bees.spi import *

_job_confs = {}


def _create_component(**kwargs):
    return importlib.import_module(f'worker_bees.{kwargs["type"]}').impl(**kwargs)


def kick_off(worker_channel: map, repo: map, completion_channel: map, chunks, job_id: str = None,
             job_id_attr: str = JOB_ATTR.ID) -> str:
    if not job_id:
        job_id = str(uuid.uuid4())
    worker_channel_impl = _create_component(**worker_channel)
    repo_impl = _create_component(**repo)
    completion_channel_impl = _create_component(**completion_channel)
    conf = (worker_channel_impl, repo_impl, completion_channel_impl)
    _job_confs[job_id] = conf
    worker_channel_impl.send(job_id,
                             [{CHUNK_ATTR.CHUNK_ID: f'{job_id}/{i}', CHUNK_ATTR.PAYLOAD: chunk} for (i, chunk) in
                              enumerate(chunks)])
    repo_impl.new_job({job_id_attr: job_id,
                       JOB_ATTR.COMPLETED: 0,
                       JOB_ATTR.TOTAL: len(chunks)})
    return job_id


def complete(chunk_id: str):
    slash_i = chunk_id.rfind('/')
    job_id = chunk_id[0:slash_i]
    # chunk_index = chunk_id[slash_i:]
    conf = _job_confs[job_id]
    job = conf[1].load(job_id)
    new_completed = conf[1].inc_completed(job)
    if new_completed == job[JOB_ATTR.TOTAL]:
        conf[2].send(job_id, None)
