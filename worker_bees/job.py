import importlib
import uuid
import boto3
import worker_bees.channel_sqs
import worker_bees.repo_dynamodb
from worker_bees.spi import *

_job_confs = {}
_conf_cache = {}


def _config(channel: str, repo: str, **kwargs):
    conf = _conf_cache.get((channel, repo))
    if not conf:
        conf = (importlib.import_module(f'worker_bees.channel_{channel}').impl(**kwargs),
                importlib.import_module(f'worker_bees.repo_{repo}').impl(**kwargs))
        _conf_cache[(channel, repo)] = conf
    return conf


def kick_off(channel: str, repo: str, chunks, job_id: str = None, **kwargs) -> str:
    if not job_id:
        job_id = str(uuid.uuid4())
    conf = _config(channel, repo)
    _job_confs[job_id] = conf
    channel_impl = conf[0]
    repo_impl = conf[1]
    channel_impl.send(job_id,
                      [{CHUNK_ATTR.CHUNK_ID: f'{job_id}/{i}', CHUNK_ATTR.PAYLOAD: chunk} for (i, chunk) in
                       enumerate(chunks)])
    job_id_attr = kwargs.get('job_id_attr', JOB_ATTR.ID)
    repo_impl.save({job_id_attr: job_id, JOB_ATTR.COMPLETED: 0, JOB_ATTR.TOTAL: len(chunks)})
    return job_id


def complete(chunk_id: str):
    slash_i = chunk_id.rfind('/')
    job_id = chunk_id[0:slash_i]
    chunk_index = chunk_id[slash_i:]
    conf = _job_confs[job_id]
    job = conf[1].load(job_id)
    job[JOB_ATTR.COMPLETED] += 1
    conf[1].save(job)
    if job[JOB_ATTR.COMPLETED] == job[JOB_ATTR.TOTAL]:
        print(f'COMPLETED {job_id}, {job[JOB_ATTR.TOTAL]} trunks')
