import importlib
import uuid
from worker_bees.spi import JOB_ATTR, CHUNK_ATTR
from worker_bees.common import from_chunk_id

_worker_channels = {}
_repos = {}
_completion_channels = {}


def _get_component(comp_map: map, job_id: str, **kwargs):
    comp = comp_map.get(job_id)
    if not comp:
        comp = importlib.import_module(f'worker_bees.{kwargs["type"]}').impl(**kwargs)
        comp_map[job_id] = comp
    return comp


def kick_off(worker_channel: map, repo: map, chunks, job_id: str = None,
             job_id_attr: str = JOB_ATTR.ID) -> str:
    if not job_id:
        job_id = str(uuid.uuid4())
    worker_channel_impl = _get_component(_worker_channels, job_id, **worker_channel)
    repo_impl = _get_component(_repos, job_id, **repo)
    worker_channel_impl.send(job_id,
                             [{CHUNK_ATTR.CHUNK_ID: f'{job_id}/{i}', CHUNK_ATTR.PAYLOAD: chunk} for (i, chunk) in
                              enumerate(chunks)])
    repo_impl.new_job({job_id_attr: job_id,
                       JOB_ATTR.COMPLETED: 0,
                       JOB_ATTR.TOTAL: len(chunks)})
    return job_id


def complete(repo: map, completion_channel: map, chunk_id: str):
    job_id, _ = from_chunk_id(chunk_id)
    repo_impl = _get_component(_repos, job_id, **repo)
    completion_channel_impl = _get_component(_completion_channels, job_id, **completion_channel)
    job = repo_impl.load(job_id)
    new_completed = repo_impl.inc_completed(job)
    if new_completed == job[JOB_ATTR.TOTAL]:
        completion_channel_impl.send(job_id, [{'job_id': job_id, 'status': 'COMPLETED'}])
