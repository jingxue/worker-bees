def to_chunk_id(job_id, chunk_idx):
    return f'{job_id}/{chunk_idx}'


def from_chunk_id(chunk_id):
    slash_i = chunk_id.rfind('/')
    job_id = chunk_id[0:slash_i]
    chunk_index = chunk_id[slash_i + 1:]
    return job_id, chunk_index
