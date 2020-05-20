from typing import List
from worker_bees.spi import Channel


class PrintChannel(Channel):
    def __init__(self, **kwargs):
        self.__msg = kwargs.get('msg_template')
        if not self.__msg:
            self.__msg = 'Job {} has been completed'

    def send(self, job_id: str, chunk_payloads: List[map]):
        print(self.__msg.format(job_id))
        if chunk_payloads:
            print(chunk_payloads)


impl = PrintChannel

