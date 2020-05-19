import collections
from typing import List
from worker_bees.spi import Channel


class InMemQueueChannel(Channel):
    def __init__(self, **kwargs):
        self.__queue = collections.deque()

    def send(self, job_id: str, chunks: List[map]):
        for chunk in chunks:
            self.__queue.append(chunk)

    def receive(self) -> map :
        return self.__queue.pop()

impl = InMemQueueChannel
