import logging
import heapq
from datetime import datetime
from collections import OrderedDict
from typing import List

from earthquake.event import Event

log = logging.getLogger(__name__)

class Buffer:
    def __init__(self, size: int):
        self.buffer = []
        self.size = size

    def __len__(self):
        return len(self.buffer)

    def add(self, item: Event) -> None:
        if len(self.buffer) == self.size:
            heapq.heapreplace(self.buffer, create_heap_item(item))
        else:
            heapq.heappush(self.buffer, create_heap_item(item))

    def check_marker(self, marker: str) -> bool:
        log.debug(f"checking marker {marker} (buffer size = {len(self.buffer)} items)")
        while len(self.buffer) > 0:
            item = heapq.heappop(self.buffer)
            if item[-1].get_marker() == marker:
                heapq.heappush(self.buffer, item)
                return True
        return False

    def get_first(self) -> Event:
        return heapq.heappop(self.buffer)[2]


def create_heap_item(item: Event) -> tuple:
    return item.datestr, item.id, item
