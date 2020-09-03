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
        self.set = set()
        self.size = size

    def __len__(self):
        return len(self.buffer)

    def add(self, item: Event) -> None:
        if item.get_marker() not in self.set:
            if len(self.buffer) == self.size:
                item2 = heapq.heappushpop(self.buffer, create_heap_item(item))
                if item2[-1].get_marker() in self.set:
                    self.set.remove(item2[-1].get_marker())
                self.set.add(item.get_marker())
            else:
                self.set.add(item.get_marker())
                heapq.heappush(self.buffer, create_heap_item(item))

    def check_marker(self, marker: str) -> bool:
        if marker in self.set:
            log.debug(f"checking marker {marker} (buffer size = {len(self.buffer)} items)")
            while len(self.buffer) > 0:
                item = heapq.heappop(self.buffer)
                self.set.remove(item[-1].get_marker())
                if item[-1].get_marker() == marker:
                    heapq.heappush(self.buffer, item)
                    self.set.add(item[-1].get_marker())
                    return True
        return False

    def get_first(self) -> Event:
        item = heapq.heappop(self.buffer)
        heapq.heappush(self.buffer, item)
        return item[-1]

    def __str__(self) -> str:
        result = []
        for k in self.buffer:
            result.append(f"{k}")
        return f"EarthquakeBuffer<{','.join(result)}>"



def create_heap_item(item: Event) -> tuple:
    return item.date, item.id, item
