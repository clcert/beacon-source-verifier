import logging
import heapq
from datetime import datetime
from collections import OrderedDict
from typing import List, Set

from earthquake.event import Event
from prometheus_client import Gauge 

log = logging.getLogger(__name__)

class Buffer:
    def __init__(self, metric: Gauge, size: int):
        self.buffer: List[Event] = []
        self.set: Set[str] = set()
        self.size = size
        self.metric = metric

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
        self.metric.observe(len(self.buffer))

    def check_marker(self, marker: str) -> bool:
        res = False
        if marker in self.set:
            log.debug(f"checking marker {marker} (buffer size = {len(self.buffer)} items)")
            while len(self.buffer) > 0:
                item = heapq.heappop(self.buffer)
                self.set.remove(item[-1].get_marker())
                if item[-1].get_marker() == marker:
                    heapq.heappush(self.buffer, item)
                    self.set.add(item[-1].get_marker())
                    res = True
                    break
        self.metric.observe(len(self.buffer))
        return res

    def get_first(self) -> Event:
        item = heapq.heappop(self.buffer)
        heapq.heappush(self.buffer, item)
        self.metric.observe(len(self.buffer))
        return item[-1]

    def __str__(self) -> str:
        result = []
        for k in self.buffer:
            result.append(f"{k}")
        return f"EarthquakeBuffer<{','.join(result)}>"



def create_heap_item(item: Event) -> tuple:
    return item.date, item.id, item
