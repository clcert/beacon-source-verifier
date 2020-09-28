import heapq
import logging
from datetime import datetime
from typing import List, Set, Tuple

from twitter.tweet import Tweet
from prometheus_client import *

log = logging.getLogger(__name__)


class Buffer:
    def __init__(self, metric: Gauge, second_start: int, size: int):
        self.buffer: List[Tweet] = []
        self.second_start: int = second_start
        self.possible: Set(str) = set()
        self.size = size
        self.metric = metric

    def __len__(self):
        return len(self.buffer)

    def add(self, item: Tweet) -> None:
        if len(self.buffer) == self.size:
            out = heapq.heappushpop(self.buffer, create_heap_item(item))
            if out[-1].datestr in self.possible:
                self.possible.remove(out[-1].datestr)
        else:
            heapq.heappush(self.buffer, create_heap_item(item))
            if item.date.second == self.second_start:
                self.possible.add(item.datestr)
        self.metric.observe(len(self.buffer))

    def check_marker(self, marker: datetime.date) -> bool:
        log.debug(
            f"checking marker {marker} (buffer size = {len(self.buffer)} items)")
        resp = False
        while len(self.buffer) > 0:
            item = heapq.heappop(self.buffer)
            if item[-1].datestr in self.possible:
                self.possible.remove(item[-1].datestr)
            if item[-1].date == marker:
                heapq.heappush(self.buffer, item)
                self.possible.add(item[-1].datestr)
                resp = True
                break
        self.metric.observe(len(self.buffer))
        return resp

    def get_list(self, end_date: datetime.date) -> List[Tweet]:
        items = []
        while len(self.buffer) > 0:
            item = heapq.heappop(self.buffer)
            if item[-1].datestr in self.possible:
                self.possible.remove(item[-1].datestr)
            if item[-1].date <= end_date:
                items.append(item[-1])
            else:
                heapq.heappush(self.buffer, item)
                self.possible.add(item[-1].datestr)
                break
        self.metric.observe(len(self.buffer))
        return items


def create_heap_item(item: Tweet) -> Tuple[datetime, int, Tweet]:
    return item.date, item.id, item
