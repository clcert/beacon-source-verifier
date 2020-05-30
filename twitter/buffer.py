import heapq
import logging
from datetime import datetime
from typing import List

from twitter.tweet import Tweet

log = logging.getLogger(__name__)


class TwitterBuffer:
    def __init__(self, size: int):
        self.buffer = []
        self.size = size

    def __len__(self):
        return len(self.buffer)

    def add(self, item: Tweet) -> None:
        if len(self.buffer) == self.size:
            heapq.heapreplace(self.buffer, create_heap_item(item))
        else:
            heapq.heappush(self.buffer, create_heap_item(item))

    def check_marker(self, marker: datetime.date) -> bool:
        log.debug(f"checking marker {marker} (buffer size = {len(self.buffer)} items)")
        while len(self.buffer) > 0:
            item = heapq.heappop(self.buffer)
            if item[-1].date == marker:
                heapq.heappush(self.buffer, item)
                return True
        return False

    def get_list(self, end_date: datetime.date) -> List[Tweet]:
        items = []
        while len(self.buffer) > 0:
            item = heapq.heappop(self.buffer)
            if item[-1].date <= end_date:
                items.append(item[-1])
            else:
                heapq.heappush(self.buffer, item)
                break
        return items


def create_heap_item(item: Tweet) -> tuple:
    return item.date, item.id, item
