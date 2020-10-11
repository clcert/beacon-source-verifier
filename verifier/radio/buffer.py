import logging
from collections import OrderedDict
from typing import List, Set

from radio.frame import Frame
from prometheus_client import Gauge


log = logging.getLogger(__name__)


class Buffer:
    def __init__(self, metric: Gauge, size: int, prefix: str):
        self.buffer = OrderedDict()
        self.prefix = prefix
        self.size = size
        self.possible: Set(str) = set()
        self.metric = metric

    def __len__(self):
        return len(self.buffer)

    def add(self, item: Frame) -> None:
        self.buffer[item.get_marker()] = item
        limit = self.prefix + "f" * (len(item.get_marker()) - len(self.prefix))
        if item.get_marker() <= limit:
            self.possible.add(item.get_marker())
        if len(self.buffer) > self.size:
            k, popped = self.buffer.popitem(False)
            if k in self.possible:
                self.possible.remove(k)
        self.metric.observe(len(self.buffer))

    def check_marker(self, marker: str) -> bool:
        log.debug(
            f"checking marker {marker} (buffer size = {len(self.buffer)} items)")
        i = 0
        resp = False
        if marker in self.buffer:
            while len(self.buffer) > 0:
                k, v = self.buffer.popitem(False)
                if k == marker:
                    self.buffer[k] = v
                    self.buffer.move_to_end(k, False)
                    log.debug(f"removed {i} elements before hash...")
                    resp = True
                    break
                if k in self.possible:
                    self.possible.remove(k)
                i += 1
        self.metric.observe(len(self.buffer))
        return resp

    def get_list(self, size: int) -> List[Frame]:
        res: List[Frame] = []
        if len(self.buffer) < size:
            log.debug(
                f"buffer not full yet ({len(self.buffer)}/{self.size}), try again in a few seconds...")
        else:
            for _ in range(size):
                k, v = self.buffer.popitem(False)
                if k in self.possible:
                    self.possible.remove(k)
                res.append(v)
        self.metric.observe(len(self.buffer))
        return res
