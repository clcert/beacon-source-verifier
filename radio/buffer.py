import logging
from collections import OrderedDict
from typing import List

from radio.mp3_frame import Frame


class RadioBuffer:
    def __init__(self, size: int):
        self.buffer = OrderedDict()
        self.size = size

    def __len__(self):
        return len(self.buffer)

    def add(self, item: Frame) -> None:
        self.buffer[item.get_marker()] = item
        if len(self.buffer) > self.size:
            self.buffer.popitem(False)

    def check_marker(self, marker: str) -> bool:
        logging.debug(f"checking marker {marker} (buffer size = {len(self.buffer)} items)")
        i = 0
        if marker in self.buffer:
            while len(self.buffer) > 0:
                k, v = self.buffer.popitem(False)
                if k == marker:
                    self.buffer[k] = v
                    self.buffer.move_to_end(k, False)
                    logging.debug(f"removed {i} elements before hash...")
                    return True
                i += 1
        logging.debug(f"marker {marker} not found...")
        return False

    def get_list(self, size: int) -> List[Frame]:
        if len(self.buffer) < size:
            logging.debug(f"buffer not full yet ({len(self.buffer)}/{self.size}), try again in a few seconds...")
            return []
        else:
            res: List[Frame] = []
            for i in range(size):
                k, v = self.buffer.popitem(False)
                res.append(v)
            return res
