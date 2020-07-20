import asyncio
import logging

from core.abstract_source import AbstractSource
from radio.buffer import Buffer
from radio.frame import Frame

log = logging.getLogger(__name__)


class Source(AbstractSource):
    BUFFER_SIZE = 26 * 1000 * 2 * 5
    FRAMES_NUM = 300
    NAME = "radio"

    def __init__(self, config: map):
        self.url = config["url"]
        self.port = config["port"]
        self.prefix = config["prefix"]
        self.buffer = Buffer(self.BUFFER_SIZE)
        super().__init__()

    async def verify(self, params: map) -> map:
        reason = ""
        valid = False
        if params.get("metadata", "0") == "0":
            reason = "empty metadata"
        elif params.get("event", "0") == "0":
            reason = "empty event"
        else:
            their_prefix = params["metadata"][:len(self.prefix)]
            if their_prefix != self.prefix:
                reason = f"wrong marker in pulse metadata. our_prefix=\"{self.prefix}\" their_prefix=\"{their_prefix}\""
            else:
                if self.buffer.check_marker(params["metadata"]):
                    while len(self.buffer) < self.FRAMES_NUM:
                        log.debug(
                            f"we need {self.FRAMES_NUM} frames to generate randomness but we have {len(self.buffer)}, waiting 5 seconds...")
                        await asyncio.sleep(5)
                    frames = self.buffer.get_list(self.FRAMES_NUM)
                    d = b''
                    log.debug(f"joining raw data from {len(frames)} frames...")
                    for frame in frames:
                        d += frame.get_raw_data()
                    log.debug(f"Data joined, comparing with event data:")
                    d_hex = d.hex()
                    if d_hex == params["event"]:
                        valid = True
                    else:
                        reason = f"event value does not match. ours={d_hex} theirs={params['event']}"
                else:
                    reason =  f"metadata \"{params['metadata']}\" not found. buffer_size={len(self.buffer)}"
        return {self.name(): {
            "valid": valid,
            "reason": reason
        }}

    async def init_collector(self) -> None:
        self.reader, self.writer = await asyncio.open_connection(self.url, self.port)
        self.writer.write(b'GET /; HTTP/1.0\r\n\r\n')
        line = await self.reader.readline()
        while len(line.strip()) != 0:
            # empty line means HTTP headers finished
            line = await self.reader.readline()

    async def collect(self):
        frame = Frame()
        await frame.read(self.reader)
        self.buffer.add(frame)

    async def finish_collector(self) -> None:
        self.writer.close()
        await self.writer.wait_closed()
