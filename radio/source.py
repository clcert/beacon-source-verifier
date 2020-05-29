import asyncio
import logging

from core.abstract_source import AbstractSource
from radio.buffer import RadioBuffer
from radio.mp3_frame import Frame


class RadioSource(AbstractSource):
    BUFFER_SIZE = 26 * 1000 * 2 * 5
    FRAMES_NUM = 300
    NAME = "radio"
    ID = 3


    def __init__(self, config: map):
        self.url = config["url"]
        self.port = config["port"]
        self.prefix = config["prefix"]
        self.buffer = RadioBuffer(self.BUFFER_SIZE)
        super().__init__()

    async def verify(self, params: map) -> map:
        if params["metadata"][:len(self.prefix)] != self.prefix:
            logging.error(f"wrong marker. It should start with \"{self.prefix}\"")
        else:
            if self.buffer.check_marker(params["metadata"]):
                while len(self.buffer) < self.FRAMES_NUM:
                    logging.debug(f"we need {self.FRAMES_NUM} frames to generate randomness but we have {len(self.buffer)}, waiting 5 seconds...")
                    await asyncio.sleep(5)
                frames = self.buffer.get_list(self.FRAMES_NUM)
                d = b''
                logging.debug(f"joining raw data from {len(frames)} frames...")
                for frame in frames:
                    d += frame.get_raw_data()
                logging.debug(f"Data joined, comparing with event data:")
                d_hex = d.hex()
                if d_hex == params["event"]:
                    return {self.name(): True}
        return {self.name(): False}

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
        if frame.get_marker()[:len(self.prefix)] == self.prefix :
            logging.info(f"valid marker: {frame.get_marker()}")
        self.buffer.add(frame)

    async def finish_collector(self) -> None:
        self.writer.close()
        await self.writer.wait_closed()
