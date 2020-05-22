import asyncio
import logging

from core.abstract_source import AbstractSource
from core.ordered_dict_buffer import OrderedDictBuffer
from radio.mp3_frame import Frame


class RadioSource(AbstractSource):
    BUFFER_SIZE = 26 * 1000 * 2 * 5
    FRAMES_NUM = 300

    def __init__(self, url: str, port: int):
        self.url = url
        self.port = port
        super().__init__(OrderedDictBuffer(self.BUFFER_SIZE))

    def name(self) -> str:
        return "radio"

    def id(self) -> int:
        return 3

    async def verify(self, params: map) -> map:
        # extract data from buffer using params.metadata
        # compare with params.event
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
            if d.hex() == params["event"]:
                return {self.name(): True}
        else:
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
        self.buffer.add(frame)

    async def finish_collector(self) -> None:
        self.writer.close()
        await self.writer.wait_closed()
