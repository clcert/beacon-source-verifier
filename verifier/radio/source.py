import asyncio
import logging

from core.source_manager import SourceManager
from core.results import VerifierException, VerifierResult


from core.abstract_source import AbstractSource
from radio.buffer import Buffer
from radio.frame import Frame

from typing import List

log = logging.getLogger(__name__)


class Source(AbstractSource):
    BUFFER_SIZE = 26 * 1000 * 2 * 5
    FRAMES_NUM = 300
    NAME = "radio"

    def __init__(self, config: map, mgr: SourceManager):
        self.url = config["url"]
        self.port = config["port"]
        self.prefix = config["prefix"]
        self.buffer = Buffer(mgr.metrics.collector_buffer_size.labels(self.name()), self.BUFFER_SIZE, config["prefix"])
        super().__init__(mgr)

    async def verify(self, params: map) -> map:
        result = VerifierResult(self.name())
        result.possible = len(self.get_possible())
        status = params.get("status", 2)
        result.ext_value_status = status
        if (status & 2) == 2 :
            result.status_code = 240
            result.add_detail(
                f"ExtValue is not valid", 
                f"beacon_status={status}")
        else:
            limit = self.prefix + "f" * \
                (len(params["metadata"]) - len(self.prefix))
            if params["metadata"] > limit:
                result.status_code = 220
                result.add_detail(
                    f"Wrong marker in pulse metadata",
                    f"limit={limit}",
                    f"metadata={params['metadata']}")
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
                        d += frame.get_canonical_form()
                    log.debug(f"Data joined, comparing with event data:")
                    d_hex = d.hex()
                    if d_hex != params["raw"]:
                        result.status_code = 221
                        result.add_detail(
                            f"Raw value does not match",
                            f"ours={d_hex}",
                            f"theirs={params['raw']}")
                else:
                    result.status_code = 222
                    result.add_detail(
                        f"Metadata not found",
                        f"metadata={params['metadata']}",
                        f"buffer_size={len(self.buffer)}")
        result.finish()
        return result

    async def init_collector(self) -> None:
        self.reader, self.writer = await asyncio.open_connection(self.url, self.port)
        self.writer.write(b'GET /; HTTP/1.0\r\n\r\n')
        line = await self.reader.readline()
        while len(line.strip()) != 0:
            # empty line means HTTP headers finished
            line = await self.reader.readline()

    async def collect(self):
        frame = Frame()
        await asyncio.wait_for(frame.read(self.reader), timeout=5)
        self.buffer.add(frame)

    async def finish_collector(self) -> None:
        self.writer.close()
        await self.writer.wait_closed()

    def get_possible(self) -> List[str]:
        return [p for p in self.buffer.possible]