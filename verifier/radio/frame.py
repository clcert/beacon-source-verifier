import asyncio
import hashlib
from enum import Enum


class InvalidSyncByteException(Exception):
    pass


class NonLayer3Exception(Exception):
    pass


class InvalidBitrateException(Exception):
    pass


class InvalidSampleRateException(Exception):
    pass


class FrameHeader:
    class Version(Enum):
        MPEG_1 = 1
        MPEG_2 = 0

    SAMPLERATE = {
        Version.MPEG_1: {
            0x00: 44100,
            0x01: 48000,
            0x02: 32000,
        },
        Version.MPEG_2: {
            0x00: 22050,
            0x01: 12000,
            0x02: 16000,
        }
    }

    BITRATE = {
        Version.MPEG_1: {
            0x01: 32,
            0x02: 40,
            0x03: 48,
            0x04: 56,
            0x05: 64,
            0x06: 80,
            0x07: 96,
            0x08: 112,
            0x09: 128,
            0x0a: 160,
            0x0b: 192,
            0x0c: 224,
            0x0d: 256,
            0x0e: 320,
        },
        Version.MPEG_2: {
            0x01: 8,
            0x02: 16,
            0x03: 24,
            0x04: 32,
            0x05: 40,
            0x06: 48,
            0x07: 56,
            0x08: 64,
            0x09: 80,
            0x0a: 96,
            0x0b: 112,
            0x0c: 128,
            0x0d: 144,
            0x0e: 160,
        },
    }

    def __init__(self):
        self.bitrate: int = 0
        self.padding: bool = False
        self.samplerate: int = 0
        self.crc: bool = False
        self.version: FrameHeader.Version = FrameHeader.Version(0)
        self.body_size = 0
        self.data = b''

    async def read(self, sock: asyncio.StreamReader):
        # Read frame header
        b = (await sock.read(1))[0]
        self.data += bytes([b])
        if b != 0xff:
            raise InvalidSyncByteException(f"Invalid sync byte in this frame (should have been \\xff but it is {b})")
        # Next byte should be 0xf<x>
        b = (await sock.read(1))[0]
        self.data += bytes([b])
        if (b & 0xf0) != 0xf0:
            raise InvalidSyncByteException(f"Invalid sync byte in this frame (should have been \\xf0 but it is {b})")
        self.version = FrameHeader.Version((b & 0x08) >> 3)
        if (b & 0x06) >> 1 != 1:
            # Layer is not 3 (0x01)
            raise NonLayer3Exception(f"MPEG frame is not layer 3 type")
        self.crc = True if (b & 0x01) == 0x01 else False
        b = (await sock.read(1))[0]
        self.data += bytes([b])
        bitrate = b >> 4
        if bitrate == 0x00 or bitrate == 0x0f:
            # invalid values
            raise InvalidBitrateException(f"Bitrate defined for type is not allowed (value obtained was {bitrate})")
        self.bitrate = self.BITRATE[self.version][bitrate]
        samplerate = (b & 0x0c) >> 2
        if samplerate == 0x03:
            raise InvalidSampleRateException(f"samplerate obtained is invalid (obtained \x03)")
        self.samplerate = self.SAMPLERATE[self.version][samplerate]
        padding = (b & 0x02) >> 1
        if padding == 1:
            self.padding = True
        b = (await sock.read(1))[0]
        self.data += bytes([b])
        self.body_size = 144000 * self.bitrate // self.samplerate - 4  # substract the header length
        if self.padding:
            self.body_size += 1  # MP3 padding size is 1 byte


class Frame:
    def __init__(self):
        self.header = FrameHeader()
        self.data = b''

    def get_canonical_form(self) -> bytes:
        return self.header.data + self.data

    def get_marker(self) -> str:
        return hashlib.sha3_512(self.get_canonical_form()).hexdigest()

    async def read(self, reader: asyncio.StreamReader):
        await self.header.read(reader)
        to_read = self.header.body_size
        while to_read != 0:
            new_data = await reader.read(to_read)
            to_read -= len(new_data)
            self.data += new_data

    def __str__(self) -> str:
        return f"Frame<hash={self.get_marker()}>"
