from enum import Enum
from socket import socket


class InvalidSyncByteException(Exception):
    pass


class NonLayer3Exception(Exception):
    pass


class InvalidBitrateException(Exception):
    pass


class InvalidSamplerateException(Exception):
    pass


class FrameHeader:
    class Version(Enum):
        MPEG_1 = 1
        MPEG_2 = 0

    # https://hydrogenaud.io/index.php?topic=32036.0
    SAMPLES_PER_FRAME = {
        Version.MPEG_1: 1152,
        Version.MPEG_2: 576
    }

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
            0x02: 48,
            0x03: 56,
            0x04: 64,
            0x05: 80,
            0x06: 96,
            0x07: 112,
            0x08: 128,
            0x09: 160,
            0x0a: 192,
            0x0b: 224,
            0x0c: 256,
            0x0d: 320,
            0x0e: 384,
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

    def __init__(self, sock: socket):
        self.bitrate: int = 0
        self.padding: bool = False
        self.samplerate: int = 0
        self.version: FrameHeader.Version = FrameHeader.Version(0)

        # Read frame header
        b = sock.recv(1)[0]
        if b != 0xff:
            raise InvalidSyncByteException()
        # Next byte should be 0xf<x>
        b = sock.recv(1)[0]
        if (b & 0xf0) != 0xf0:
            raise InvalidSyncByteException()
        self.version = FrameHeader.Version((b & 0x08) >> 3)
        if (b & 0x06) >> 1 != 1:
            # Layer is not 3 (0x01)
            raise NonLayer3Exception()
        b = sock.recv(1)[0]
        bitrate = b & 0xf0 >> 4
        if bitrate == 0x00 or bitrate == 0x0f:
            # invalid values
            raise InvalidBitrateException()
        self.bitrate = self.BITRATE[self.version][bitrate]
        samplerate = b & 0x0c >> 2
        if samplerate == 0x03:
            raise InvalidSamplerateException()
        padding = b & 0x02 >> 1
        if padding == 1:
            self.padding = True
        # we do not use next byte
        _ = sock.recv(1)
        self.frame_size = self.bitrate // 8 * 1000 * self.SAMPLES_PER_FRAME[self.version] // self.samplerate
        if self.padding:
            self.frame_size += 1  # MP3 padding size is 1 byte

class Frame:
    def __init__(self, sock: socket):
        self.header = FrameHeader(sock)
        # Read Frame Data
        self.data = sock.recv(self.header.frame_size)

