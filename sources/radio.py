import socket
from abc import ABC

from sources.abstract_source import AbstractSource
from sources.mp3_frame import Frame


class RadioSource(AbstractSource, ABC):
    RADIO_URL = ("200.89.71.21", 8000)
    BUFFER_SIZE = 100
    HEADER_SIZE = 4

    def __init__(self):
        self.socket: socket.socket
        super(AbstractSource, self).__init__(self.BUFFER_SIZE)

    def name(self) -> str:
        return "radio"

    def id(self) -> int:
        return 3

    def __verify(self, params: map) -> bool:
        pass

    def __init_collector(self) -> None:
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.connect(self.RADIO_URL)
        self.socket.send(b'GET /; HTTP/1.0\r\n\r\n')

    def __collect(self):
        try:
            frame = Frame(self.socket)
            print("frame with version={} layer= {} bitrate={} samplerate={} size={}".format(frame.header.version,
                                                                                            frame.header.layer,
                                                                                            frame.header.bitrate,
                                                                                            frame.header.samplerate,
                                                                                            frame.header.frame_size))
        except Exception as e:
            print("exception {}".format(e))

    def __finish_collector(self) -> None:
        self.socket.close()
