from typing import List, Set, Dict
from datetime import datetime

class PulseResult:

    codes: Dict[int, str] = {
        100: "pulse_ok",
        110: "pulse_extraction_unsuccessful",
        120: "pulse_beacon_value_invalid",
        130: "pulse_timeout",
        199: "pulse_unknown_error"
    }

    def __init__(self):
        self.status_code = 100
        self.start_time: datetime.datetime = datetime.now()
        self.end_time: datetime.datetime = datetime.now()
        self.detail = []
        self.pulse_url = ""

    def get_dict(self):
        return {
            "id": self.get_id(),
            "chain": self.get_chain(),
            "pulse_url": self.pulse_url,
            "valid": self.status_code % 100 == 0,
            "status_code": self.status_code,
            "running_time": self.running_time(),
            "reason": PulseResult.codes[self.status_code],
            "detail": self.detail
        }

    def get_id(self) -> int:
        return int(self.pulse_url.split("/")[-1])

    def get_chain(self) -> int:
        return int(self.pulse_url.split("/")[-3])

    def running_time(self) -> int:
        return (self.end_time - self.start_time).total_seconds()
 
    def add_detail(self, *details: str) -> None:
        self.detail += details

    def finish(self):
        self.end_time = datetime.now()

class VerifierResult:

    lsbs: Dict[int, str] = {
        0: "first_pulse_of_chain",
        1: "extraction_error",
        2: "repeated_event",
        3: "alt_source",
    }

    codes: Dict[int, str] = {
        200: "verifier_ok",
        210: "verifier_empty_metadata",
        211: "verifier_empty_raw_data",
        220: "verifier_invalid_for_extraction_parameters",
        221: "verifier_data_does_not_match_with_buffer",
        222: "verifier_data_not_found_in_buffer",
        240: "verifier_wrong_status_code",
        250: "verifier_timeout",
        299: "verifier_unknown_error"
    }

    def __init__(self, scope: str):
        self.scope: str = scope
        self.ext_value_status: int = 0
        self.status_code: int = 200
        self.possible: int = 0
        self.start_time: datetime.datetime = datetime.now()
        self.end_time: datetime.datetime = datetime.now()
        self.detail: List[str] = []

    def get_dict(self) -> Dict[str, any]:
        return {
            "valid": self.status_code % 100 == 0,
            "ext_value_status": self.to_ext_value_map(),
            "possible": self.possible,
            "running_time": self.running_time(),
            "reason": VerifierResult.codes[self.status_code],
            "detail": self.detail,
        }

    def add_detail(self, *details: str) -> None:
        self.detail += details

    def to_ext_value_map(self) -> Dict[str, bool]:
        extvalues = {}
        for i, text in VerifierResult.lsbs.items():
            extvalues[text] = self.ext_value_status & (2**i) == i
        return extvalues

    def running_time(self) -> int:
        return (self.end_time - self.start_time).total_seconds()

    def finish(self):
        self.end_time = datetime.now()

class VerifierException(Exception):

    def __init__(self, result: VerifierResult):
        self.result: VerifierResult = result

    def get_dict(self) -> Dict[str, any]:
        return self.result.get_dict()


class PulseException(Exception):

    def __init__(self, result: PulseResult):
        self.result: PulseResult = result

    def get_dict(self) -> Dict[str, any]:
        return self.result.get_dict()
