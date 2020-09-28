from prometheus_client import *
from prometheus_client import start_http_server
from datetime import datetime


class Metrics():
    def __init__(self):
        # Pulse metrics
        self.pulse_number = Gauge(
            'pulse_number',
            'Current pulse number',
            ["chain"]
        )
        self.pulse_status = Summary(
            'pulse_status',
            'Pulse status',
            ['code']
        )
        # Verification Metrics
        self.verification_possible = Summary(
            'verification_possible',
            'Possible correct values on this pulse by source',
            ['source']
        )
        self.verification_ext_value_status = Summary(
            'verification_ext_value_status',
            "Verification External Value Status",
            ['source', 'code']
        )
        self.verification_status = Summary(
            'verification_status',
            'Verification status',
            ['source', 'code']
        )
        self.verification_seconds = Summary(
            'verification_seconds',
            'Verification seconds',
            ['source']
        )
        # Collector Metrics
        self.collector_status = Enum(
            'collector_status',
            'Collector status',
            ['source'],
            states=['starting', 'running', 'stopping', 'stopped']
        )
        self.collector_buffer_size = Summary(
            'collector_buffer_size',
            'Collector Buffer Size',
            ['source']
        )
        # Exception number
        self.exceptions_number = Summary(
            'exceptions_number',
            'Number of unexpected exceptions since last restart',
        )
        # Twitter Metadata
        self. twitter_extra_tweets = Summary(
            'twitter_verifier_extra_tweets',
            "Tweets that one side has but the other has not",
            ['owner']
        )

    def start_server(self, port):
        start_http_server(port)
