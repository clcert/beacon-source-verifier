import asyncio
import logging
import sched
import signal
import sys
import time

from radio.source import RadioSource
from core.source_manager import SourceManager

BEACON_VERIFIER_API = "https://random.uchile.cl/beacon/2.0-beta1"
logging.basicConfig(format="%(asctime)s [%(levelname)s] %(message)s", level=logging.DEBUG)

VERIFICATION_INTERVAL = 2 * 60
VERIFY_TIMEOUT = 30
STOP_COLLECTOR_TIMEOUT = 10

if __name__ == "__main__":
    logging.info("Starting Verifier Process")
    sourceManager = SourceManager(BEACON_VERIFIER_API, VERIFICATION_INTERVAL, VERIFY_TIMEOUT, STOP_COLLECTOR_TIMEOUT)
    sourceManager.add_source(RadioSource("200.89.71.21", 8000))
    try:
        sourceManager.start_collection()
        asyncio.run(asyncio.sleep(VERIFICATION_INTERVAL))
        asyncio.run(sourceManager.run_verification())
    except KeyboardInterrupt as e:
        print('Finishing...')
        asyncio.run(sourceManager.stop_collection())
        sys.exit(0)
