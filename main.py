import asyncio
import json
import logging
import sys

from radio.source import RadioSource
from core.source_manager import SourceManager
from twitter.source import TwitterSource

logging.basicConfig(format="%(asctime)s [%(levelname)s] %(message)s", level=logging.DEBUG)
config = {}

sources = [
     RadioSource,
     TwitterSource,
]

if __name__ == "__main__":
    logging.info("Reading config...")
    try:
        with open("config.json") as f:
            config = json.load(f)
    except Exception as e:
        logging.error(f"cannot read config file: {e}")
        exit(1)
    logging.info("Starting Verifier Process")
    sourceManager = SourceManager(config)
    if not "sources" in config:
        logging.error(f"cannot find sources section in config file")
        exit(1)
    for source in sources:
        if not source.NAME in config["sources"]:
            logging.error(f"cannot find config for source {source.NAME}")
            exit(1)
        source_config = config["sources"][source.NAME]
        source_instance = source(source_config)
        sourceManager.add_source(source_instance)
    try:
        sourceManager.start_collection()
        asyncio.run(sourceManager.run_verification())
    except KeyboardInterrupt as e:
        print('Finishing...')
        asyncio.run(sourceManager.stop_collection())
        sys.exit(0)
