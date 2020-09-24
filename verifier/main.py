import asyncio
import json
import logging
import sys

from core.source_manager import SourceManager
from radio.source import Source as RadioSource
from twitter.source import Source as TwitterSource
from earthquake.source import Source as EarthquakeSource
from ethereum.source import Source as EthereumSource

config = {}

log = logging.getLogger(__name__)

sources = [
    RadioSource,
    TwitterSource,
    EarthquakeSource,
    EthereumSource,
]

if __name__ == "__main__":
    try:
        with open("config.json") as f:
            config = json.load(f)
    except Exception as e:
        print(f"cannot read config file: {e}")
        exit(1)
    logging.basicConfig(
        format='%(asctime)s  [%(name)s] %(levelname)s - %(message)s',
        level=logging.getLevelName(config["log_level"].upper()),
        handlers=[
            logging.FileHandler(filename=config["log_name"]),
            logging.StreamHandler(sys.stdout)
        ]
    )
    log.info("Starting Verifier Process")
    sourceManager = SourceManager(config)
    if not "sources" in config:
        log.error(f"cannot find sources section in config file")
        exit(1)
    for source in sources:
        if not source.NAME in config["sources"]:
            log.error(f"cannot find config for source {source.NAME}")
            exit(1)
        source_config = config["sources"][source.NAME]
        if source_config.get("enabled", False):
            source_instance = source(source_config, sourceManager)
            sourceManager.add_source(source_instance)
    try:
        sourceManager.start_collection()
        asyncio.run(sourceManager.run_verification())
    except KeyboardInterrupt as e:
        print('Finishing...')
        asyncio.run(sourceManager.stop_collection())
        sys.exit(0)
