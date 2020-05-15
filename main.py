import radio.source
from core.source_manager import SourceManager

if __name__ == "__main__":
    sourceManager = SourceManager(30)
    sourceManager.add_source(radio.RadioSource())
    sourceManager.start_collection()