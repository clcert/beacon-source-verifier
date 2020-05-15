import sources.radio
from source_manager import SourceManager

if __name__ == "__main__":
    sourceManager = SourceManager(30)
    sourceManager.add_source(sources.RadioSource())
    sourceManager.start_collection()