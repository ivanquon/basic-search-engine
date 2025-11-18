import pickle
import utils.logger
import os
import pathlib
from indexer import Posting

source_folder = "sources/ANALYST"

if __name__ == "__main__":
    logger = utils.logger.get_logger("RESULTS")
    with open("merged_indexes.pkl", "rb") as file:
        index = pickle.load(file)
        logger.info(f"Total Indexed Documents: {len(list(pathlib.Path(source_folder).rglob('*json')))}")
        logger.info(f"Total Unique Tokens: {len(index)}")
        logger.info(f"Total Size {os.path.getsize('merged_indexes.pkl') / 1024} KB")