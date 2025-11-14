import json
import utils.logger
import os
import pathlib

source_folder = "sources/DEV"

if __name__ == "__main__":
    logger = utils.logger.get_logger("RESULTS")
    with open("merged_indexes.json", "r") as file:
        index = json.load(file)
        logger.info(f"Total Indexed Documents: {len(list(pathlib.Path(source_folder).rglob('*json')))}")
        logger.info(f"Total Unique Tokens: {len(index)}")
        logger.info(f"Total Size {os.path.getsize('merged_indexes.json') / 1024} KB")