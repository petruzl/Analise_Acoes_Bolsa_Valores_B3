import logging
import os
from src.config import LOG_DIR

def setup_logger():
    log_file = os.path.join(LOG_DIR, "pipeline.log")

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )

    return logging.getLogger()