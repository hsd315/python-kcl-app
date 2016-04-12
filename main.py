#!/usr/bin/env python3
import config
import logging
from amazon_kclpy import kcl
from record_processor import RecordProcessor

logger = logging.getLogger(__name__)


class StdOutProcessor(RecordProcessor):

    SLEEP_SECONDS = config.SLEEP_SECONDS
    CHECKPOINT_RETRIES = config.CHECKPOINT_RETRIES
    CHECKPOINT_FREQ_SECONDS = config.CHECKPOINT_FREQ_SECONDS
    
    def process_record(self, data: str, partition_key: str, sequence_number: int):
        logger.info(data)

if __name__ == "__main__":
    app = kcl.KCLProcess(StdOutProcessor())
    app.run()

