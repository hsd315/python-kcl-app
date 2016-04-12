import logging
import logging.config
import os
import yaml
import logging


SLEEP_SECONDS = os.getenv('SLEEP_SECONDS', 5)
CHECKPOINT_RETRIES = os.getenv('CHECKPOINT_RETRIES', 5)
CHECKPOINT_FREQ_SECONDS = os.getenv('CHECKPOINT_FREQ_SECONDS', 60)
LOGGING_YML = os.getenv('LOGGING_YML', os.path.join(os.path.dirname(__file__), 'logging.yml'))

# set up logging
with open(LOGGING_YML, 'rt') as f:
    logging_cfg = yaml.load(f.read())
    logging.config.dictConfig(logging_cfg)

logger = logging.getLogger(__name__)
logger.info('Config properly read')
