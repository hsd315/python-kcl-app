import logging
import logging.config
import os
import yaml
import logging


AWS_REGION = os.getenv('AWS_REGION', 'us-east-1')
AWS_KINESIS_STREAM_NAME = os.getenv('AWS_KINESIS_STREAM_NAME', 'kaeawc')
AWS_KINESIS_SHARD_COUNT = os.getenv('AWS_KINESIS_SHARD_COUNT', 1)
AWS_KINESIS_PARTITION_KEY = os.getenv('AWS_KINESIS_PARTITION_KEY', 'a')
AWS_KINESIS_POLLING_TIMEDELTA = int(os.getenv('AWS_KINESIS_POLLING_TIMEDELTA', '3'))

SLEEP_SECONDS = os.getenv('SLEEP_SECONDS', 5)
CHECKPOINT_RETRIES = os.getenv('CHECKPOINT_RETRIES', 5)
CHECKPOINT_FREQ_SECONDS = os.getenv('CHECKPOINT_FREQ_SECONDS', 60)
# LOGGING_YML = os.getenv('LOGGING_YML', os.path.join(os.path.dirname(__file__), 'logging.yml'))
#
# # set up logging
# with open(LOGGING_YML, 'rt') as f:
#     logging_cfg = yaml.load(f.read())
#     logging.config.dictConfig(logging_cfg)

logging.basicConfig()
logger = logging.getLogger(__name__)
print('Config properly read')
