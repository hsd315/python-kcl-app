import json
import time
import logging
from boto.kinesis.exceptions import ResourceNotFoundException

logger = logging.getLogger(__name__)


class Streams(object):

    def __init__(self, kinesis):
        self.kinesis = kinesis

    def get_or_create_stream(self, stream_name, shard_count):
        stream = None
        try:
            stream = self.kinesis.describe_stream(stream_name)
            data = json.dumps(stream, sort_keys=True, indent=2, separators=(',', ': '))
            logger.debug(data)
        except ResourceNotFoundException:
            while (stream is None) or ('ACTIVE' not in stream['StreamDescription']['StreamStatus']):
                if stream is None:
                    logger.debug('Could not find ACTIVE stream:{0} trying to create.'.format(
                        stream_name))
                    self.kinesis.create_stream(stream_name, shard_count)
                else:
                    logger.debug("Stream status: %s", stream['StreamDescription']['StreamStatus'])
                time.sleep(1)
                stream = self.kinesis.describe_stream(stream_name)

        return stream
