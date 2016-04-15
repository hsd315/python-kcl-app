#!/usr/bin/env python3
import boto
import config
import logging
import time
from datetime import datetime, timedelta
from random import choice
import string
from boto.kinesis.exceptions import ResourceNotFoundException
from streams import Streams

make_string = lambda x: "".join(choice(string.ascii_lowercase) for i in range(x))
logger = logging.getLogger(__name__)


class Producer(object):
    """The Poster thread that repeatedly posts records to shards in a given
    Kinesis stream.
    """
    def __init__(self, stream_name: str, partition_key: str, polling_timedelta: int):
        self._pending_records = []
        self.stream_name = stream_name
        self.partition_key = partition_key
        self.default_records = [
            make_string(100), make_string(1000), make_string(500),
            make_string(5000), make_string(10), make_string(750),
            make_string(10), make_string(2000), make_string(500)
        ]
        self.polling_timedelta = polling_timedelta
        self.total_records = 0

    def add_records(self, records: list):
        """ Add given records to the Poster's pending records list.
        """
        if len(records) is 1:
            self._pending_records.extend(records[0])
        else:
            self._pending_records.extend(records)

    def put_all_records(self):
        """Put all pending records in the Kinesis stream."""
        precs = self._pending_records
        self._pending_records = []
        self.put_records(precs)
        self.put_records(precs, batch=True)
        self.total_records += len(precs)
        return len(precs)

    def put_records(self, records: list, batch=False):
        """Put the given records in the Kinesis stream."""
        sum = 0
        if batch:
            print('batching %s records' % len(records))
            batch_records = [{'Data': data, 'PartitionKey': self.partition_key} for data in records]
            start = datetime.utcnow()
            kinesis.put_records(
                stream_name=self.stream_name,
                records=batch_records)
            end = datetime.utcnow()
            duration = end - start
            milliseconds = duration.microseconds / 1000
            print('record batch put in %s milliseconds' % milliseconds)
        else:
            print('putting %s records individually' % len(records))
            for record in records:
                start = datetime.utcnow()
                kinesis.put_record(
                    stream_name=self.stream_name,
                    data=record, partition_key=self.partition_key)
                end = datetime.utcnow()
                duration = end - start
                sum += duration.microseconds / 1000

            avg_ms = sum / len(records)
            print('record avg put in %sms, total %sms' % (avg_ms, sum))

    def run(self):
        while True:
            self.add_records(self.default_records)
            self.put_all_records()


if __name__ == '__main__':
    print('connecting to region')
    kinesis = boto.kinesis.connect_to_region(region_name=config.AWS_REGION)
    print('get_or_create_stream')
    stream = Streams(kinesis).get_or_create_stream(
        stream_name=config.AWS_KINESIS_STREAM_NAME,
        shard_count=config.AWS_KINESIS_SHARD_COUNT)
    print('producing data into stream')
    producer = Producer(
        stream_name=config.AWS_KINESIS_STREAM_NAME,
        partition_key=config.AWS_KINESIS_PARTITION_KEY,
        polling_timedelta=config.AWS_KINESIS_POLLING_TIMEDELTA)
    producer.run()
