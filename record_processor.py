#!/usr/bin/env python3
import logging
import time
import base64
import config
from amazon_kclpy.kcl import (
    Checkpointer,
    RecordProcessorBase,
    CheckpointError,
)
from errors import (
    ShutdownException,
    ThrottlingException,
    InvalidStateException
)

logger = logging.getLogger(__name__)


class RecordProcessor(RecordProcessorBase):
    """
    A RecordProcessor processes a shard in a stream. Its methods will be called with this pattern:
    - initialize will be called once
    - process_records will be called zero or more times
    - shutdown will be called if this MultiLangDaemon instance loses the lease to this shard
    """

    SLEEP_SECONDS = config.SLEEP_SECONDS
    CHECKPOINT_RETRIES = config.CHECKPOINT_RETRIES
    CHECKPOINT_FREQ_SECONDS = config.CHECKPOINT_FREQ_SECONDS

    def __init__(self):
        self.largest_seq = None
        self.last_checkpoint_time = None

    def initialize(self, shard_id: str):
        """
        Called once by a KCLProcess before any calls to process_records
        :type shard_id: str
        :param shard_id: The shard id that this processor is going to be working on.
        """
        self.last_checkpoint_time = time.time()
        logger.info('Initialized with Shard Id %s', shard_id)

    def checkpoint(self, checkpointer: Checkpointer, sequence_number: str=None):
        """
        Checkpoints with retries on retryable exceptions.

        :param checkpointer: A checkpointer provided to either process_records or shutdown
        :param sequence_number: A sequence number to checkpoint at.
        """

        try:
            for attempt in range(0, self.CHECKPOINT_RETRIES):
                try:
                    return checkpointer.checkpoint(sequence_number)
                except CheckpointError as exception:
                    self.checkpoint_error(attempt, sequence_number, exception)
                finally:
                    time.sleep(self.SLEEP_SECONDS)
        except CheckpointError:
            logger.critical('Critical error during checkpoint')

    def checkpoint_error(self, attempt: int, sequence_number: str, exception: CheckpointError):
        """
        Handles a CheckpointError, optionally stopping all current processing

        :param attempt: Number of attempts made to process current data
        :param exception: The CheckpointError that occurred while processing
        """

        error = exception.value
        if error == ShutdownException.__class__.__name__:
            """
            A ShutdownException indicates that this record processor should be shutdown. This is due to
            some failover event, e.g. another MultiLangDaemon has taken the lease for this shard.
            """
            logger.info('Encountered shutdown exception at sequence %s, skipping checkpoint', sequence_number)
            raise ShutdownException(error)
        elif error == ThrottlingException.__class__.__name__:
            """
            A ThrottlingException indicates that one of our dependencies is is over burdened, e.g. too many
            dynamo writes. We will sleep temporarily to let it recover.
            """
            if self.CHECKPOINT_RETRIES - 1 == attempt:
                logger.error('Failed to checkpoint at sequence %s after %s attempts, giving up.',
                             sequence_number,
                             attempt)
                raise ThrottlingException(error)
            else:
                logger.info('Was throttled during checkpoint at sequence %s, will attempt again in %s seconds',
                            sequence_number,
                            self.SLEEP_SECONDS)

        elif error == InvalidStateException.__class__.__name__:
            logger.error('MultiLangDaemon reported an invalid state during checkpoint.')
        else:
            logger.error('Encountered an error during checkpoint at sequence %s, error was %s.',
                         sequence_number,
                         attempt)

    def process_record(self, data: str, partition_key: str, sequence_number: int):
        """
        Called for each record that is passed to process_records.

        :param data: The blob of data that was contained in the record.
        :param partition_key: The key associated with this recod.
        :param sequence_number: The sequence number associated with this record.
        """
        return

    def process_records(self, records: list, checkpointer: Checkpointer):
        """
        Called by a KCLProcess with a list of records to be processed and a checkpointer which accepts sequence numbers
        from the records to indicate where in the stream to checkpoint.

        :param records: A list of records that are to be processed. Record "data" is a base64
            encoded string. You can use base64.b64decode to decode the data into a string.
            We currently do not do this decoding for you so as to leave it to your discretion
            whether you need to decode this particular piece of data.
        :param checkpointer: A checkpointer which accepts a sequence number or no parameters.

        Example record:
        {"data":"<base64 encoded string>","partitionKey":"someKey","sequenceNumber":"1234567890"}
        """
        try:
            for record in records:
                # record data is base64 encoded, so we need to decode it first
                data = base64.b64decode(record.get('data'))
                seq = record.get('sequenceNumber')
                seq = int(seq)
                key = record.get('partitionKey')
                self.process_record(data, key, seq)
                if self.largest_seq is None or seq > self.largest_seq:
                    self.largest_seq = seq
            # Checkpoints every 60 seconds
            if time.time() - self.last_checkpoint_time > self.CHECKPOINT_FREQ_SECONDS:
                self.checkpoint(checkpointer, str(self.largest_seq))
                self.last_checkpoint_time = time.time()
        except Exception as e:
            logger.error("Encountered an exception while processing records. Exception was {e}\n".format(e=e))

    def shutdown(self, checkpointer: Checkpointer, reason: str):
        """
        Called by a KCLProcess instance to indicate that this record processor should
        shutdown. After this is called, there will be no more calls to any other methods
        of this record processor.

        :param checkpointer: A checkpointer which accepts a sequence number or no parameters.
        :param reason: The reason this record processor is being shutdown, either
            TERMINATE or ZOMBIE. If ZOMBIE, clients should not checkpoint because
            there is possibly another record processor which has acquired the lease
            for this shard. If TERMINATE then checkpointer.checkpoint() should be
            called to checkpoint at the end of the shard so that this processor will
            be shutdown and new processor(s) will be created to for the child(ren) of
            this shard.
        """
        try:
            if reason == 'TERMINATE':
                # Inside checkpoint with no parameter will checkpoint at the
                # largest sequence number reached by this processor on this
                # shard id
                logger.info('Was told to terminate, will attempt to checkpoint.')
                self.checkpoint(checkpointer, None)
            else:
                # reason == 'ZOMBIE'
                logger.info('Shutting down due to failover. Will not checkpoint.')
        except:
            pass
