import dataclasses as dc
import logging
import time
from contextlib import AbstractContextManager
from queue import Queue, ShutDown
from threading import Thread
from typing import cast, TYPE_CHECKING

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
from google.cloud import bigquery
from tenacity import wait_exponential_jitter, stop_after_delay, sleep, retry_if_exception, Retrying

from .transfer_config import Destination, Source
from .utils import value_to_dynamo

if TYPE_CHECKING:
    from mypy_boto3_dynamodb import DynamoDBClient as CoreDynamoDBClient

logger = logging.getLogger("bq_to_dynamo")

QUEUE_MAX_SIZE = 1_000  # Shared input queue for DynamoDB writers
BQ_PAGE_SIZE = 10_000

THREAD_POOL_START_SIZE = 3
THREAD_POOL_MAX_SIZE = 30
THREAD_POOL_STABILISATION_WINDOW = 15  # Seconds, to wait after a new thread is added to the pool
WRITER_MAX_BATCH_SIZE = 25  # As per AWS docs: https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_BatchWriteItem.html

THROTTLING_ERRORS = [
    "ProvisionedThroughputExceededException",
    "ThrottlingException",
    "RequestLimitExceeded",
]

AWS_CONNECTION_PARAMS = Config(
    max_pool_connections=THREAD_POOL_MAX_SIZE,
    retries={  # type: ignore
        "max_attempts": 0,  # We will retry manually, with exponential backoff
        "mode": "standard"
    }
)


# Open a new session every time (when a worker starts), to avoid ExpiredTokenException error
def create_aws_session() -> boto3.session.Session:
    return boto3.session.Session()


def create_bq_client(project: str) -> bigquery.Client:
    return bigquery.Client(project=project)


@dc.dataclass(slots=True)
class TransferProgressSpan:
    """A span of progress (like 1s window)."""
    started_at: float = dc.field(default_factory=time.monotonic)
    rows_written: int = 0
    rows_processed: int = 0  # Rows were sent (not necessarily written) to DynamoDB
    rows_unprocessed: int = 0  # Rows (in a batch) that were not written (e.g. due to throttling)
    throttled_writes: int = 0


@dc.dataclass(slots=True)
class TransferProgress:
    total_rows: int = 0  # Total rows in the source table / query (BigQuery)
    rows_read: int = 0  # Rows read from BigQuery

    current: TransferProgressSpan = dc.field(default_factory=TransferProgressSpan)

    @property
    def speed(self) -> float:  # Transfer speed, rows/sec.
        current_span = self.current
        return current_span.rows_written / (time.monotonic() - current_span.started_at)

    def run(self):
        while True:
            sleep(10)  # Span duration
            self.current = TransferProgressSpan()


@dc.dataclass(slots=True)
class ConsumerTransferProgress:
    """Progress of a single consumer thread."""
    total: TransferProgress
    started_at: float = dc.field(default_factory=time.monotonic)
    rows_written: int = 0
    rows_processed: int = 0  # Rows were sent (not necessarily written) to DynamoDB
    rows_unprocessed: int = 0  # Rows (in a batch) that were not written (e.g. due to throttling)
    throttled_writes: int = 0

    @property
    def speed(self) -> float:  # Transfer speed, rows/sec.
        return self.rows_written / (time.monotonic() - self.started_at)

    def handle_throttling_error(self, _):
        self.throttled_writes += 1
        self.total.current.throttled_writes += 1

    def handle_write_result(self, num_items: int, num_unprocessed_items: int):
        assert num_items > num_unprocessed_items, "Whole batch was not processed"
        self.rows_written += num_items - num_unprocessed_items
        self.rows_processed += num_items
        self.rows_unprocessed += num_unprocessed_items
        current_span = self.total.current
        current_span.rows_written += num_items - num_unprocessed_items
        current_span.rows_processed += num_items
        current_span.rows_unprocessed += num_unprocessed_items


def can_scale_up(progress: TransferProgress) -> bool:
    current_span = progress.current
    if current_span.throttled_writes > 0:
        return False  # Throttling errors in the current span, wait
    if current_span.rows_processed > 0:
        ratio = current_span.rows_unprocessed / current_span.rows_processed
        return ratio < 0.1  # Scale up if less than 10% of rows were unprocessed (e.g. throttled)
    return False


class UnprocessedItemsError(Exception):
    pass


def create_retry_policy(progress: ConsumerTransferProgress) -> Retrying:
    return Retrying(
        reraise=True,  # Reraise the original exception after all retries fail (instead of RetryError)
        after=progress.handle_throttling_error,
        retry=retry_if_exception(lambda e: isinstance(e, UnprocessedItemsError) or (
                isinstance(e, ClientError) and e.response["Error"]["Code"] in THROTTLING_ERRORS  # type: ignore
        )),
        stop=stop_after_delay(3 * 60),  # Stop after 3 minutes
        wait=wait_exponential_jitter(initial=1, max=10)  # First wait 1 second, increase exponentially, max 10 seconds
    )


def bq_row_to_dynamo(row: bigquery.Row) -> dict:
    return {k: value_to_dynamo(row[k]) for k in row.keys()}


class DynamoDbClient:
    def __init__(self, progress: ConsumerTransferProgress):
        self.progress = progress
        self.retryer = create_retry_policy(progress)
        # We can refresh the client later, like every time N minutes, to avoid ExpiredTokenException
        self._client: "CoreDynamoDBClient" = create_aws_session().client("dynamodb", config=AWS_CONNECTION_PARAMS)

    def _batch_write_item(self, table_arn: str, items: list) -> list:
        response = self._client.batch_write_item(
            RequestItems={  # type: ignore
                table_arn: items
            }
        )
        table_unprocessed_items = []
        if unprocessed_items := response.get("UnprocessedItems"):
            table_unprocessed_items = list(unprocessed_items.values())[0]
        self.progress.handle_write_result(len(items), len(table_unprocessed_items))
        return table_unprocessed_items

    def batch_write_item(self, table_arn: str, items: list) -> None:
        payload = items
        for attempt in self.retryer:
            with attempt:
                if unprocessed := self._batch_write_item(table_arn, payload):
                    payload = unprocessed  # Retry with unprocessed items only
                    raise UnprocessedItemsError()


# See boto3.dynamodb.table.BatchWriter for reference
class BatchDynamoWriter(AbstractContextManager):
    def __init__(self, config: Destination, progress: ConsumerTransferProgress):
        self.config = config
        self.dynamo_client = DynamoDbClient(progress)
        self._items = []

    def __exit__(self, exc, _, __):
        if not exc:
            while self._items:
                self._flush()

    def put(self, row: bigquery.Row):
        item = {'PutRequest': {'Item': bq_row_to_dynamo(row)}}
        self._items.append(item)
        if len(self._items) >= WRITER_MAX_BATCH_SIZE:
            self._flush()

    def _flush(self):
        items = self._items
        self._items = []
        if not items:
            return  # Nothing to flush
        assert len(items) <= WRITER_MAX_BATCH_SIZE
        self.dynamo_client.batch_write_item(self.config.table_arn, items)


class ConsumerThread(Thread):
    def __init__(self, queue: Queue, config: Destination, progress: TransferProgress):
        super().__init__()
        self.queue = queue
        self.config = config
        self.progress = ConsumerTransferProgress(progress)
        self.exception: Exception | None = None

    def run(self):
        self.progress.started_at = time.monotonic()
        try:
            with BatchDynamoWriter(self.config, self.progress) as writer:
                while True:
                    writer.put(self.queue.get())
        except ShutDown:
            logger.info("Queue has been closed, exiting consumer thread")
        except Exception as e:
            self.exception = e  # Will be reraised by the pool


class WritersPool:
    def __init__(self, queue: Queue, config: Destination, progress: TransferProgress):
        self._queue = queue
        self._config = config
        self._progress = progress
        self._workers: list[ConsumerThread] = []
        self._controller = Thread(daemon=True, target=self._run_controller)

    @property
    def progress(self) -> list[ConsumerTransferProgress]:
        return [worker.progress for worker in self._workers]

    def is_alive(self) -> bool:
        exceptions = [worker.exception for worker in self._workers if worker.exception]
        if exceptions:
            raise ExceptionGroup("Unhandled exceptions in consumer threads", exceptions)
        return any(thread.is_alive() for thread in self._workers)

    def _start_consumer_thread(self):
        consumer = ConsumerThread(self._queue, self._config, self._progress)
        consumer.start()
        self._workers.append(consumer)

    def _run_controller(self):
        while len(self._workers) < THREAD_POOL_MAX_SIZE:
            sleep(THREAD_POOL_STABILISATION_WINDOW)
            while True:
                if can_scale_up(self._progress):
                    logger.info("Adding new consumer thread to the pool")
                    self._start_consumer_thread()
                    break
                sleep(1)
        logger.info("Consumer thread pool is full, no more threads will be added")

    def start(self):
        logger.info("Adding initial consumer thread(s) to the pool")
        for _ in range(THREAD_POOL_START_SIZE):
            self._start_consumer_thread()

        # Start the control thread (to grow the pool gradually)
        self._controller.start()


class BigQueryReader:
    def __init__(self, config: Source, progress: TransferProgress):
        self.config = config
        self.progress = progress
        # BQ client is thread-safe, see https://github.com/googleapis/python-bigquery/pull/132/files
        self.client = create_bq_client(config.project)

    def prepare_results(self) -> bigquery.Table:
        if self.config.query:
            query_job = self.client.query(self.config.query)  # Create a query job
            query_job.result()  # Wait for the query to complete

            # Get the destination table for the query results.
            #
            # All queries write to a destination table. If a destination table is not
            # specified, the BigQuery populates it with a reference to a temporary
            # anonymous table after the query completes.
            table_ref = query_job.destination
            if not table_ref:
                raise RuntimeError("No destination table available")
        else:
            table_ref = cast(str, self.config.table)

        # Get the schema (and other properties) for the destination table.
        #
        # A schema is useful for converting from BigQuery types to Python types.
        destination = self.client.get_table(table_ref)
        if destination.num_rows:
            self.progress.total_rows = destination.num_rows
        else:
            logger.warning("Cannot determine the number of rows in the destination table")

        return destination

    def read_into(self, source: bigquery.Table, queue: Queue):
        try:
            # Download rows.
            #
            # The client library automatically handles pagination.
            rows = self.client.list_rows(source, page_size=BQ_PAGE_SIZE)
            for row in rows:
                # Schedule the row for writing to DynamoDB (or wait if the queue is full)
                queue.put(row)
                self.progress.rows_read += 1
        except ShutDown:
            logger.info("Queue has been closed, stopping BigQuery reader")
        except Exception:  # noqa
            logger.exception("Unexpected exception occurred while reading from BigQuery")
        finally:
            queue.shutdown()  # Signal completion to the writers
