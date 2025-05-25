import logging
from queue import Queue
from threading import Thread

from rich.layout import Layout
from rich.live import Live
from rich.panel import Panel
from rich.progress import Progress, BarColumn, TextColumn, TimeElapsedColumn
from rich.table import Table

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("bq_to_dynamo.runner")
logging.getLogger("botocore.credentials").setLevel(logging.WARNING)

import rich_click as click
from tenacity import sleep

from .app import QUEUE_MAX_SIZE, BigQueryReader, WritersPool, TransferProgress
from .transfer_config import read_transfer_config


class ProgressMonitor:
    def __init__(self, stats: TransferProgress, writers: WritersPool):
        self.stats = stats
        self.writers = writers
        self.layout = layout = Layout()
        layout.split(
            Layout(name="progress_bar", size=3),
            Layout(name="current_stats", size=3),
            Layout(name="threads"),
        )
        self.overall_progress = overall_progress = Progress(
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            "[progress.percentage]{task.percentage:>3.0f}%",
            "•",
            TextColumn("{task.completed}/{task.total} rows"),
            TimeElapsedColumn(),
            expand=True,
        )
        self.overall_task = overall_progress.add_task(
            "Total Progress", total=stats.total_rows, completed=stats.rows_read)
        layout["progress_bar"].update(Panel(overall_progress))

    def _render_current(self):
        stats_table = Table.grid()
        stats_table.add_row("Transfer speed (rows/sec):", f"{self.stats.speed:.2f}")
        return Panel(stats_table, title="Current Span Stats")

    def _render_workers(self):
        thread_table = Table(title="Threads", show_header=True, header_style="bold magenta")
        thread_table.add_column("Thread")
        thread_table.add_column("Speed (rows/sec)")
        thread_table.add_column("Written")
        thread_table.add_column("Sent")
        thread_table.add_column("Throttled")
        thread_table.add_column("Throttled (batch requests)")
        for i, consumer in enumerate(self.writers.progress):
            thread_table.add_row(f"Thread {i + 1}",
                                 f"{consumer.speed:.2f}",
                                 f"{consumer.rows_written}",
                                 f"{consumer.rows_processed}",
                                 f"{consumer.rows_unprocessed}",
                                 f"{consumer.throttled_writes}")
        return thread_table

    def update(self):
        self.overall_progress.update(self.overall_task, completed=self.stats.rows_read)
        self.layout["current_stats"].update(self._render_current())
        self.layout["threads"].update(self._render_workers())


@click.command()
@click.argument("transfer_config_path", type=click.Path(exists=True, dir_okay=False))
def main(transfer_config_path):
    transfer_config = read_transfer_config(transfer_config_path)

    stats = TransferProgress()
    # Shared queue for all threads, with rows received from BigQuery
    rows_queue = Queue(maxsize=QUEUE_MAX_SIZE)

    reader = BigQueryReader(transfer_config.source, stats)
    writers = WritersPool(rows_queue, transfer_config.destination, stats)

    logger.info("Preparing the source table...")
    bq_source_table = reader.prepare_results()

    stats_thread = Thread(daemon=True, target=stats.run)
    reader_thread = Thread(target=reader.read_into, args=(bq_source_table, rows_queue))

    stats_thread.start()
    reader_thread.start()
    writers.start()

    logger.info("Starting transfer...")
    sleep(1)  # Give some time writer threads to start

    try:
        monitor = ProgressMonitor(stats, writers)
        with Live(monitor.layout, refresh_per_second=1, screen=True):
            while writers.is_alive() and reader_thread.is_alive():
                sleep(0.5)
                monitor.update()
    except KeyboardInterrupt:
        logger.info("Aborted, shutting down gracefully")
    finally:
        if not rows_queue.is_shutdown:
            rows_queue.shutdown()  # Both reader and writers should stop (gracefully) after it


if __name__ == "__main__":
    main()
