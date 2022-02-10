import queue
from logging import NOTSET, Handler, LogRecord
from logging.handlers import QueueListener
import sys
from typing import Union
import traceback as tb

import elasticsearch as es
from elasticsearch.client import Elasticsearch

from .sending_handler import ElasticSendingHandler
from .queue_handler import ObjectQueueHandler


class ElasticHandler(Handler):
    def __init__(self, host, index, level=NOTSET,
                 flush_period: float = 1, batch_size: int = 1000,
                 timezone: str = None) -> None:
        super().__init__(level)

        es_client = self._create_elastic_client(host)
        if es_client is None:
            # Disable emiting LogRecord to queue
            setattr(self, 'emit', lambda *a, **kw: None)

            return
        else:
            self._es_client = es_client

        _queue = queue.Queue(maxsize=100000)

        # Object for writing logs to the queue.
        self._queue_handler = ObjectQueueHandler(_queue)

        # Object for reading logs from the queue.
        _elastic_listener = ElasticSendingHandler(
            level, es_client, index,
            flush_period=flush_period,
            batch_size=batch_size,
            timezone=timezone)
        self._queue_listener = QueueListener(_queue, _elastic_listener)
        self._queue_listener.start()

    def emit(self, record: LogRecord) -> None:
        """Write logs to the queue."""

        self._queue_handler.emit(record)

    def close(self) -> None:
        if hasattr(self, '_queue_listener'):
            self._queue_listener.stop()

        if hasattr(self, '_es_client'):
            self._es_client.close()

        return super().close()

    def _create_elastic_client(self, host) -> Union[Elasticsearch, None]:
        # Check all elastic configss are not None
        if host is None:
            return None

        try:
            es_client: Elasticsearch = es.Elasticsearch(
                hosts=[host])
            es_client.info()

            return es_client
        except Exception:
            tb.print_exc(file=sys.stderr)

            return None
