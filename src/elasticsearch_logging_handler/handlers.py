import queue
from datetime import datetime
from logging import NOTSET, Handler, LogRecord
from logging.handlers import QueueHandler, QueueListener
from typing import Union
import threading

import pytz
import elasticsearch as es
from elasticsearch.client import Elasticsearch
from elasticsearch.helpers import bulk


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

        return super().close()

    def _create_elastic_client(self, host) -> Union[Elasticsearch, None]:
        # Check all elastic configss are not None
        if host is None:
            return None

        es_client: Elasticsearch = es.Elasticsearch(
            hosts=[host])

        try:
            es_client.info()

            return es_client
        except Exception as ex:
            self.handleError(ex)

            return None


class ObjectQueueHandler(QueueHandler):
    """QueueHandler that preserves message as an object in the separate field - msg_object."""

    def prepare(self, record: LogRecord) -> LogRecord:
        """Create msg_object as raw message before it will be formatted as str."""

        record.__setattr__('msg_object', record.msg)

        record = super().prepare(record)

        return record


class ElasticSendingHandler(Handler):
    def __init__(self, level,
                 es_client: Elasticsearch, index: str,
                 flush_period: float = 1,
                 batch_size: int = 1000,
                 timezone: str = None) -> None:
        super().__init__(level=level)

        self._es_client = es_client
        self._index = index

        self._flush_period = flush_period
        self._batch_size = batch_size
        self._timezone = timezone

        self.__message_buffer = []
        self.__buffer_lock = threading.Lock()

        self.__timer = None
        self.__schedule_flush()

    def __schedule_flush(self):
        """Start timer that one-time flushes message buffer."""

        if self.__timer is None:
            self.__timer = threading.Timer(self._flush_period, self.flush)
            self.__timer.setDaemon(True)
            self.__timer.start()

    def flush(self):
        """Send all messages from buffer to Elasticsearch."""

        if self.__timer is not None and self.__timer.is_alive():
            self.__timer.cancel()
        self.__timer = None

        if self.__message_buffer:
            try:
                with self.__buffer_lock:
                    actions, self.__message_buffer = self.__message_buffer, []

                bulk(self._es_client, actions, stats_only=True)
            except Exception as exception:
                self.handleError(exception)

    def emit(self, record: LogRecord):
        """Add log message to the buffer. \n
        If the buffer is filled up, immedeately flush it."""

        action = self.__prepare_action(record)

        with self.__buffer_lock:
            self.__message_buffer.append(action)

        if len(self.__message_buffer) >= self._batch_size:
            self.flush()
        else:
            self.__schedule_flush()

    def __prepare_action(self, record: LogRecord):
        timestamp_dt: datetime = datetime.fromtimestamp(record.created)

        if self._timezone:
            tz_info = pytz.timezone('Europe/Kiev')
            timestamp_dt: datetime = timestamp_dt.astimezone(tz_info)

        timestamp_iso = timestamp_dt.isoformat()

        message = record.msg_object

        action = {
            '_index': self._index,
            '_op_type': 'index',
            '@timestamp': timestamp_iso,
            'level': record.levelname,
            'content': message
        }

        return action

    def close(self):
        self.flush()

        return super().close()
