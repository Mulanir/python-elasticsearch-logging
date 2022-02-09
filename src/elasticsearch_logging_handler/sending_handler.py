
import sys
from datetime import datetime
from logging import Handler, LogRecord
import threading
import traceback as tb

from elasticsearch.client import Elasticsearch
from elasticsearch.helpers import bulk
import pytz


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
            except Exception:
                tb.print_exc(file=sys.stderr)

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
