
from logging import LogRecord
from logging.handlers import QueueHandler


class ObjectQueueHandler(QueueHandler):
    """QueueHandler that preserves message as an object in the separate field - msg_object."""

    def prepare(self, record: LogRecord) -> LogRecord:
        """Create msg_object as raw message before it will be formatted as str."""

        record.__setattr__('msg_object', record.msg)

        record = super().prepare(record)

        return record
