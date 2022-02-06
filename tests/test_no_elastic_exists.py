import logging
from src.elasticsearch_logging_handler.handlers import ElasticHandler


def test_wrong_host():
    random_host = 'http://123123.123'
    index = 'test_index'

    handler = ElasticHandler(random_host, index)

    test_logger = logging.getLogger('test')
    test_logger.addHandler(handler)
    test_logger.critical('test')
