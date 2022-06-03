import logging

import pytest

from src.elasticsearch_logging_handler import ElasticHandler


def test_wrong_host(capsys: pytest.CaptureFixture[str]):
    random_host = 'http://123123.123'
    index = 'test_index'

    handler = ElasticHandler(random_host, index)

    test_logger = logging.getLogger('test')
    test_logger.addHandler(handler)
    test_logger.critical('test')

    captured = capsys.readouterr()

    assert captured.out == ''
    assert captured.err == ''


def test_host_is_none(capsys: pytest.CaptureFixture[str]):
    random_host = None
    index = 'test_index'

    handler = ElasticHandler(random_host, index)

    test_logger = logging.getLogger('test')
    test_logger.addHandler(handler)
    test_logger.critical('test')

    captured = capsys.readouterr()

    assert captured.out == ''
    assert captured.err == ''
