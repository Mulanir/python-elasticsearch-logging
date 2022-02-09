import os
import time
import logging
import socket

import pytest
import docker
import elasticsearch as es
from elasticsearch.client import Elasticsearch


@pytest.fixture(scope='session')
def elastic_host():
    port = _get_free_port()

    docker_client = docker.from_env()
    docker_image_name = 'elasticsearch:7.17.0'
    docker_container_name = 'elasticsearch_test_emulator'

    container = docker_client.containers.run(
        docker_image_name, detach=True, ports={
            '9200': port
        }, environment={
            'discovery.type': 'single-node'
        }, name=docker_container_name)

    for _ in range(60):
        if _check_running(docker_client, docker_container_name):
            break
        else:
            time.sleep(1)
    else:
        try:
            container.stop()
            container.remove()
        except Exception as ex:
            logging.exception(ex)

        raise TimeoutError('Elasticsearch container startup failed.')

    host = f'http://localhost:{port}'

    for _ in range(60):
        if _check_ready(host):
            break
        else:
            time.sleep(1)
    else:
        try:
            container.stop()
            container.remove()
        except Exception as ex:
            logging.exception(ex)

        raise TimeoutError('Elasticsearch container ready failed.')

    yield host

    container.stop()
    container.remove()


def _get_free_port():
    sock = socket.socket()
    sock.bind(('', 0))
    port = sock.getsockname()[1]
    sock.close()

    return port


def _check_running(docker_client, container_name):
    RUNNING = 'running'

    container = docker_client.containers.get(container_name)
    container_state = container.attrs['State']
    container_is_running = container_state['Status'] == RUNNING

    return container_is_running


def _check_ready(host):
    try:
        es_client = es.Elasticsearch(
            hosts=[host])
        es_client.info()

        return True
    except:
        return False


@pytest.fixture
def get_logger():
    def inner(handler):
        test_logger = logging.getLogger('test')
        test_logger.addHandler(handler)

        return test_logger

    return inner
