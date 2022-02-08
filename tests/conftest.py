import os
import time
import logging
import socket

import pytest
import docker


@pytest.fixture(scope='session')
def elastic_host():
    sock = socket.socket()
    sock.bind(('', 0))
    port = sock.getsockname()[1]

    docker_client = docker.from_env()
    docker_image_name = 'docker.elastic.co/elasticsearch/elasticsearch:7.17.0'
    docker_container_name = 'elasticsearch_test_emulator'

    container = docker_client.containers.run(
        docker_image_name, detach=True, ports={
            '9200': port
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

    yield f'http://localhost:{port}'

    container.stop()
    container.remove()


def _check_running(docker_client, container_name):
    RUNNING = 'running'

    container = docker_client.containers.get(container_name)
    container_state = container.attrs['State']
    container_is_running = container_state['Status'] == RUNNING

    return container_is_running
