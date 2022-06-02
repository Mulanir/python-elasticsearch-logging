import time
import logging

import pytest
import docker
import docker.models.containers as containers
import docker.errors
import elasticsearch as es


@pytest.fixture(scope='session')
def elastic_service():
    port = '9200'

    docker_client = docker.from_env()
    docker_image_name = 'elasticsearch:7.17.3'
    docker_container_name = 'elasticsearch_test_emulator'

    _remove_existing_test_container(docker_client, docker_container_name)

    container = docker_client.containers.run(
        docker_image_name, detach=True,
        ports={'9200': port},
        environment={'discovery.type': 'single-node'},
        name=docker_container_name)

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
            container.remove(v=True)
        except Exception as ex:
            logging.exception(ex)

        raise TimeoutError('Elasticsearch container ready failed.')

    yield host

    container.stop()
    container.remove(v=True)


def _remove_existing_test_container(docker_client: docker.DockerClient, container_name):
    try:
        existing_container: containers.Container = docker_client.containers.get(
            container_name)

        existing_container.stop()
        existing_container.remove(v=True)
    except docker.errors.NotFound:
        return


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


@pytest.fixture(scope='function')
def elastic_host(elastic_service):
    es_client: es.Elasticsearch = es.Elasticsearch(
        hosts=[elastic_service])

    yield elastic_service

    es_client.indices.delete(index='test-index', ignore=[400, 404])


@pytest.fixture
def debug_logger():
    test_logger = logging.getLogger('test')
    test_logger.setLevel(logging.DEBUG)

    def factory(handler):
        test_logger.addHandler(handler)

        return test_logger

    yield factory

    test_logger.handlers.clear()
