import time
from typing import List, Dict, Any, Generator  # noqa
from collections import defaultdict

import boto3
from botocore.exceptions import ClientError

from aeropress.aws.log import handle_logs
from aeropress import logger
from aeropress import AeropressException

ecs_client = boto3.client('ecs')


def register_all(tasks: list, clean_stale: bool = False) -> None:
    # First, validate log definitions.
    _validate_log_definitions(tasks)

    # Handle logs; create missing log groups, set retention days, clean stale log groups if necessary.
    handle_logs(tasks, clean_stale_log_groups=clean_stale)

    # Register task definitions
    _register_task_definitions(tasks)


def _retrieve_service_task_definitions() -> list:
    cluster_service_dict = defaultdict(list)  # type: Dict[str, List[str]]

    cluster_arns = ecs_client.list_clusters()['clusterArns']

    # Retrieve all services.
    for cluster_arn in cluster_arns:
        next_token = None
        while True:
            if next_token:
                resp = ecs_client.list_services(cluster=cluster_arn, maxResults=100, nextToken=next_token)
            else:
                resp = ecs_client.list_services(cluster=cluster_arn, maxResults=100)

            cluster_service_dict[cluster_arn].extend(resp['serviceArns'])

            next_token = resp.get('nextToken')

            # All services are loaded.
            if not resp.get('nextToken'):
                break

    # Tasks under using.
    service_task_definitions = []  # type: List[str]
    for cluster, services in cluster_service_dict.items():
        service_chunks = [service for service in _chunks(services, 10)]  # service names can have at most 10 items
        for service_chunk in service_chunks:
            resp = ecs_client.describe_services(cluster=cluster, services=service_chunk)
            for service in resp['services']:
                service_task_definitions.append(service['taskDefinition'])

    return service_task_definitions


def _chunks(l: list, n: int) -> Generator:
    for i in range(0, len(l), n):
        yield l[i:i + n]


def clean_stale_tasks() -> None:
    """
    Clean stale tasks. Leave only active tasks those are used by services.
    """
    service_task_definitions = _retrieve_service_task_definitions()

    all_task_definitions = []  # type: List[Dict[str, Any]]
    next_token = None
    while True:
        if next_token:
            resp = ecs_client.list_task_definitions(status='ACTIVE', maxResults=100, nextToken=next_token)
        else:
            resp = ecs_client.list_task_definitions(status='ACTIVE', maxResults=100)

        all_task_definitions.extend(resp['taskDefinitionArns'])

        next_token = resp.get('nextToken')

        # All task definitions are loaded.
        if not next_token:
            break

    for task_definition in all_task_definitions:
        if task_definition not in service_task_definitions:
            logger.info('Deregistering task definition %s', task_definition)
            slept = 0
            while True:
                try:
                    response = ecs_client.deregister_task_definition(taskDefinition=task_definition)
                    logger.debug('Deregistered stale task: %s', response)
                except ClientError as e:
                    if e.response['Error']['Code'] == 'ThrottlingException':
                        logger.info('Request is throttled. Waiting...')
                        time.sleep(5)
                        slept += 5
                else:
                    break

                # Give up trying after 20 seconds.
                if slept >= 20:
                    break

    logger.info('Cleaned all stale tasks.')


def _register_task_definitions(tasks: list) -> None:
    for task_dict in tasks:
        # Create container definitions.
        container_definitions = []
        for container_definition in task_dict['containerDefinitions']:
            container_definitions.append({
                'name': container_definition['name'],
                'image': container_definition['image'],
                'logConfiguration': container_definition['logConfiguration'],
                'memoryReservation': container_definition['memoryReservation'],
                'cpu': container_definition.get('cpu', 0),
                'entryPoint': container_definition.get('entryPoint', []),
                'environment': container_definition.get('environment', []),
                'portMappings': container_definition.get('portMappings', []),
                'ulimits': container_definition.get('ulimits', []),
                'mountPoints': container_definition.get('mountPoints', []),
            })

        logger.info('Creating task definition: %s', task_dict['family'])
        response = ecs_client.register_task_definition(
            family=task_dict['family'],
            taskRoleArn=task_dict['taskRoleArn'],
            executionRoleArn=task_dict['executionRoleArn'],
            networkMode=task_dict['networkMode'],
            containerDefinitions=container_definitions,
            requiresCompatibilities=task_dict['requiresCompatibilities'],
            volumes=task_dict.get('volumes', []),
        )
        logger.debug('Created task definition details: %s', response)


def _validate_log_definitions(tasks: list) -> None:
    for task_dict in tasks:
        for container_definition in task_dict['containerDefinitions']:
            if not container_definition.get('logConfiguration'):
                continue

            if not container_definition['logConfiguration'].get('options'):
                continue

            options = container_definition['logConfiguration']['options']
            if not options['awslogs-group'].startswith('/ecs'):
                logger.error("log groups must start with '/ecs/' prefix: %s", options)
                raise AeropressException()

            if options['awslogs-stream-prefix'] != 'ecs':
                logger.error("logstream prefixes must be 'ecs' : %s", options)
                raise AeropressException()
