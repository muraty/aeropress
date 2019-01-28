import os

import boto3

from aeropress.aws.log import get_existing_log_group_names, create_missing_log_groups, clean_stale_log_groups
from aeropress import logger

ecs_client = boto3.client('ecs', region_name='eu-west-1')


def register_all(tasks: list) -> None:
    # First, validate log definitions.
    _validate_log_definitions(tasks)

    # Prepare log groups
    # TODO: Make this inside log.py
    defined_log_group_names = _get_defined_log_group_names(tasks)
    existing_log_group_names = get_existing_log_group_names()

    # Create missing log groups
    # TODO: Make this inside log.py
    missing_log_group_names = defined_log_group_names.difference(existing_log_group_names)
    create_missing_log_groups(missing_log_group_names)

    # Delete stale log groups
    # TODO: Make this inside log.py
    stale_log_group_names = existing_log_group_names.difference(defined_log_group_names)
    clean_stale_log_groups(stale_log_group_names)

    # Register task definitions
    _register_task_definitions(tasks)


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
                'entryPoint': container_definition['entryPoint'],
            })

        response = ecs_client.register_task_definition(
            family=task_dict['family'],
            taskRoleArn=task_dict['taskRoleArn'],
            executionRoleArn=task_dict['executionRoleArn'],
            networkMode=task_dict['networkMode'],
            containerDefinitions=container_definitions,
            requiresCompatibilities=task_dict['requiresCompatibilities'],
        )

        logger.info('Created task definition: %s', task_dict['family'])
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
                os._exit(1)

            if options['awslogs-stream-prefix'] != 'ecs':
                logger.error("logstream prefixes must be 'ecs' : %s", options)
                os._exit(1)


def _get_defined_log_group_names(tasks: list) -> set:
    defined_group_names = set()
    for task_dict in tasks:
        for container_definition in task_dict['containerDefinitions']:
            if not container_definition.get('logConfiguration'):
                continue

            if not container_definition['logConfiguration'].get('options'):
                continue

            defined_group_names.add(container_definition['logConfiguration']['options']['awslogs-group'])

    return defined_group_names
