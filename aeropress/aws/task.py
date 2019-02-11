import boto3
from typing import List, Dict, Any  # noqa

from aeropress.aws.log import handle_logs
from aeropress import logger
from aeropress import AeropressException

ecs_client = boto3.client('ecs')


def register_all(tasks: list, clean_stale: bool) -> None:
    # First, validate log definitions.
    _validate_log_definitions(tasks)

    # Handle logs; create missing log groups, set retention days, clean stale log groups if necessary.
    handle_logs(tasks, clean_stale_log_groups=clean_stale)

    # Register task definitions
    _register_task_definitions(tasks)


# TODO: Do not delete a task if it is still being used by a service.
def clean_stale_tasks() -> None:
    """
    Clean stale tasks. Leave only active revision.
    """
    active_task_revisions = {}  # type: Dict[str, int]
    all_task_definitions = []  # type: List[Dict[str, Any]]
    next_token = None
    while True:
        if next_token:
            resp = ecs_client.list_task_definitions(status='ACTIVE', maxResults=100, nextToken=next_token)
        else:
            resp = ecs_client.list_task_definitions(status='ACTIVE', maxResults=100)

        for task_definition_arn in resp['taskDefinitionArns']:
            # Example arn: 'arn:aws:ecs:eu-west-00000000:task-definition/task-foo:23'
            parts = task_definition_arn.split(':')
            name = parts[-2].split('/')[1]
            revision = int(parts[-1])
            all_task_definitions.append(
                {
                    'name': name,
                    'revision': revision,
                }
            )

            # Initialize dict.
            if active_task_revisions.get(name) is None:
                active_task_revisions[name] = revision
                continue

            # Set the active revision.
            if revision > active_task_revisions[name]:
                active_task_revisions[name] = revision

        next_token = resp.get('nextToken')

        # All task definitions are loaded.
        if not next_token:
            break

    for task_definition in all_task_definitions:
        task_name = task_definition['name']
        revision = task_definition['revision']
        active_revision = active_task_revisions[task_name]

        if revision == active_revision:
            continue

        if revision > active_revision:
            logger.error('Active revision is not set correct! Active revision is set to %s for ',
                         active_revision,
                         task_name)
            raise AeropressException()

        stale_task_name = task_name + ':' + str(revision)
        active_task_name = task_name + ':' + str(active_revision)
        logger.info('Deregistering task definition %s. Active revision is %s', stale_task_name, active_task_name)
        response = ecs_client.deregister_task_definition(taskDefinition=stale_task_name)
        logger.debug('Deregistered stale task: %s', response)

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
                'entryPoint': container_definition['entryPoint'],
                'environment': container_definition['environment'],
            })

        logger.info('Creating task definition: %s', task_dict['family'])
        response = ecs_client.register_task_definition(
            family=task_dict['family'],
            taskRoleArn=task_dict['taskRoleArn'],
            executionRoleArn=task_dict['executionRoleArn'],
            networkMode=task_dict['networkMode'],
            containerDefinitions=container_definitions,
            requiresCompatibilities=task_dict['requiresCompatibilities'],
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
