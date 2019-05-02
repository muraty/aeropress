from datetime import datetime, timedelta
from typing import List, Any, Dict  # noqa

import boto3

from aeropress import logger

logs_client = boto3.client('logs')


def handle_logs(tasks: list, clean_stale_log_groups: bool = False) -> None:
    # Prepare log groups
    defined_log_group_names = _get_defined_log_group_names(tasks)
    existing_log_group_names = _get_existing_log_group_names()

    # Create missing log groups
    missing_log_group_names = defined_log_group_names.difference(existing_log_group_names)
    _create_missing_log_groups(missing_log_group_names)

    # Set retention policy to 7 days.
    retention_days = 7  # TODO: Should be configurable
    for log_group_name in defined_log_group_names:
        logger.info('Setting retention days to %s for log group: %s ', retention_days, log_group_name)
        response = logs_client.put_retention_policy(logGroupName=log_group_name, retentionInDays=retention_days)
        logger.debug('Set retetion days to %s. Response: %s', retention_days, response)

    # Clean stale log groups.
    if clean_stale_log_groups:
        stale_log_group_names = existing_log_group_names.difference(defined_log_group_names)
        _clean_stale_log_groups(stale_log_group_names)


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


def _create_missing_log_groups(missing_log_group_names: set) -> None:
    for missing_log_group_name in missing_log_group_names:
        logger.info('Creating log group: %s', missing_log_group_name)
        response = logs_client.create_log_group(logGroupName=missing_log_group_name)
        logger.debug('Created log group details: %s', response)


def _clean_stale_log_groups(stale_log_group_names: set) -> None:
    for stale_log_group_name in stale_log_group_names:
        logger.info('Cleaning stale log group: %s', stale_log_group_name)
        response = logs_client.delete_log_group(logGroupName=stale_log_group_name)
        logger.debug('Clean stale log group details: %s', response)


def clean_stale_log_streams(services: list, days_ago: int) -> None:
    # Retrieve existing log streams.
    existing_logs = _get_existing_logs(services)

    # Extract container id from log stream names.
    container_instance_ids = _get_container_ids(existing_logs)

    # Retrieve failed contaniner instances.
    cluster_name = services[0]['cluster']  # We assume that, all services are in the same cluster.
    failed_container_ids = _get_failed_container_ids(cluster_name, container_instance_ids)
    logger.info('Failed container count: %s', len(failed_container_ids))

    _delete_log_streams(existing_logs, failed_container_ids, days_ago)


def _get_container_ids(existing_logs: list) -> list:
    """
    Extract container-id from existing logs and store them inside a list.
    """
    container_instance_ids = []  # type: List[str]

    for existing_log in existing_logs:
        for existing_log_stream in existing_log['log_streams']:
            log_stream_name = existing_log_stream['logStreamName']
            # Example log stream name: 'ecs/container-foo/XXXXXXX-YYYY-WWWW-ZZZZ-XXXXXXXX'
            container_id = log_stream_name.split('/')[-1]
            container_instance_ids.append(container_id)

    return container_instance_ids


def _delete_log_streams(existing_logs: list, failed_container_ids: list, days_ago: int) -> None:
    """
    Delete log streams that are older than given days_ago and their container are not running anymore.
    """
    deleted_count = 0
    for existing_log in existing_logs:
        existing_log_streams = existing_log['log_streams']
        existing_log_group_name = existing_log['log_group_name']
        for existing_log_stream in existing_log_streams:
            # Example log stream name: 'ecs/container-foo/XXXXXXX-YYYY-WWWW-ZZZZ-XXXXXXXX'
            container_id = existing_log_stream['logStreamName'].split('/')[-1]
            if container_id not in failed_container_ids:
                continue

            last_event_time = existing_log_stream.get('lastEventTimestamp', existing_log_stream.get('creationTime'))
            if not last_event_time:
                logger.warning('Neither creation time nor last event time is known! %s', existing_log_stream)
                continue

            # AWS returns timestamp in milliseconds.
            last_event_datetime = datetime.fromtimestamp(last_event_time / 1000)
            if datetime.utcnow() - timedelta(days=days_ago) < last_event_datetime:
                continue

            logger.info('Deleting log stream: %s of log group %s. Last event time: %s',
                        existing_log_stream['logStreamName'],
                        existing_log_group_name,
                        last_event_datetime)

            response = logs_client.delete_log_stream(
                logGroupName=existing_log_group_name,
                logStreamName=existing_log_stream['logStreamName'],
            )
            deleted_count += 1
            logger.debug('Deleted log stream: %s', response)

    logger.info('Deleted %s stale log streams.', deleted_count)


def _get_failed_container_ids(cluster_name: str, container_instance_ids: list) -> list:
    ecs_client = boto3.client('ecs')
    start = 0
    end = 100
    failed_container_ids = []
    while True:
        resp = ecs_client.describe_container_instances(cluster=cluster_name,
                                                       containerInstances=container_instance_ids[start:end])

        for failure in resp.get('failures', []):
            # Example arn:
            # Old Style :'arn:aws:ecs:eu-west-1:000000000:container-instance/XXXXX-WWWW-XXXX-ZZZZ-YYYY'
            # New Style :'arn:aws:ecs:eu-west-1:000000000:container-instance/<cluster-name>/XXXXX-WWWW-XXXX-ZZZZ-YYYY'
            container_id = failure['arn'].split('/')[-1]
            failed_container_ids.append(container_id)

        if end >= len(container_instance_ids):
            break

        if end + 100 >= len(container_instance_ids):
            start = end
            end = len(container_instance_ids)
        else:
            start = end
            end = end + 100

    return failed_container_ids


def _get_existing_logs(services: list) -> list:
    """
    Return List of log group names grouped by log group names
    Example:
    [
        {'name': 'log-group-1',
         'log_streams': [
            {'arn': 'arn:aws:logs:eu-west-1:00000:log-group:log-group-1:log-stream:ecs/container-foo/XXX',
             'creationTime': 1548674449535,
             'firstEventTimestamp': 1548674517172,
             'lastEventTimestamp': 1548681757955,
             'lastIngestionTime': 1548681758533,
             'logStreamName': 'ecs/container-foo/XXX',
             'storedBytes': 0,
             'uploadSequenceToken': 'YYY'}]
            },
    ]
    """
    log_group_names = []
    for service_dict in services:
        for container_definition in service_dict['task']['containerDefinitions']:
            log_group_names.append(container_definition['logConfiguration']['options']['awslogs-group'])

    # Existing log streams of all ecs log groups.
    existing_log_streams = []  # type: List[Dict[str, Any]]
    for log_group_name in log_group_names:
        next_token = None
        log_streams = []  # type: List[Dict[str, Any]]
        while True:
            if next_token:
                resp = logs_client.describe_log_streams(logStreamNamePrefix='ecs',
                                                        logGroupName=log_group_name,
                                                        nextToken=next_token,
                                                        limit=50)
            else:
                resp = logs_client.describe_log_streams(logStreamNamePrefix='ecs',
                                                        logGroupName=log_group_name,
                                                        limit=50)

                log_streams.extend(resp['logStreams'])

            next_token = resp.get('nextToken')

            # All log streams are loaded.
            if not next_token:
                break

        existing_log_streams.append({'log_group_name': log_group_name, 'log_streams': log_streams})

    return existing_log_streams


def _get_existing_log_group_names() -> set:
    existing_group_names = set()
    next_token = None
    while True:
        if next_token:
            resp = logs_client.describe_log_groups(nextToken=next_token, limit=50)
        else:
            resp = logs_client.describe_log_groups(limit=50)

        for log_group in resp['logGroups']:
            name = log_group['logGroupName']
            parts = name.split('/')

            if len(parts) != 3:
                continue

            # Get only ecs prefixed log groups.
            # TODO: Make this configurable.
            if parts[1] != 'ecs':
                continue

            existing_group_names.add(name)

        next_token = resp.get('nextToken')

        # All log groups are loaded.
        if not next_token:
            break

    return existing_group_names
