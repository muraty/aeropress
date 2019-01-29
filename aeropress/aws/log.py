import boto3

from aeropress import logger

logs_client = boto3.client('logs', region_name='eu-west-1')


def create_missing_log_groups(missing_log_group_names: set) -> None:
    for missing_log_group_name in missing_log_group_names:
        logger.info('Creating log group: %s', missing_log_group_name)
        response = logs_client.create_log_group(logGroupName=missing_log_group_name)
        logger.debug('Created log group details: %s', response)


def clean_stale_log_groups(stale_log_group_names: set) -> None:
    for stale_log_group_name in stale_log_group_names:
        logger.info('Cleaning stale log group: %s', stale_log_group_name)
        response = logs_client.delete_log_group(logGroupName=stale_log_group_name)
        logger.debug('Clean stale log group details: %s', response)


def get_existing_log_group_names() -> set:
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
