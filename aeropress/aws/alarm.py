from typing import List, Dict, Any  # noqa

import boto3

from aeropress.aws.scale import get_existing_scaling_policies
from aeropress import logger
from aeropress import AeropressException

cloudwatch_client = boto3.client('cloudwatch')


def create_scaling_alarms(services: list, clean_stale: bool) -> None:
    # Filter out those services which does not have alarm attribute.
    filtered_services = [service_dict for service_dict in services if service_dict.get('alarm')]

    # Are all alarm names valid?
    _validate_alarm_names(filtered_services)

    # Clean stale alarms
    if clean_stale:
        existing_alarms = _get_existing_alarms()
        _clean_stale_alarms(existing_alarms, filtered_services)

    # Create or update alarms.
    existing_policies = get_existing_scaling_policies()
    _create_or_update_alarms(filtered_services, existing_policies)


def _create_or_update_alarms(services: list, existing_policies: list) -> None:
    # Create or update alarms.
    for service_dict in services:
        resource_id = 'service/' + service_dict['cluster'] + '/' + service_dict['serviceName']
        _create_or_update_metric_alarm(service_dict['alarm'], resource_id, existing_policies, service_dict['metric'])


# TODO: should be configurable
def _clean_stale_alarms(existing_alarms: list, services: list) -> None:
    # Extract alarm name definitions to a list.
    defined_alarm_names = [sd['alarm']['AlarmName'] for sd in services]

    # If an alarm exist on Cloudwatch but is not defined now, it is an stale alarm, anymore.
    stale_alarm_names = [ea['AlarmName'] for ea in existing_alarms if ea['AlarmName'] not in defined_alarm_names]

    # Batch delete for stale alarms.
    if stale_alarm_names:
        logger.info('Deleting stale alarms: %s', stale_alarm_names)
        response = cloudwatch_client.delete_alarms(AlarmNames=stale_alarm_names)
        logger.debug('Deleted stale alarms details: %s', response)


def _validate_alarm_names(services: list) -> None:
    """
    All ecs alarm names must start with 'ecs:' prefix.
    """
    for service_dict in services:
        # TODO: Should be configurable
        if service_dict['alarm']['AlarmName'].startswith('ecs:'):
            continue

        logger.error("Alarm names must start with 'ecs:' prefix %s" % service_dict)
        raise AeropressException()


def _get_existing_alarms() -> list:
    """
    Get existing ecs alarms. ecs alarms start with 'ecs:' prefix.
    """
    existing_alarms = []  # type: List[Dict[Any, Any]]
    next_token = ''
    while True:
        if next_token:
            resp = cloudwatch_client.describe_alarms(NextToken=next_token)
        else:
            resp = cloudwatch_client.describe_alarms()

        for alarm in resp['MetricAlarms']:
            # TODO: Should be configurable
            if not alarm['AlarmName'].startswith('ecs:'):
                continue

            existing_alarms.append(alarm)

        next_token = resp.get('NextToken')

        # All alarms are loaded.
        if not next_token:
            break

    return existing_alarms


def _create_or_update_metric_alarm(alarm_dict: dict,
                                   resource_id: str,
                                   existing_policies: list,
                                   metric_dict: dict) -> None:
    # We should have alredy defined OK and Alarm action policies, before.
    ok_actions = []
    for policy_name in alarm_dict['OKActions']:
        ok_actions.append(_find_policy_arn(policy_name, resource_id, existing_policies))

    alarm_actions = []
    for policy_name in alarm_dict['AlarmActions']:
        alarm_actions.append(_find_policy_arn(policy_name, resource_id, existing_policies))

    # put_metric_alarm creates or updates an alarm and associates it with the specified metric.
    response = cloudwatch_client.put_metric_alarm(
        AlarmName=alarm_dict['AlarmName'],
        AlarmDescription=alarm_dict['AlarmDescription'],
        ActionsEnabled=True,
        OKActions=ok_actions,
        AlarmActions=alarm_actions,
        EvaluationPeriods=alarm_dict['EvaluationPeriods'],
        DatapointsToAlarm=alarm_dict['DataPointsToAlarm'],
        Threshold=alarm_dict['Threshold'],
        ComparisonOperator=alarm_dict['ComparisonOperator'],
        TreatMissingData=alarm_dict['TreatMissingData'],
        Namespace=metric_dict['MetricStat']['Metric']['Namespace'],
        MetricName=metric_dict['MetricStat']['Metric']['MetricName'],
        Period=metric_dict['MetricStat']['Period'],
        Unit=metric_dict['MetricStat']['Unit'],
        Statistic=metric_dict['MetricStat']['Stat'],
    )

    return response


def _find_policy_arn(policy_name: str, resource_id: str, existing_policies: list) -> str:
    for existing_policy in existing_policies:
        if existing_policy['PolicyName'] == policy_name and existing_policy['ResourceId'] == resource_id:
            return existing_policy['PolicyARN']

    # TODO: Raise exception
    return ''
