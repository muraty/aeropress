from typing import List, Dict, Any  # noqa

import boto3

from aeropress import logger
from botocore.exceptions import ClientError

scaling_client = boto3.client('application-autoscaling')


def create_scaling_policies(services: list, clean_stale: bool) -> None:
    # Get already defined scaling policies
    existing_policies = get_existing_scaling_policies()

    # Filter out those services which does not have a scale policy
    filtered_services = [service_dict for service_dict in services if service_dict.get('scale')]

    # Clean stale policies
    if clean_stale:
        _clean_stale_policies(filtered_services, existing_policies)

    # Create or update all policies
    _create_or_update_all_policies(filtered_services, existing_policies)


def get_existing_scaling_policies() -> list:
    # Retrieve all existing policies.
    existing_policies = []  # type: List[Dict[Any, Any]]
    next_token = None
    while True:
        if next_token:
            resp = scaling_client.describe_scaling_policies(ServiceNamespace='ecs', NextToken=next_token)
        else:
            resp = scaling_client.describe_scaling_policies(ServiceNamespace='ecs')

        existing_policies.extend(resp['ScalingPolicies'])
        next_token = resp.get('NextToken')

        # All policies are loaded.
        if not next_token:
            break

    return existing_policies


def _create_or_update_all_policies(services: list, existing_policies: list) -> None:
    for service_dict in services:
        resource_id = 'service/' + service_dict['cluster'] + '/' + service_dict['serviceName']

        if not service_dict.get('scale'):
            continue

        if not service_dict['scale'].get('policies'):
            continue

        # Create or update the policies.
        for policy_dict in service_dict['scale']['policies']:
            logger.info('Crating scaling policy: %s for %s', policy_dict['PolicyName'], resource_id)
            response = scaling_client.put_scaling_policy(
                    PolicyName=policy_dict['PolicyName'],
                    PolicyType=policy_dict['PolicyType'],
                    ServiceNamespace='ecs',
                    ResourceId=resource_id,
                    ScalableDimension=policy_dict['ScalableDimension'],
                    StepScalingPolicyConfiguration=policy_dict['StepScalingPolicyConfiguration'],
            )
            logger.debug('Created scaling policy details: %s', response)


def _is_stale_policy(existing_policy_dict: dict, services: list) -> bool:
    for service_dict in services:
        resource_id = 'service/' + service_dict['cluster'] + '/' + service_dict['serviceName']

        if not service_dict.get('scale'):
            continue

        if not service_dict['scale'].get('policies'):
            continue

        for defined_policy_dict in service_dict['scale']['policies']:
            if existing_policy_dict['PolicyName'] != defined_policy_dict['PolicyName']:
                continue

            if existing_policy_dict['ResourceId'] != resource_id:
                continue

            # PolicyName and ResourceId matches to one of defined policies.
            return False

    return True


def _clean_stale_policies(services: list, existing_policies: list) -> None:
    for existing_policy_dict in existing_policies:
        if not _is_stale_policy(existing_policy_dict, services):
            continue

        logger.info('Removing state policy: %s', existing_policy_dict['PolicyName'])
        response = scaling_client.delete_scaling_policy(
                PolicyName=existing_policy_dict['PolicyName'],
                ServiceNamespace='ecs',
                ResourceId=existing_policy_dict['ResourceId'],
                ScalableDimension='ecs:service:DesiredCount'
        )
        logger.debug('Removed stale policy details: %s', response)


def register_scalable_targets(services: list) -> None:
    for service_dict in services:
        resource_id = 'service/' + service_dict['cluster'] + '/' + service_dict['serviceName']

        if service_dict.get('scale'):
            _register_scalable_target(service_dict['scale'], resource_id)
        else:
            _deregister_scalable_target(resource_id)


def _register_scalable_target(scale_dict: dict, resource_id: str) -> None:
    logger.info('Registering service as a scalable target: %s', resource_id)
    response = scaling_client.register_scalable_target(
            ServiceNamespace='ecs',
            ResourceId=resource_id,
            ScalableDimension='ecs:service:DesiredCount',
            MinCapacity=scale_dict['MinCapacity'],
            MaxCapacity=scale_dict['MaxCapacity'],
    )
    logger.debug('Registered service as a scalable target details: %s', response)


def _deregister_scalable_target(resource_id: str) -> None:
    try:
        response = scaling_client.deregister_scalable_target(
                ServiceNamespace='ecs',
                ResourceId=resource_id,
                ScalableDimension='ecs:service:DesiredCount',
        )
        logger.info('Deregistered service as a scalable target: %s', resource_id)
        logger.debug('Service deregistration response: %s', response)
    except ClientError as e:
        if e.response['Error']['Code'] == 'ObjectNotFoundException':
            logger.debug('No need to deregister..')
