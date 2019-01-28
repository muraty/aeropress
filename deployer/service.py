import boto3

from deployer.metric import create_metrics
from deployer.alarm import create_scaling_alarms
from deployer.scale import create_scaling_policies, register_scalable_targets
from deployer import logger

ecs_client = boto3.client('ecs', region_name='eu-west-1')


def update_all(services: list) -> None:
    # TODO: Add cluster check.

    # Create missing services
    _create_missing_services(services)

    # Register service to be able to create scaling policies.
    register_scalable_targets(services)

    # Create missing scaling policies.
    create_scaling_policies(services)

    # Create missing metrics via Cloudwatch for policies
    create_metrics(services)

    # Create missing scaling alarms
    create_scaling_alarms(services)

    # Update all services with force-new-deployament flag.
    _update_services(services)


def _get_existing_services(services: list) -> list:
    cluster_names = set([service_dict['cluster'] for service_dict in services])

    # Retrieve all servives for all clusters.
    existing_services = []
    for cluster_name in cluster_names:
        next_token = None
        while True:
            if next_token:
                resp = ecs_client.list_services(cluster=cluster_name, maxResults=100, nextToken=next_token)
            else:
                resp = ecs_client.list_services(cluster=cluster_name, maxResults=100)
            service_arns = resp['serviceArns']
            # Put only service name, not full arn.
            # Example service_arn: arn:aws:ecs:eu-west-1:0000000000:service/service-foo
            existing_services.extend([service_arn.split('/')[-1] for service_arn in service_arns])

            next_token = resp.get('nextToken')

            # All services are loaded.
            if not resp.get('nextToken'):
                break

    return existing_services


def _create_missing_services(services: list) -> None:
    # Get existing services
    existing_services = _get_existing_services(services)

    # Find missing services.
    missing_services = [s for s in services if s['serviceName'] not in existing_services]

    # Create missing services.
    for service_dict in missing_services:
        response = ecs_client.create_service(
            cluster=service_dict['cluster'],
            serviceName=service_dict['serviceName'],
            taskDefinition=service_dict['taskDefinition'],
            desiredCount=service_dict['desiredCount'],
            launchType=service_dict['launchType'],
            schedulingStrategy=service_dict['schedulingStrategy'],
            deploymentController=service_dict['deploymentController'],
        )
        logger.info('Created service: %s', service_dict['serviceName'])
        logger.debug('Created service details: %s', response)


def _update_services(services: list) -> None:
    for service_dict in services:
        # If a task revision is not specified, the latest ACTIVE revision is used.
        response = ecs_client.update_service(
            cluster=service_dict['cluster'],
            service=service_dict['serviceName'],
            desiredCount=service_dict['desiredCount'],
            taskDefinition=service_dict['taskDefinition'],
            forceNewDeployment=True,
        )
        logger.info('Updated service: %s', service_dict['serviceName'])
        logger.debug('Updated service details: %s', response)
