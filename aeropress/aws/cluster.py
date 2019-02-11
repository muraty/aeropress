import boto3

from aeropress import logger

ecs_client = boto3.client('ecs')


def create(cluster_name: str) -> None:
    logger.info('Creating cluster %s', cluster_name)
    response = ecs_client.create_cluster(clusterName=cluster_name)
    logger.debug('Created cluster details: %s', response)


def describe(cluster: str) -> list:
    return ecs_client.describe_clusters(clusters=[cluster])
