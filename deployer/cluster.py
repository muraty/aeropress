import boto3

from deployer import logger

ecs_client = boto3.client('ecs', region_name='eu-west-1')


def create(cluster_name: str) -> None:
    response = ecs_client.create_cluster(
        clusterName=cluster_name,
    )

    logger.info('Created cluster %s', cluster_name)
    logger.debug('Created cluster details: %s', response)


def describe(cluster: str) -> list:
    return ecs_client.describe_clusters(
        clusters=[cluster],
    )
