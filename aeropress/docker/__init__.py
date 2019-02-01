import base64
from typing import Dict, Any  # noqa

import docker
import boto3

from aeropress import logger

ecr_client = boto3.client('ecr', region_name='eu-west-1')


# TODO: More detailed logging.
def build_image(build_path: str, dockerfile_path: str, build_args_list: list, local_tag: str) -> None:
    """
    path: Path to the directory containing the Dockerfile
    build_args:  A dictionary of build arguments
    tag: A tag to add to the final image
    dockerfile: path within the build context to the Dockerfile
    """
    logger.info('Building docker image..')

    build_args_dict = {}  # type: Dict[str, Any]
    for build_arg in build_args_list:
        build_args_dict[build_arg[0]] = build_arg[1]

    docker_client = docker.from_env()
    image, build_logs = docker_client.images.build(path=build_path,
                                                   dockerfile=dockerfile_path,
                                                   buildargs=build_args_dict,
                                                   tag=local_tag)


def push_image_to_ecr(local_tag: str, remote_tag: str) -> None:
    resp = ecr_client.get_authorization_token()
    password = (base64.b64decode(resp['authorizationData'][0]['authorizationToken'])).decode('utf-8').split(':')[-1]

    repository, remote_tag = remote_tag.split(':')
    # First, login to ECR.
    docker_client = docker.from_env()
    registry = 'https://' + repository
    logger.info('Logging to %s ', registry)
    resp = docker_client.login('AWS', password, registry=registry, reauth=True, email=None)
    logger.info('Login result: %s', resp)

    # Push image.
    image = docker_client.images.get(local_tag)
    image.tag(remote_tag)
    resp = docker_client.images.push(repository, tag=remote_tag)
    logger.info('Push result: %s', resp)
