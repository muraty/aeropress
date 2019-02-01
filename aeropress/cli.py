import os
import yaml
import logging
import argparse
from pathlib import Path
from typing import List, Dict, Any  # noqa

from aeropress import logger
from aeropress import AeropressException
from aeropress.aws import task, service
from aeropress import docker as aeropress_docker
from aeropress._version import __version__


def main() -> None:
    parser = argparse.ArgumentParser(description='aeropress AWS ECS deployment helper')
    subparsers = parser.add_subparsers(help='sub-command help', dest='subparser_name')

    parser_deploy = subparsers.add_parser('deploy',
                                          help='Deploy docker image to ECS.')
    parser_docker = subparsers.add_parser('docker',
                                          help='Docker commands.')
    parser_clean = subparsers.add_parser('clean',
                                         help='Clean commands for stale entitites on AWS.')

    # deploy subcommand
    parser_deploy.add_argument('--deploy',
                               dest='deploy_image',
                               action='store_true',
                               help='')
    parser_deploy.add_argument('--path',
                               type=str,
                               dest='deploy_config_path',
                               help='Config path that includes service definitions.')
    parser_deploy.add_argument('--image-url',
                               type=str,
                               dest='deploy_image_url',
                               help='Image URL for docker image.')
    parser_deploy.add_argument('--service-name',
                               type=str,
                               default='all',
                               dest='deploy_service_name',
                               help='Service name that will be updated. If not present, all services will be updated')

    # clean sub command
    parser_clean.add_argument('--stale-tasks',
                              action='store_true',
                              dest='clean_stale_tasks',
                              help='Cleans all stale tasks and leave only active revisions.')

    # docker sub command
    parser_docker.add_argument('--build-image',
                               action='store_true',
                               dest='docker_build_image',
                               help='Builds Docker image.')
    parser_docker.add_argument("--build-args",
                               action='append',
                               dest='docker_build_args',
                               type=lambda kv: kv.split("="),
                               help='Build arguments key-value pair list for building Docker image.')
    parser_docker.add_argument('--build-path',
                               type=str,
                               dest='docker_build_path',
                               help='Path to the directory containing the Dockerfile.')
    parser_docker.add_argument('--dockerfile-path',
                               type=str,
                               dest='docker_dockerfile_path',
                               help='path within the build context to the Dockerfile')
    parser_docker.add_argument('--image-tag',
                               type=str,
                               dest='docker_image_tag',
                               help='A tag to add to the final image.')
    parser_docker.add_argument('--push',
                               action='store_true',
                               dest='docker_push_image',
                               help='Push image url to given registry')
    parser_docker.add_argument('--local-tag',
                               type=str,
                               dest='docker_local_tag',
                               help='local tag for pushing image')
    parser_docker.add_argument('--remote-tag',
                               type=str,
                               dest='docker_remote_tag',
                               help='remote tag for pushing image')

    # Main command
    parser.add_argument('--logging-level',
                        default='info',
                        choices=['debug', 'info', 'warning', 'error'],
                        type=str.lower,
                        dest='logging_level',
                        help='Print debug logs')
    parser.add_argument('--version',
                        action='version',
                        dest='version',
                        version='{version}'.format(version=__version__))

    args = parser.parse_args()

    # TODO:
    # region_name param

    # Setup logger
    setup_logging(args.logging_level)

    # Clean stale tasks and exit.
    if args.subparser_name == 'clean':
        if args.clean_stale_tasks:
            task.clean_stale_tasks()
            return

    if args.subparser_name == 'docker':
        if args.docker_build_image:
            aeropress_docker.build_image(args.docker_build_path,
                                         args.docker_dockerfile_path,
                                         args.docker_build_args,
                                         args.docker_image_tag)
            return

        if args.docker_push_image:
            logger.info('Pushing image with remote tag: %s', args.docker_remote_tag)
            aeropress_docker.push_image_to_ecr(args.docker_local_tag, args.docker_remote_tag)
            return

    if args.subparser_name == 'deploy':
        if args.deploy_image:
            # Create config dict, first.
            config_path = Path(args.deploy_config_path)
            services = _load_config(config_path, args.deploy_image_url)

            # Validate definitions
            if not _is_valid_config(services):
                logger.error('Config is not valid!')
                raise AeropressException()

            logger.info("Deploying the image '%s' from path: %s", args.deploy_image_url, args.deploy_path)
            deploy(services, args.deploy_service_name)
            return


def _load_config(root_path: Path, image_url: str) -> list:
    logger.info('Reading yaml config files from %s', root_path)

    services = []  # type: List[Dict[str, Any]]

    # Reading yaml services definitions into a list of dictionary.
    for root, dirs, files in os.walk(root_path.as_posix()):
        for name in files:
            path = Path(os.path.join(root, name))
            with open(path.as_posix()) as f:
                _yaml_dict = yaml.load(f.read())

            for key, value in _yaml_dict.items():
                # Handle service defnitions.
                for service_k, service_v in value.items():
                    if service_k != 'task':
                        continue

                    # Inject image-url into container definitions.
                    for container_definition in service_v['containerDefinitions']:
                        # TODO: Check default. If there is an image-url already, do not inject this!
                        container_definition['image'] = image_url

                services.append(value)

    return services


# TODO: Add more check.
def _is_valid_config(services: list) -> bool:
    if not services:
        logger.error('No service definition is found!')
        return False

    for service_dict in services:
        if service_dict['taskDefinition'] != service_dict['task']['family']:
            logger.error('Task definition is not found for service %s!', service_dict['serviceName'])
            return False

    return True


def deploy(services: list, service_name: str) -> None:
    if service_name == 'all':
        clean_stale = True
        tasks = [service_dict['task'] for service_dict in services]
    else:
        clean_stale = False
        selected_service = {}  # type: Dict[str, Any]
        for service_dict in services:
            if service_dict['serviceName'] == service_name:
                selected_service = service_dict.copy()
                break

        if not selected_service:
            service_names = '\n'.join([service_dict['serviceName'] for service_dict in services])
            logger.error("Given service %s is not found! Valid service names: %s ", service_name, service_names)
            raise AeropressException()

        tasks = [selected_service['task']]
        services = [selected_service]

    # Register task definitions.
    task.register_all(tasks, clean_stale)

    # Update all services (Create if not exists.)
    service.update_all(services, clean_stale)


def setup_logging(level: str) -> None:
    FORMAT = "[%(asctime)s %(levelname)s %(name)s - %(message)s"
    level = getattr(logging, level.upper())
    h = logging.StreamHandler()
    h.setLevel(level)
    h.setFormatter(logging.Formatter(FORMAT))
    logger.setLevel(level)
    logger.addHandler(h)


if __name__ == '__main__':
    main()
