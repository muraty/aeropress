import os
import yaml
import logging
import argparse
from pathlib import Path
from typing import List, Dict, Any  # noqa

from aeropress import logger
from aeropress import AeropressException
from aeropress.aws import task, service
from aeropress._version import __version__


def main() -> None:
    parser = argparse.ArgumentParser(description='aeropress AWS ECS deployment helper')
    parser.add_argument('--path',
                        type=str,
                        help='Config path that includes service definitions.')
    parser.add_argument('--image-url',
                        type=str,
                        help='Image URL for docker image.')
    parser.add_argument('--clean-stale-tasks',
                        action='store_true',
                        help='Cleans all stale tasks and leave only active revisions.')
    parser.add_argument('--service-name',
                        type=str,
                        default='all',
                        help='Service name that will be updated. If not present, all services will be updated')
    parser.add_argument('--logging-level',
                        default='info',
                        choices=['debug', 'info', 'warning', 'error'],
                        type=str.lower,
                        help='Print debug logs')
    parser.add_argument('--version',
                        action='version',
                        version='{version}'.format(version=__version__))

    args = parser.parse_args()

    # TODO:
    # region_name param
    # Sub commands: deploy, clean

    # Setup logger
    setup_logging(args.logging_level)

    # Clean stale tasks and exit.
    if args.clean_stale_tasks:
        task.clean_stale_tasks()
        return

    # Create config dict, first.
    config_path = Path(args.path)
    services = _load_config(config_path, args.image_url)

    # Validate definitions
    if not _is_valid_config(services):
        logger.error('Config is not valid!')
        raise AeropressException()

    logger.info("Deploying the image '%s' from path: %s", args.image_url, args.path)
    deploy(services, args.service_name)


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
