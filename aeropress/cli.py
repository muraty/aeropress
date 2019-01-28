import os
import yaml
import logging
import argparse
from collections import defaultdict
from pathlib import Path
from typing import List, Dict, Any  # noqa

from aeropress import logger
from aeropress.aws import task, service
from aeropress._version import __version__


def main() -> None:
    parser = argparse.ArgumentParser(description='aeropress AWS ECS deployment helper')
    parser.add_argument('path', type=str, help='Config path that includes service definitions.')
    parser.add_argument('image_url', type=str, help='Image URL for docker image.')
    parser.add_argument('--logging-level',
                        default='info',
                        choices=['debug', 'info', 'warning', 'error'],
                        type=str.lower,
                        help='Print debug logs')
    parser.add_argument('--version', action='version',
                        version='{version}'.format(version=__version__))

    args = parser.parse_args()

    # TODO:
    # Add param for clean stale tasks.
    # region_name param
    # Clean stale tasks param

    # Setup logger
    setup_logging(args.logging_level)

    # Create config dict, first.
    config_path = Path(args.path)
    config_dict = _load_config(config_path, args.image_url)

    # Validate definitions
    if not _is_valid_config(config_dict):
        logger.error('Config is not valid!')
        os._exit(1)

    logger.info("Deploying the image '%s' from path: %s", args.image_url, args.path)
    deploy(config_dict)


def _load_config(root_path: Path, image_url: str) -> Dict:
    logger.info('Reading yaml config files from %s', root_path)

    config = defaultdict(list)  # type: Dict[str, List[Dict[str, Any]]]

    # Reading yaml files into a dictionary.
    parent_map = {
        'task': 'tasks',
        'service': 'services',
        'cluster': 'clusters',
    }
    for root, dirs, files in os.walk(root_path.as_posix()):
        for name in files:
            path = Path(os.path.join(root, name))
            with open(path.as_posix()) as f:
                _yaml_dict = yaml.load(f.read())

            for key, data in _yaml_dict.items():
                parent_key = parent_map[key]

                # Inject image-url into container definitions.
                if key == 'task':
                    for container_definition in data['containerDefinitions']:
                        # TODO: Check default. If there is an image-url already, do not inject this!
                        container_definition['image'] = image_url

                config[parent_key].append(data)

    return config


# TODO: Add more check.
def _is_valid_config(config: dict) -> bool:
    if not config['services']:
        logger.error('No service definition is found!')
        return False

    task_definitions = [task_dict['family'] for task_dict in config['tasks']]

    for service_dict in config['services']:
        if service_dict['taskDefinition'] not in task_definitions:
            logger.error('Task definition %s is not found!', service_dict['taskDefinition'])
            return False

    return True


def deploy(config_dict: dict) -> None:
    # Register task definitions.
    task.register_all(config_dict['tasks'])

    # Update all services (Create if not exists.)
    service.update_all(config_dict['services'])


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
