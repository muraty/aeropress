import os
import argparse
import logging
from pathlib import Path
from typing import List, Dict, Any  # noqa
from collections import defaultdict

import yaml

from deployer import service, task

logger = logging.getLogger(__name__)


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
    else:
        logger.info('No config file is found!')

    return config


def deploy(path: str, image_url: str) -> None:
    # Create Path object
    config_path = Path(path)

    config = _load_config(config_path, image_url)

    # Register task definitions.
    task.register_all(config['tasks'])

    # Update all services (Create if not exists.)
    service.update_all(config['services'])


def main() -> None:
    # TODO: Write descriptions.
    parser = argparse.ArgumentParser(description='ECS deployer')
    parser.add_argument('path', type=str)
    parser.add_argument('image_url', type=str)

    args = parser.parse_args()

    # TODO:
    # Add param for clean stale tasks.
    # region_name param
    # Clean stale tasks param

    deploy(args.path, args.image_url)


if __name__ == '__main__':
    main()
