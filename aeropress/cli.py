import os
import yaml
import logging
import argparse
from pathlib import Path
from typing import List, Dict, Any  # noqa

from aeropress import logger
from aeropress import AeropressException
from aeropress.aws import task, service, log
from aeropress._version import __version__


def main() -> None:
    parser = argparse.ArgumentParser(description='aeropress AWS ECS deployment helper')
    subparsers = parser.add_subparsers(help='sub-command help', dest='subparser_name')

    parser_deploy = subparsers.add_parser('deploy',
                                          help='Deploy docker image to ECS.')
    parser_clean = subparsers.add_parser('clean',
                                         help='Clean commands for stale entitites on AWS.')
    parser_register = subparsers.add_parser('register',
                                            help='Register tasks on ECS.')

    # deploy subcommand
    parser_deploy.add_argument('--image-url',
                               type=str,
                               dest='deploy_image_url',
                               default=None,
                               help='Image URL for docker image.')
    parser_deploy.add_argument('--service-names',
                               nargs='+',
                               dest='deploy_service_names',
                               help='Service name that will be updated. If not present, all services will be updated')
    parser_deploy.add_argument('--path',
                               type=str,
                               dest='config_path',
                               help='Config path that includes service definitions.')

    # clean sub command
    parser_clean.add_argument('--stale-tasks',
                              action='store_true',
                              dest='clean_stale_tasks',
                              help='Cleans all stale tasks and leave only active revisions.')
    parser_clean.add_argument('--stale-log-streams',
                              action='store_true',
                              dest='clean_stale_log_streams',
                              help='Cleans all stale log streams and leave only active revisions.')
    parser_clean.add_argument('--days-ago',
                              type=int,
                              dest='log_stream_days_ago',
                              help='Timedelta for deleting stale log streams.')
    parser_clean.add_argument('--path',
                              type=str,
                              dest='config_path',
                              help='Config path that includes service & task definitions.')

    # register sub command
    parser_register.add_argument('--task-definition',
                                 type=str,
                                 dest='task_definition',
                                 help='Task definition that will be registered.')
    parser_register.add_argument('--path',
                                 type=str,
                                 dest='config_path',
                                 help='Config path that includes service & task definitions.')
    parser_register.add_argument('--image-url',
                                 type=str,
                                 dest='image_url',
                                 default=None,
                                 help='Image URL for docker image.')
    parser_register.add_argument('--entrypoint',
                                 type=str,
                                 dest='entrypoint',
                                 default=[],
                                 nargs='+',
                                 help='Container entrypoint. Must be list of strings.')

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

    # Setup logger
    setup_logging(args.logging_level)

    # Create config dict, first.
    config_path = Path(args.config_path)

    # Clean stale tasks and exit.
    if args.subparser_name == 'clean':
        if args.clean_stale_tasks:
            logger.info('Cleaning stale tasks...')
            task.clean_stale_tasks()
            return

        if args.clean_stale_log_streams:
            services = _load_config(config_path)
            logger.info('Cleaning stale log streams from starting %s day(s) ago...', args.log_stream_days_ago)
            log.clean_stale_log_streams(services, args.log_stream_days_ago)
            return

    if args.subparser_name == 'deploy':
        services = _load_config(config_path, args.deploy_image_url)
        logger.info("Deploying the image '%s' from path: %s", args.deploy_image_url, args.config_path)
        deploy(services, args.deploy_service_names)
        return

    if args.subparser_name == 'register':
        services = _load_config(config_path, args.image_url, args.entrypoint)

        task_dict = None
        for service_dict in services:
            if args.task_definition == service_dict['task']['family']:
                task_dict = service_dict['task']
                break

        if not task_dict:
            logger.error("Could not find task definition '%s' on '%s'", args.task_definition, args.config_path)
            return

        logger.info("Registering task definition '%s' fom path: %s", args.task_definition, args.config_path)
        task.register_all([task_dict], False)
        return


def _load_config(root_path: Path, image_url: str = None, entrypoint: list = []) -> list:
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

                    # Override defaults for container definitions.
                    for container_definition in service_v['containerDefinitions']:
                        if image_url:
                            container_definition['image'] = image_url

                        if entrypoint:
                            container_definition['entryPoint'] = entrypoint

                services.append(value)

    # Validate definitions
    if not _is_valid_config(services):
        logger.error('Config is not valid!')
        raise AeropressException()

    return services


# TODO: Add more check.
def _is_valid_config(services: list) -> bool:
    if not services:
        logger.error('No service definition is found!')
        return False

    for service_dict in services:
        # We might run only one task with run-task.
        if not service_dict.get('taskDefinition'):
            continue

        if service_dict['taskDefinition'] != service_dict['task']['family']:
            logger.error('Task definition is not found for service %s!', service_dict['serviceName'])
            return False

    return True


def _find_service(service_name: str, services: list) -> Dict:
    for service_dict in services:
        if not service_dict.get('serviceName'):
            continue

        if service_dict['serviceName'] == service_name:
            return service_dict.copy()

    return {}


def deploy(services: list, service_names: list) -> None:
    selected_services = []  # type: List[Dict[str, Any]]
    for service_name in service_names:
        service_dict = _find_service(service_name, services)

        if not service_dict:
            logger.error("Service '%s' is not found! ", service_name)
            raise AeropressException()

        selected_services.append(service_dict)

    # Register task definitions.
    tasks = [selected_service['task'] for selected_service in selected_services]
    task.register_all(tasks)

    # Update or create all services. (We might have tasks without services, eliminating them..)
    filtered_selected_services = [sd for sd in selected_services if sd.get('serviceName')]
    service.update_all(filtered_selected_services)


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
