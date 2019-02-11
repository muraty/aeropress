from datetime import datetime

import boto3

from aeropress import logger

cloudwatch_client = boto3.client('cloudwatch')


def create_metrics(services: list) -> None:
    """
    Cloudwatch does not let us to delete metrics. That's why we only create missing metrics.
    """
    # Get existing metrics
    existing_metrics = _get_existing_metrics()

    # Find missing metrics
    defined_metrics = [service_dict['metric'] for service_dict in services if service_dict.get('metric')]
    missing_metrics = [sm for sm in defined_metrics if sm['MetricStat']['Metric']['MetricName'] not in existing_metrics]

    _create_missing_metrics(missing_metrics)


def _get_existing_metrics() -> list:
    existing_metrics = []
    next_token = None
    while True:
        if next_token:
            resp = cloudwatch_client.list_metrics(NextToken=next_token)
        else:
            resp = cloudwatch_client.list_metrics()

        existing_metrics.extend([m['MetricName'] for m in resp['Metrics']])
        next_token = resp.get('NextToken')

        # All metrics are loaded.
        if not next_token:
            break

    return existing_metrics


def _create_missing_metrics(missing_metrics: list) -> None:
    for missing_metric in missing_metrics:
        logger.info('Creating metric: %s', missing_metric['MetricStat']['Metric']['MetricName'])
        response = cloudwatch_client.put_metric_data(
            Namespace=missing_metric['MetricStat']['Metric']['Namespace'],
            MetricData=[
                {
                    'MetricName': missing_metric['MetricStat']['Metric']['MetricName'],
                    'Timestamp': datetime.utcnow(),
                    'Value': 0,
                    'Unit': missing_metric['MetricStat']['Unit']
                },
            ]
        )
        logger.debug('Created metric details: %s', response)
