import base64
import gzip
import json
import logging
import os
from io import BytesIO

from python3.shipper.shipper import LogzioShipper

KEY_INDEX = 0
VALUE_INDEX = 1
LOG_LEVELS = ['alert', 'trace', 'debug', 'notice', 'info', 'warn',
              'warning', 'error', 'err', 'critical', 'crit', 'fatal',
              'severe', 'emerg', 'emergency']
              
LOG_LEVELS_IGNORE = ['info']

PYTHON_EVENT_SIZE = 3
LAMBDA_JS_EVENT_SIZE = 4
NODEJS_EVENT_SIZE = 5
LAMBDA_LOG_GROUP = '/aws/lambda/'


# set logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)


def _extract_aws_logs_data(event):
    # type: (dict) -> dict
    event_str = event['awslogs']['data']
    try:
        logs_data_decoded = base64.b64decode(event_str)
        logs_data_unzipped = gzip.GzipFile(fileobj=BytesIO(logs_data_decoded))
        logs_data_unzipped = logs_data_unzipped.read()
        logs_data_dict = json.loads(logs_data_unzipped)
        return logs_data_dict
    except ValueError as e:
        logger.error("Got exception while loading json, message: {}".format(e))
        raise ValueError("Exception: json loads")


def _extract_lambda_log_message(log):
    # type: (dict) -> None
    str_message = str(log['message'])
    start_split = 0
    message_parts = str_message[start_split:].split('\t')
    size = len(message_parts)
    if size == PYTHON_EVENT_SIZE or size == NODEJS_EVENT_SIZE or size ==LAMBDA_JS_EVENT_SIZE:
        log['@timestamp'] = message_parts[0]
        log['requestID'] = message_parts[1]
        log['message'] = message_parts[size - 1]
    if size == NODEJS_EVENT_SIZE or size ==LAMBDA_JS_EVENT_SIZE:
        log['log_level'] = message_parts[2].lower()


def _add_timestamp(log):
    # type: (dict) -> None
    if '@timestamp' not in log:
        log['@timestamp'] = str(log['timestamp'])
        del log['timestamp']

def _add_level(log):

    if 'level' not in log:
        message = log['message']
        if 'Task timed out after' in message:
            log['level'] = 'error'

def _parse_to_json(log):
    # type: (dict) -> None
    try:
        json_object = json.loads(log['message'])
        if os.environ['FORMAT'].lower() == 'json':
            for key, value in json_object.items():
                log[key] = value
        else: #extract level
            if 'level' in json_object: 
                log_level = json_object['level']
                if log_level.lower() in LOG_LEVELS:
                    log['log_level'] = log_level
    except (KeyError, ValueError) as e:
        pass


def _parse_cloudwatch_log(log, additional_data):
    # type: (dict, dict) -> bool
    _add_timestamp(log)
    _add_level(log)
    if LAMBDA_LOG_GROUP in additional_data['logGroup']:
        if _is_valid_log(log):
            _extract_lambda_log_message(log)
        else:
            return False
    log.update(additional_data)
    _parse_to_json(log)
    if 'log_level' in log and log['log_level'].lower() in LOG_LEVELS_IGNORE:
        return False
    if 'level' in log and log['level'].lower() in LOG_LEVELS_IGNORE:
        return False
    return True


def _get_additional_logs_data(aws_logs_data, context):
    # type: (dict, 'LambdaContext') -> dict
    additional_fields = ['logGroup', 'logStream', 'messageType', 'owner']
    additional_data = dict((key, aws_logs_data[key]) for key in additional_fields)
    try:
        additional_data['function_version'] = context.function_version
        additional_data['invoked_function_arn'] = context.invoked_function_arn
    except KeyError:
        logger.info('Failed to find context value. Continue without adding it to the log')

    try:
        # If ENRICH has value, add the properties
        if os.environ['ENRICH']:
            properties_to_enrich = os.environ['ENRICH'].split(";")
            for property_to_enrich in properties_to_enrich:
                property_key_value = property_to_enrich.split("=")
                additional_data[property_key_value[KEY_INDEX]] = property_key_value[VALUE_INDEX]
    except KeyError:
        pass

    try:
        additional_data['type'] = os.environ['TYPE']
    except KeyError:
        logger.info("Using default TYPE 'logzio_cloudwatch_lambda'.")
        additional_data['type'] = 'logzio_cloudwatch_lambda'
    return additional_data


def _is_valid_log(log):
    # type (dict) -> bool
    message = log['message']
    is_info_log = message.startswith('START') or message.startswith('END') or message.startswith('REPORT') or message.startswith('INIT_START')
    return not is_info_log

def is_simple_value(value):
    return isinstance(value, (str, int, float, bool))
    
def flatten_object(obj):
    flattened = {}
    for key, value in obj.items():
        if is_simple_value(value):
            flattened[key] = value
        else:
            if key == 'data':
                for k, v in value.items():
                    if is_simple_value(v):
                        flattened[k] = v
            flattened[key] = json.dumps(value)
    flattened['logVerstion'] = 'v3'
    return flattened

def lambda_handler(event, context):
    # type (dict, 'LambdaContext') -> None

    aws_logs_data = _extract_aws_logs_data(event)
    additional_data = _get_additional_logs_data(aws_logs_data, context)
    shipper = LogzioShipper()

    logger.info("About to send {} logs".format(len(aws_logs_data['logEvents'])))
    for log in aws_logs_data['logEvents']:
        if not isinstance(log, dict):
            raise TypeError("Expected log inside logEvents to be a dict but found another type")
        if _parse_cloudwatch_log(log, additional_data):

            shipper.add(flatten_object(log))

    shipper.flush()
