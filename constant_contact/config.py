"""Application configuration"""
import os
import logging
import logging.handlers

# environment variables and their defaults if not defined
ENV_VARS = {
    'DEBUG': False,
    'APP_LOG_FILE': '/opt/python/log/application.log',
    'APP_LOG_FILESIZE': 10000000,
    'APP_LOG_NUM_BACKUPS': 20,
    'APP_QUEUE_MIN_RETRIES': 3,
    'APP_QUEUE_MAX_RETRIES': 15,
    'APP_QUEUE_RETRY_DELAY': 1,
    'APP_CC_CALL_MAX_RETRIES': 3,
    'APP_CC_CALL_RETRY_SLEEP_FACTOR': .5,
    'JANRAIN_URI': '',
    'JANRAIN_CLIENT_ID': '',
    'JANRAIN_CLIENT_SECRET': '',
    'JANRAIN_SCHEMA_NAME': 'user',
    'JANRAIN_BATCH_SIZE': 1000,
    'JANRAIN_CC_ATTRIBUTE_MAPPING': '',
    'JANRAIN_CC_CUSTOM_FIELD_MAPPING': '',
    'JANRAIN_CCID_ATTR_NAME': 'customContactId',
    'AWS_SQS_QUEUE_NAME': '',
    'AWS_REGION': '',
    'AWS_SDB_DOMAIN_NAME': '',
    'AWS_SDB_ITEM_NAME': '',
    'CC_API_KEY': '',
    'CC_ACCESS_TOKEN': '',
    'CC_LIST_IDS': ''
}

def get_config():
    config = {}
    for key, default_value in ENV_VARS.items():
        value = os.getenv(key, default_value)
        # empty string means use default value
        if value is '':
            value = default_value
        if isinstance(ENV_VARS[key], bool) and not isinstance(value, bool):
            if value.upper() != 'FALSE':
                value = True
            else:
                value = False
        elif isinstance(ENV_VARS[key], int) and not isinstance(value, int):
            try:
                value = int(value)
            except ValueError:
                value = 0
        config[key] = value
    return config

def setup_logging(app):
    handler = logging.handlers.RotatingFileHandler(
        app.config['APP_LOG_FILE'],
        backupCount=app.config['APP_LOG_NUM_BACKUPS'],
        maxBytes=app.config['APP_LOG_FILESIZE'])
    handler.setLevel(logging.INFO)
    formatter = logging.Formatter('[%(asctime)s] %(levelname)s: %(message)s')
    handler.setFormatter(formatter)
    app.logger.addHandler(handler)
    app.logger.setLevel(logging.DEBUG)

def apply_configuration(app):
    app.config.update(get_config())
    setup_logging(app)
