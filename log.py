import logging
import logging.config
import yaml


with open('logging.yaml', 'r') as f:
    log_cfg = yaml.safe_load(f.read())

logging.config.dictConfig(log_cfg)
logger = logging.getLogger('dev')
logger.setLevel(logging.INFO)
