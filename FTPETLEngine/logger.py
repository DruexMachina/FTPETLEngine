"""Initialize logging defaults"""

import logging
import logging.handlers
from logging.config import dictConfig


dictConfig(
    {
        'version': 1,
        'disable_existing_loggers': False,
    })
logger = logging.getLogger(__package__)
default_formatter = logging.Formatter((
    "[%(asctime)s] [%(levelname)s] [%(name)s] [%(funcName)s():%(lineno)s] "
    "[PID:%(process)d TID:%(thread)d] %(message)s"), "%d/%m/%Y %H:%M:%S")
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.DEBUG)
console_handler.setFormatter(default_formatter)
logger.setLevel(logging.DEBUG)
logger.addHandler(console_handler)
