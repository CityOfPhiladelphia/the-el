import json
import csv
import sys
import os
import re
import logging
from logging.config import dictConfig

import click
import dsnparse

csv.field_size_limit(sys.maxsize)

def get_logger(logging_config):
    try:
        with open(logging_config) as file:
            config = yaml.load(file)
        dictConfig(config)
    except:
        FORMAT = '[%(asctime)-15s] %(levelname)s [%(name)s] %(message)s'
        logging.basicConfig(format=FORMAT, level=logging.INFO, stream=sys.stderr)

    logger = logging.getLogger('the-el')

    def exception_handler(type, value, tb):
        logger.exception("Uncaught exception: {}".format(str(value)), exc_info=(type, value, tb))

    sys.excepthook = exception_handler

    return logger

@click.group()
def main():
    pass

@main.command()
@click.argument('connection-string')
@click.argument('table')
def extract(connection_string, table):
    connection_config = dsnparse.parse(connection_string)

    if connection_config.scheme == 'postgresql':
        from .adaptors.postgres import PostgresAdaptor
        with PostgresAdaptor(connection_config) as adaptor:
            adaptor.extract(table)
    else:
        raise Exception('Unsupported `{}`'.format(connection_config['scheme']))
