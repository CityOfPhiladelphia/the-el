import json
import csv
import sys
import os

import click
from sqlalchemy import create_engine
from jsontableschema_sql import Storage
from smart_open import smart_open

@click.group()
def main():
    pass

def get_connection_string(connection_string):
    connection_string = os.getenv('CONNECTION_STRING', connection_string)
    if connection_string == None:
        raise Exception('`CONNECTION_STRING` environment variable or `--connection-string` option required')
    return connection_string

def create_storage_adaptor(connection_string, db_schema, geometry_support):
    engine = create_engine(connection_string)
    storage = Storage(engine, dbschema=db_schema, geometry_support=geometry_support)
    return storage

def fopen(file, mode='rb'):
    if file == None:
        if mode == 'rb':
            return sys.stdin
        elif mode == 'wb':
            return sys.stdout
    else:
        return smart_open(file, mode=mode)

@main.command()
@click.argument('table_name')
@click.option('--connection-string')
@click.option('-o','--output-file')
@click.option('--db-schema')
@click.option('--geometry-support')
def describe_table(table_name, connection_string, output_file, db_schema, geometry_support):
    connection_string = get_connection_string(connection_string)

    storage = create_storage_adaptor(connection_string, db_schema, geometry_support)
    descriptor = storage.describe(table_name)

    with fopen(output_file) as file:
        json.dump(descriptor, file)

@main.command()
@click.argument('table_name')
@click.argument('table_schema_path')
@click.option('--connection-string')
@click.option('--db-schema')
@click.option('--index-fields')
@click.option('--geometry-support')
def create_table(table_name, table_schema_path, connection_string, db_schema, index_fields, geometry_support):
    connection_string = get_connection_string(connection_string)

    storage = create_storage_adaptor(connection_string, db_schema, geometry_support)

    with fopen(table_schema_path) as file:
        table_schema = json.load(file)

    if index_fields != None:
        index_fields = index_fields.split(',')

    storage.create(table_name, table_schema, index_fields=index_fields)

@main.command()
@click.argument('table_name')
@click.option('--connection-string')
@click.option('-f','--input-file')
@click.option('--db-schema')
@click.option('--geometry-support')
def write(table_name, connection_string, input_file, db_schema, geometry_support):
    connection_string = get_connection_string(connection_string)

    storage = create_storage_adaptor(connection_string, db_schema, geometry_support)

    ## TODO: skip csv header?
    ## TODO: csv settings? use Frictionless Data csv standard?
    ## TODO: support line delimted json?
    with fopen(input_file) as file:
        rows = csv.reader(file)
        storage.write(table_name, rows, as_generator=True)

@main.command()
@click.argument('table_name')
@click.option('--connection-string')
@click.option('-o','--output-file')
@click.option('--db-schema')
@click.option('--geometry-support')
def read(table_name, connection_string, output_file, db_schema, geometry_support):
    connection_string = get_connection_string(connection_string)

    storage = create_storage_adaptor(connection_string, db_schema, geometry_support)

    ## TODO: skip csv header?
    ## TODO: csv settings? use Frictionless Data csv standard?
    ## TODO: support line delimted json?
    with fopen(input_file, mode='wb') as file:
        writer = csv.writer(file)
        for row in storage.iter(table_name):
            writer.writerow(row)
