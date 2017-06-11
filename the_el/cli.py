import json
import csv
import sys
import os
import re
import codecs

import click
from sqlalchemy import create_engine
from jsontableschema_sql import Storage
from smart_open import smart_open
import boto3
import boto
from boto.s3.key import Key

from .postgres import copy_from, copy_to
from . import carto 

@click.group()
def main():
    pass

def get_connection_string(connection_string):
    connection_string = os.getenv('CONNECTION_STRING', connection_string)
    if connection_string == None:
        raise Exception('`CONNECTION_STRING` environment variable or `--connection-string` option required')
    return connection_string

def create_storage_adaptor(connection_string, db_schema, geometry_support, from_srid=None, to_srid=None):
    engine = create_engine(connection_string)
    storage = Storage(engine, dbschema=db_schema, geometry_support=geometry_support, from_srid=from_srid, to_srid=to_srid, views=True)
    return engine, storage

s3_regex = r'^s3://([^/]+)/(.+)'

def fopen(file, mode='r'):
    if file == None:
        if mode == 'r':
            return sys.stdin
        elif mode == 'w':
            return sys.stdout
    else:
        # HACK: get boto working with instance credentials via boto3
        match = re.match(s3_regex, file)
        if match != None:
            client = boto3.client('s3')
            s3_connection = boto.connect_s3(
                aws_access_key_id=client._request_signer._credentials.access_key,
                aws_secret_access_key=client._request_signer._credentials.secret_key,
                security_token=client._request_signer._credentials.token)
            bucket = s3_connection.get_bucket(match.groups()[0])
            if mode == 'w':
                file = bucket.get_key(match.groups()[1], validate=False)
            else:
                file = bucket.get_key(match.groups()[1])
        return smart_open(file, mode=mode)

def get_table_schema(table_schema_path):
    with fopen(table_schema_path) as file:
        contents = file.read()
        if not isinstance(contents, str):
             contents = contents.decode('utf-8')
        return json.loads(contents)

@main.command()
@click.argument('table_name')
@click.option('--connection-string')
@click.option('-o','--output-file')
@click.option('--db-schema')
@click.option('--geometry-support')
def describe_table(table_name, connection_string, output_file, db_schema, geometry_support):
    connection_string = get_connection_string(connection_string)

    engine, storage = create_storage_adaptor(connection_string, db_schema, geometry_support)
    descriptor = storage.describe(table_name)

    with fopen(output_file, 'w') as file:
        json.dump(descriptor, file)

@main.command()
@click.argument('table_name')
@click.argument('table_schema_path')
@click.option('--connection-string')
@click.option('--db-schema')
@click.option('--indexes-fields')
@click.option('--geometry-support')
def create_table(table_name, table_schema_path, connection_string, db_schema, indexes_fields, geometry_support):
    table_schema = get_table_schema(table_schema_path)

    if re.match(carto.carto_connection_string_regex, connection_string) != None:
        load_postgis = geometry_support == 'postgis'
        return carto.create_table(table_name, load_postgis, table_schema, connection_string)

    connection_string = get_connection_string(connection_string)

    engine, storage = create_storage_adaptor(connection_string, db_schema, geometry_support)

    if indexes_fields != None:
        indexes_fields = indexes_fields.split(',')

    storage.create(table_name, table_schema, indexes_fields=indexes_fields)

@main.command()
@click.argument('table_name')
@click.option('--table-schema-path')
@click.option('--connection-string')
@click.option('-f','--input-file')
@click.option('--db-schema')
@click.option('--geometry-support')
@click.option('--skip-headers', is_flag=True)
def write(table_name,
          table_schema_path,
          connection_string,
          input_file,
          db_schema,
          geometry_support,
          skip_headers):
    table_schema = get_table_schema(table_schema_path)

    ## TODO: csv settings? use Frictionless Data csv standard?
    ## TODO: support line delimted json?
    with fopen(input_file) as file:
        if re.match(s3_regex, input_file) != None:
            rows = csv.reader(codecs.iterdecode(file, 'utf-8'))
        else:
            rows = csv.reader(file)

        if skip_headers:
            next(rows)

        if re.match(carto.carto_connection_string_regex, connection_string) != None:
            load_postgis = geometry_support == 'postgis'
            carto.load(db_schema, table_name, load_postgis, table_schema, connection_string, rows)
        else:
            connection_string = get_connection_string(connection_string)

            engine, storage = create_storage_adaptor(connection_string, db_schema, geometry_support)

            if table_schema_path != None:
                table_schema = get_table_schema(table_schema_path)
                storage.describe(table_name, descriptor=table_schema)

                if geometry_support == None and engine.dialect.driver == 'psycopg2':
                    copy_from(engine, table_name, table_schema, rows)
                else:
                    storage.write(table_name, rows)

@main.command()
@click.argument('table_name')
@click.option('--connection-string')
@click.option('-o','--output-file')
@click.option('--db-schema')
@click.option('--geometry-support')
@click.option('--from-srid')
@click.option('--to-srid')
def read(table_name, connection_string, output_file, db_schema, geometry_support, from_srid, to_srid):
    connection_string = get_connection_string(connection_string)

    engine, storage = create_storage_adaptor(connection_string, db_schema, geometry_support, from_srid=from_srid, to_srid=to_srid)

    ## TODO: csv settings? use Frictionless Data csv standard?
    ## TODO: support line delimted json?
    with fopen(output_file, mode='w') as file:
        writer = csv.writer(file)

        descriptor = storage.describe(table_name)
        fields = map(lambda x: x['name'], descriptor['fields'])
        writer.writerow(fields)

        if geometry_support == None and engine.dialect.driver == 'psycopg2':
            copy_to(engine, table_name, file)
        else:
            for row in storage.iter(table_name):
                row_out = []
                for field in row:
                    if isinstance(field, dict) or isinstance(field, list):
                        field = json.dumps(field)
                    row_out.append(field)
                writer.writerow(row_out)

@main.command()
@click.argument('new_table_name')
@click.argument('old_table_name')
@click.option('--connection-string')
@click.option('--db-schema')
@click.option('--select-users', help='Users to grant SELECT on updated table')
def swap_table(new_table_name, old_table_name, connection_string, db_schema, select_users):
    if re.match(carto.carto_connection_string_regex, connection_string) != None:
        if select_users != None:
            select_users = select_users.split(',')
        else:
            select_users = []
        return carto.swap_table(db_schema, new_table_name, old_table_name, select_users, connection_string)

    connection_string = get_connection_string(connection_string)
    engine = create_engine(connection_string)
 
    if engine.dialect.driver == 'psycopg2':
        conn = engine.raw_connection()
        try:
            with conn.cursor() as cur:
                sql = 'ALTER TABLE "{}" RENAME TO "{}_old";'.format(old_table_name, old_table_name) +\
                      'ALTER TABLE "{}" RENAME TO "{}";'.format(new_table_name, old_table_name) +\
                      'DROP TABLE "{}_old";'.format(old_table_name)
                cur.execute(sql)
            conn.commit()
        except:
            conn.rollback()
            raise
        conn.close()
    else:
        raise Exception('`{}` not supported by swap_table'.format(engine.dialect.driver))
