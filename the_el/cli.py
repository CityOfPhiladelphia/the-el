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

from . import postgres
from . import carto

csv.field_size_limit(sys.maxsize)

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

def fopen(file, mode='r'):
    if file == None:
        if mode == 'r':
            return sys.stdin
        elif mode == 'w':
            return sys.stdout
    else:
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

    with fopen(output_file, mode='w') as file:
        json.dump(descriptor, file)

@main.command()
@click.argument('table_name')
@click.argument('table_schema_path')
@click.option('--connection-string')
@click.option('--db-schema')
@click.option('--indexes-fields')
@click.option('--geometry-support')
@click.option('--if-not-exists', is_flag=True, default=False)
def create_table(table_name, table_schema_path, connection_string, db_schema, indexes_fields, geometry_support, if_not_exists):
    table_schema = get_table_schema(table_schema_path)

    if indexes_fields != None:
        indexes_fields = indexes_fields.split(',')

    if re.match(carto.carto_connection_string_regex, connection_string) != None:
        load_postgis = geometry_support == 'postgis'
        return carto.create_table(table_name, load_postgis, table_schema, if_not_exists, indexes_fields, connection_string)

    connection_string = get_connection_string(connection_string)

    engine, storage = create_storage_adaptor(connection_string, db_schema, geometry_support)

    storage.create(table_name, table_schema, indexes_fields=indexes_fields)

@main.command()
@click.argument('table_name')
@click.option('--table-schema-path')
@click.option('--connection-string')
@click.option('-f','--input-file')
@click.option('--db-schema')
@click.option('--geometry-support')
@click.option('--from-srid')
@click.option('--skip-headers', is_flag=True)
@click.option('--indexes-fields')
@click.option('--upsert', is_flag=True)
def write(table_name,
          table_schema_path,
          connection_string,
          input_file,
          db_schema,
          geometry_support,
          from_srid,
          skip_headers,
          indexes_fields,
          upsert):
    table_schema = get_table_schema(table_schema_path)

    ## TODO: csv settings? use Frictionless Data csv standard?
    ## TODO: support line delimted json?
    with fopen(input_file) as file:
        rows = csv.reader(file)

        if skip_headers:
            next(rows)

        if re.match(carto.carto_connection_string_regex, connection_string) != None:
            load_postgis = geometry_support == 'postgis'

            if indexes_fields != None:
                indexes_fields = indexes_fields.split(',')

            carto.load(db_schema, table_name, load_postgis, table_schema, connection_string, rows, indexes_fields)
        else:
            connection_string = get_connection_string(connection_string)

            engine, storage = create_storage_adaptor(connection_string, db_schema, geometry_support, from_srid=from_srid)

            ## TODO: truncate? carto does. Makes this idempotent

            if table_schema_path != None:
                table_schema = get_table_schema(table_schema_path)
                storage.describe(table_name, descriptor=table_schema)
            else:
                storage.describe(table_name)

            if upsert:
                postgres.upsert(engine, db_schema, table_name, table_schema, rows)
            elif geometry_support == None and engine.dialect.driver == 'psycopg2':
                postgres.copy_from(engine, table_name, table_schema, rows)
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
    ## TODO: support line delimited json?
    with fopen(output_file, mode='w') as file:
        writer = csv.writer(file)

        descriptor = storage.describe(table_name)
        fields = map(lambda x: x['name'], descriptor['fields'])
        writer.writerow(fields)

        if geometry_support == None and engine.dialect.driver == 'psycopg2':
            postgres.copy_to(engine, table_name, file)
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
    elif engine.dialect.driver == 'cx_oracle':
        conn = engine.connect()
        if select_users != None:
            select_users = select_users.split(',')
        else:
            select_users = []
        grants_sql = []
        for user in select_users:
            grants_sql.append('GRANT SELECT ON {} TO {}'.format(old_table_name, user.strip()))

        # Oracle does not allow table modification within a transaction, so make individual transactions:
        sql1 = 'ALTER TABLE {} RENAME TO {}_old'.format(old_table_name, old_table_name)
        sql2 = 'ALTER TABLE {} RENAME TO {}'.format(new_table_name, old_table_name)
        sql3 = 'DROP TABLE {}_old'.format(old_table_name)

        try:
            conn.execute(sql1)
        except:
            print("Could not rename {} table. Does it exist?".format(old_table_name))
            raise
        try:
            conn.execute(sql2)
        except:
            print("Could not rename {} table. Does it exist?".format(new_table_name))
            rb_sql = 'ALTER TABLE {}_old RENAME TO {}'.format(old_table_name, old_table_name)
            conn.execute(rb_sql)
            raise
        try:
            conn.execute(sql3)
        except:
            print("Could not drop {}_old table. Do you have permission?".format(old_table_name))
            rb_sql1 = 'DROP TABLE {}'.format(old_table_name)
            conn.execute(rb_sql1)
            rb_sql2 = 'ALTER TABLE {}_old RENAME TO {}'.format(old_table_name, old_table_name)
            conn.execute(rb_sql2)
            raise
        try:
            for sql in grants_sql:
                conn.execute(sql)
        except:
            print("Could not grant all permissions to {}.".format(old_table_name))
            raise
    else:
        raise Exception('`{}` not supported by swap_table'.format(engine.dialect.driver))
