import re
import json
from datetime import datetime, date

from sqlalchemy import *
from sqlalchemy.dialects import postgresql
from sqlalchemy.schema import CreateTable
from sqlalchemy.sql import literal_column
from jsontableschema_sql.mappers import load_postgis_support, descriptor_to_columns_and_constraints
import requests
import jsontableschema
from jsontableschema.exceptions import InvalidObjectType
import click

carto_connection_string_regex = r'^carto://(.+):(.+)'

def get_table(table_name, json_table_schema):
    metadata = MetaData()
     ## not including primary and foreign keys, cartodb_id is always the pk
    columns, ignored_constraints, indexes = descriptor_to_columns_and_constraints(
        '', table_name, json_table_schema, [], None)

    ## Add a unique constraint on the pk to prevent duplicates
    constraints = []
    pk = json_table_schema.get('primaryKey', None)
    if pk is not None:
        constraints.append(UniqueConstraint(*pk, name='uniq_' + table_name + '_' + '_'.join(pk)))

    return Table(table_name, metadata, *(columns+constraints+indexes))

def carto_sql_call(creds, str_statement):
    data = {
        'q': str_statement,
        'api_key': creds[1]
    }
    response = requests.post('https://{}.carto.com/api/v2/sql/'.format(creds[0]), data=data)
    try:
        response.raise_for_status()
    except:
        print(str(response.status_code) + ': ' + response.text)
        raise

def create_table(table_name, load_postgis, json_table_schema, if_not_exists, indexes_fields, connection_string):
    if load_postgis:
        load_postgis_support()

    creds = re.match(carto_connection_string_regex, connection_string).groups()
    statement = CreateTable(get_table(table_name, json_table_schema))
    str_statement = statement.compile(dialect=postgresql.dialect())

    if if_not_exists:
        str_statement = str(str_statement).replace('CREATE TABLE', 'CREATE TABLE IF NOT EXISTS')

    carto_sql_call(creds, str_statement)

    if indexes_fields:
        str_statement = ''
        for indexes_field in indexes_fields:
            str_statement += 'CREATE INDEX {table}_{field} ON "{table}" ("{field}");\n'.format(table=table_name, field=indexes_field)
        carto_sql_call(creds, str_statement)

def generate_select_grants(table, users):
    grants_sql = ''
    for user in users:
        grants_sql += 'GRANT SELECT ON "{}" TO "{}";'.format(table, user)
    return grants_sql

def swap_table(db_schema, new_table_name, old_table_name, select_users, connection_string):
    creds = re.match(carto_connection_string_regex, connection_string).groups()
    sql = 'BEGIN;' +\
          'ALTER TABLE "{}" RENAME TO "{}_old";'.format(old_table_name, old_table_name) +\
          'ALTER TABLE "{}" RENAME TO "{}";'.format(new_table_name, old_table_name) +\
          'DROP TABLE "{}_old";'.format(old_table_name) +\
          generate_select_grants(old_table_name, select_users) +\
          'COMMIT;'
    carto_sql_call(creds, sql)

def type_fields(schema, row):
    missing_values = []
    if 'missingValues' in schema._Schema__descriptor:
        missing_values = schema._Schema__descriptor['missingValues']

    typed_row = []
    for index, field in enumerate(schema.fields):
        value = row[index]
        if field.type == 'geojson':
            if value == '' or value == 'NULL' or value == None:
                value = None
            else:
                value = literal_column("ST_GeomFromGeoJSON('{}')".format(value))
        elif field.type == 'string' and 'None' not in missing_values and value == 'None':
            value = 'None'
        elif field.type == 'string' and value.lower() == 'nan':
            value = value # HACK: tableschema-py 1.0 fixes this but is not released yet
        elif field.type == 'array' or field.type == 'object':
            if value in missing_values:
                value = None
            else:
                value = literal_column('\'' + value + '\'::jsonb')
        else:
            try:
                value = field.cast_value(value)
            except InvalidObjectType:
                value = json.loads(value)

        if isinstance(value, datetime):
            value = literal_column("'" + value.strftime('%Y-%m-%d %H:%M:%S') + "'")
        elif isinstance(value, date):
            value = literal_column("'" + value.strftime('%Y-%m-%d') + "'")

        if value is None:
            value = literal_column('null')

        typed_row.append(value)

    return typed_row

def insert(creds, table, rows):
    statement = table.insert(values=rows)
    str_statement = statement.compile(dialect=postgresql.dialect(), compile_kwargs={"literal_binds": True})
    carto_sql_call(creds, str_statement)

def cartodbfytable(creds, db_schema, table_name):
    carto_sql_call(creds, "select cdb_cartodbfytable('{}', '{}');".format(db_schema, table_name))

def vacuum_analyze(creds, table_name):
    carto_sql_call(creds, 'VACUUM ANALYZE "{}";'.format(table_name))

def truncate(creds, table_name):
    carto_sql_call(creds, 'TRUNCATE TABLE "{}";'.format(table_name))

def load(db_schema, table_name, load_postgis, json_table_schema, connection_string, rows, batch_size=500):
    if load_postgis:
        load_postgis_support()

    creds = re.match(carto_connection_string_regex, connection_string).groups()
    table = get_table(table_name, json_table_schema)
    schema = jsontableschema.Schema(json_table_schema)

    truncate(creds, table_name)

    _buffer = []
    for row in rows:
        _buffer.append(type_fields(schema, row))
        if len(_buffer) >= batch_size:
            insert(creds, table, _buffer)
            _buffer = []

    if len(_buffer) > 0:
        insert(creds, table, _buffer)

    cartodbfytable(creds, db_schema, table_name)
    vacuum_analyze(creds, table_name)
