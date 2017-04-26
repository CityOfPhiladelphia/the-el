import re
import json
from datetime import datetime

from sqlalchemy import *
from sqlalchemy.dialects import postgresql
from sqlalchemy.schema import CreateTable
from sqlalchemy.sql import literal_column
from jsontableschema_sql.mappers import load_postgis_support, descriptor_to_columns_and_constraints
import requests
import jsontableschema

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
        constraints.append(UniqueConstraint(*pk, name='uniq_' + '_'.join(pk)))

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

def create_table(table_name, json_table_schema, connection_string):
    creds = re.match(carto_connection_string_regex, connection_string).groups()
    statement = CreateTable(get_table(table_name, json_table_schema))
    str_statement = statement.compile(dialect=postgresql.dialect())
    carto_sql_call(creds, str_statement)

def swap_table(db_schema, new_table_name, old_table_name, connection_string):
    creds = re.match(carto_connection_string_regex, connection_string).groups()
    sql = 'BEGIN;' +\
          'ALTER TABLE "{}" RENAME TO "{}_old";'.format(old_table_name, old_table_name) +\
          'ALTER TABLE "{}" RENAME TO "{}";'.format(new_table_name, old_table_name) +\
          'DROP TABLE "{}_old";'.format(old_table_name) +\
          'COMMIT;'
    carto_sql_call(creds, sql)

def type_fields(schema, row):
    typed_row = []
    for index, field in enumerate(schema.fields):
        value = row[index]
        if field.type != 'geojson':
            try:
                value = field.cast_value(value)
            except InvalidObjectType:
                value = json.loads(value)
        if isinstance(value, datetime):
            value = literal_column("'" + value.strftime('%Y-%m-%d %H:%M:%S') + "'")
        typed_row.append(value)

    return typed_row

def insert(creds, table, rows):
    statement = table.insert(values=rows)
    str_statement = statement.compile(dialect=postgresql.dialect(), compile_kwargs={"literal_binds": True})
    carto_sql_call(creds, str_statement)

def cartodbfytable(creds, db_schema, table_name):
    carto_sql_call(creds, "select cdb_cartodbfytable('{}', '{}');".format(db_schema, table_name))

def load(db_schema, table_name, json_table_schema, connection_string, rows, batch_size=500):
    creds = re.match(carto_connection_string_regex, connection_string).groups()
    table = get_table(table_name, json_table_schema)
    schema = jsontableschema.Schema(json_table_schema)

    _buffer = []
    for row in rows:
        _buffer.append(type_fields(schema, row))
        if len(_buffer) >= batch_size:
            insert(creds, table, _buffer)
            _buffer = []

    if len(_buffer) > 0:
        insert(creds, table, _buffer)

    cartodbfytable(creds, db_schema, table_name) ## TODO: move to beginning of swap_table?
