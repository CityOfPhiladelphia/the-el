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

def carto_sql_call(logger, creds, str_statement, log_response=False):
    data = {
        'q': str_statement,
        'api_key': creds[1]
    }
    response = requests.post('https://{}.carto.com/api/v2/sql/'.format(creds[0]), data=data)
    try:
        response.raise_for_status()
    except:
        logger.error('HTTP ' + str(response.status_code) + ': ' + response.text)
        raise

    if log_response:
        logger.info('HTTP ' + str(response.status_code) + ': ' + response.text)

    return response.json()

def create_indexes(logger, creds, table_name, indexes_fields):
    logger.info('{} - creating table indexes - {}'.format(table_name, ','.join(indexes_fields)))
    str_statement = ''
    for indexes_field in indexes_fields:
        str_statement += 'CREATE INDEX {table}_{field} ON "{table}" ("{field}");\n'.format(table=table_name, field=indexes_field)
    carto_sql_call(logger, creds, str_statement)

def create_table(logger, table_name, load_postgis, json_table_schema, if_not_exists, indexes_fields, connection_string):
    if load_postgis:
        load_postgis_support()

    creds = re.match(carto_connection_string_regex, connection_string).groups()
    statement = CreateTable(get_table(table_name, json_table_schema))
    str_statement = statement.compile(dialect=postgresql.dialect())

    if if_not_exists:
        str_statement = str(str_statement).replace('CREATE TABLE', 'CREATE TABLE IF NOT EXISTS')

    carto_sql_call(logger, creds, str_statement)

    check_table_sql = "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = '{}');"
    response = carto_sql_call(logger, creds, check_table_sql.format(table_name))
    exists = response['rows'][0]['exists']

    if not exists:
        message = '{} - Could not create table'.format(table_name)
        logger.error(message)
        raise Exception(message)

    if indexes_fields:
        create_indexes(creds, table_name, indexes_fields)

def generate_select_grants(logger, table, users):
    grants_sql = ''
    for user in users:
        logger.info('{} - Granting SELECT to {}'.format(table, user))
        grants_sql += 'GRANT SELECT ON "{}" TO "{}";'.format(table, user)
    return grants_sql

def swap_table(logger, db_schema, new_table_name, old_table_name, select_users, connection_string):
    creds = re.match(carto_connection_string_regex, connection_string).groups()
    sql = 'BEGIN;' +\
          'ALTER TABLE "{}" RENAME TO "{}_old";'.format(old_table_name, old_table_name) +\
          'ALTER TABLE "{}" RENAME TO "{}";'.format(new_table_name, old_table_name) +\
          'DROP TABLE "{}_old";'.format(old_table_name) +\
          generate_select_grants(logger, old_table_name, select_users) +\
          'COMMIT;'
    carto_sql_call(logger, creds, sql)

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

def insert(logger, creds, table, rows):
    statement = table.insert(values=rows)
    str_statement = statement.compile(dialect=postgresql.dialect(), compile_kwargs={"literal_binds": True})
    response_json = carto_sql_call(logger, creds, str_statement)
    return response_json['total_rows']

def cartodbfytable(logger, creds, db_schema, table_name):
    logger.info('{} - cdb_cartodbfytable\'ing table'.format(table_name))
    carto_sql_call(logger, creds, "select cdb_cartodbfytable('{}', '{}');".format(db_schema, table_name))

def vacuum_analyze(logger, creds, table_name):
    logger.info('{} - vacuum analyzing table'.format(table_name))
    carto_sql_call(logger, creds, 'VACUUM ANALYZE "{}";'.format(table_name))

def truncate(logger, creds, table_name):
    logger.info('{} - Truncating'.format(table_name))
    carto_sql_call(logger, creds, 'TRUNCATE TABLE "{}";'.format(table_name))

def verify_count(logger, creds, table_name, num_rows_expected, num_rows_inserted):
    data = carto_sql_call(logger, creds, 'SELECT count(*) FROM "{}";'.format(table_name))
    actual = data['rows'][0]['count']
    message = '{} - row count - expected: {} inserted: {} actual: {}'.format(
        table_name,
        num_rows_expected,
        num_rows_inserted,
        actual)
    if actual != num_rows_expected:
        logger.error(message)
        raise Exception('Rows counted does not match - expected: {} inserted: {} actual: {}'.format(
            num_rows_expected,
            num_rows_inserted,
            data['rows'][0]['count']))
    logger.info(message)

def load(logger,
         db_schema,
         table_name,
         load_postgis,
         json_table_schema,
         connection_string,
         rows,
         indexes_fields,
         do_truncate,
         batch_size=500):
    if load_postgis:
        load_postgis_support()

    creds = re.match(carto_connection_string_regex, connection_string).groups()
    table = get_table(table_name, json_table_schema)
    schema = jsontableschema.Schema(json_table_schema)

    if do_truncate:
        truncate(logger, creds, table_name)

    _buffer = []
    num_rows_expected = 0
    total_num_rows_inserted = 0
    for row in rows:
        num_rows_expected += 1
        _buffer.append(type_fields(schema, row))
        buf_ln = len(_buffer)
        if buf_ln >= batch_size:
            num_rows_inserted = insert(logger, creds, table, _buffer)
            logger.info('{} - Inserted {} rows'.format(table_name, num_rows_inserted))
            if buf_ln != num_rows_inserted:
                message = '{} - Number of rows inserted does not match expected - expected: {} actual: {}'.format(
                    table_name,
                    buf_ln,
                    num_rows_inserted)
                logger.error(message)
                raise Exception(message)
            total_num_rows_inserted += num_rows_inserted
            _buffer = []

    if len(_buffer) > 0:
        insert(logger, creds, table, _buffer)

    verify_count(logger, creds, table, num_rows_expected, total_num_rows_inserted)

    cartodbfytable(logger, creds, db_schema, table_name)

    if indexes_fields:
        create_indexes(logger, creds, table_name, indexes_fields)

    vacuum_analyze(logger, creds, table_name)
