import io
import json
import csv

import jsontableschema
from jsontableschema.exceptions import InvalidObjectType
from sqlalchemy.dialects.postgresql import insert

class TransformStream(object):
    def __init__(self, fun):
        self.fun = fun

    def read(self, *args, **kwargs):
        return self.fun()

def type_fields(schema, row):
    missing_values = []
    if 'missingValues' in schema._Schema__descriptor:
        missing_values = schema._Schema__descriptor['missingValues']

    typed_row = []
    for index, field in enumerate(schema.fields):
        value = row[index]
        
        if value in missing_values:
                value = None
        elif field.type != 'geojson':
            try:
                value = field.cast_value(value)
            except InvalidObjectType:
                value = json.loads(value)

        typed_row.append(value)

    return typed_row

def copy_from(engine, table_name, table_schema, rows):
    schema = jsontableschema.Schema(table_schema)

    def transform():
        try:
            row = next(rows)
            typed_row = type_fields(schema, row)
            with io.StringIO() as out:
                writer = csv.writer(out)
                writer.writerow(typed_row)
                return out.getvalue()
        except StopIteration:
            return ''

    transformed_rows = TransformStream(transform)

    conn = engine.raw_connection()
    with conn.cursor() as cur:
        copy = 'COPY {} FROM STDIN CSV'.format(table_name)
        cur.copy_expert(copy, transformed_rows)
        conn.commit()
    conn.close()

def copy_to(engine, table_name, file):
    conn = engine.raw_connection()
    with conn.cursor() as cur:
        copy = 'COPY {} TO STDOUT WITH CSV'.format(table_name)
        cur.copy_expert(copy, file)
    conn.close()

upsert_sql = '''
INSERT INTO {table_name} ({columns})
VALUES ({params_str})
ON CONFLICT ({conflict_columns})
DO UPDATE SET ({columns}) = ({params_str})
'''

def get_upsert_sql(db_schema, table_name, primary_keys, columns):
    if db_schema:
        table_name = '{}.{}'.format(db_schema, table_name)

    return upsert_sql.format(
        table_name=table_name,
        columns=', '.join(columns),
        params_str=', '.join(['%s' for s in range(len(columns))]),
        conflict_columns=', '.join(primary_keys))

def upsert(engine, db_schema, table_name, table_schema, rows):
    if 'primaryKey' not in table_schema:
        raise Exception('`primaryKey` required for upsert')

    schema = jsontableschema.Schema(table_schema)

    upsert_sql = get_upsert_sql(
        db_schema,
        table_name,
        table_schema['primaryKey'],
        list(map(lambda x: x['name'], table_schema['fields'])))

    conn = engine.raw_connection()
    with conn.cursor() as cur:
        try:
            for row in rows:
                typed_row = type_fields(schema, row)
                cur.execute(upsert_sql, typed_row + typed_row) # has to do it twice, insert and set
            conn.commit()
        except:
            conn.rollback()
            raise
    conn.close()
