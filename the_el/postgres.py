import io
import json
import csv

import jsontableschema

class TransformStream(object):
    def __init__(self, fun):
        self.fun = fun

    def read(self, *args, **kwargs):
        return self.fun()

def copy_from(engine, table_name, table_schema, rows):
    schema = jsontableschema.Schema(table_schema)

    def type_fields(row):
        typed_row = []
        for index, field in enumerate(schema.fields):
            value = row[index]
            if field.type != 'geojson':
                try:
                    value = field.cast_value(value)
                except InvalidObjectType:
                    value = json.loads(value)
            typed_row.append(value)

        return typed_row

    def transform():
        try:
            row = next(rows)
            typed_row = type_fields(row)
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
