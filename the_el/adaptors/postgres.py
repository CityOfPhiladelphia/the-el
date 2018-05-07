import csv
import sys

import arrow
import psycopg2
from psycopg2 import sql
from datetime import datetime

from .base import BaseAdaptor

class PostgresAdaptor(BaseAdaptor):
    def __init__(self, connection_config, *args, **kwargs):
        self.conn = psycopg2.connect(
            host=connection_config.hostname,
            port=connection_config.port,
            dbname=connection_config.database,
            user=connection_config.username,
            password=connection_config.password)

    def destroy_connection(self):
        self.conn.close()

    def normalize_table_path(self, table):
        path = table.split('.')
        if len(path) > 2:
            raise Exception('Invalid table path: {}'.format(table))
        if len(path) == 1:
            return 'public', path[0]
        return path

    def extract(self, table):
        catalog, table_name = self.normalize_table_path(table)

        with self.conn.cursor() as cur:
            cur.execute(
                sql.SQL('SELECT * FROM {}.{}').format(
                    sql.Identifier(catalog),
                    sql.Identifier(table_name)))

            writer = csv.writer(sys.stdout)
            writer.writerow(list(map(lambda x: x[0], cur.description)))

            resultset = cur.fetchmany(10000)
            while len(resultset) > 0:
                for row in resultset:
                    new_row = []
                    for col in row:
                        if isinstance(col, datetime):
                            col = arrow.get(col).isoformat()
                        new_row.append(col)
                    writer.writerow(new_row)
                resultset = cur.fetchmany(10000)
