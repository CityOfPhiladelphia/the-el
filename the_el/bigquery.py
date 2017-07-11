import os
import json

from google.oauth2 import service_account
from apiclient.discovery import build
from jsontableschema_bigquery import Storage

bigquery_connection_string_regex = r'bigquery://(?P<project>[^/]+)/(?P<dataset>[^/]+)'

def get_credentials(service_account_path=None, service_account_json=None):
    if not service_account_path and 'GOOGLE_APPLICATION_CREDENTIALS' in os.environ:
        service_account_path = os.environ['GOOGLE_APPLICATION_CREDENTIALS']

    if service_account_path: 
        return service_account.Credentials.from_service_account_file(service_account_path)

    if not service_account_json and 'GOOGLE_SERVICE_ACCOUNT_JSON' in os.environ:
        service_account_json = os.environ['GOOGLE_SERVICE_ACCOUNT_JSON']

    if isinstance(service_account_json, str):
        service_account_json = json.loads(service_account_json)

    if service_account_json:
        return service_account.Credentials.from_service_account_info(service_account_json)

    raise Exception('No Google credentials provided')

def get_bigquery_storage(project, dataset):
    credentials = get_credentials()
    service = build('bigquery', 'v2', credentials=credentials)
    return Storage(service, project, dataset)

def describe_table(project, dataset, table_name):
    storage = get_bigquery_storage(project, dataset)

    return storage.describe(table_name)

def create_table(project, dataset, table_name, json_table_schema):
    storage = get_bigquery_storage(project, dataset)

    storage.create(table_name, json_table_schema)

def iter(project, dataset, table_name):
    storage = get_bigquery_storage(project, dataset)

    yield from storage.iter(table_name)

def write(project, dataset, table_name, table_schema, rows):
    storage = get_bigquery_storage(project, dataset)

    storage.describe(table_name, descriptor=table_schema)

    storage.write(table_name, rows)

def drop(project, dataset, table_name):
    storage = get_bigquery_storage(project, dataset)

    storage.delete(table_name)
