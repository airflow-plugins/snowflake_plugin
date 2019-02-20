from airflow.hooks.base_hook import BaseHook
import snowflake.connector
from cryptography.hazmat.primitives.serialization import load_pem_private_key
from cryptography.hazmat.backends import default_backend
from jwt.utils import force_bytes
import jwt
import requests
import json
import time
import uuid


class SnowflakeHook(BaseHook):
    def __init__(self, snowflake_conn_id='snowflake_default'):
        self.snowflake_conn_id = snowflake_conn_id
        self.snowflake_conn = self.get_connection(snowflake_conn_id)
        self.user = self.snowflake_conn.login
        self.password = self.snowflake_conn.password
        if self.snowflake_conn.extra:
            self.extra_params = self.snowflake_conn.extra_dejson
            self.account = self.extra_params.get('account', None)
            self.region = self.extra_params.get('region', None)
            self.database = self.extra_params.get('database', None)
            self.role = self.extra_params.get('role', None)
            self.schema = self.extra_params.get('schema', None)
            self.warehouse = self.extra_params.get('warehouse', None)
            self.private_key = self.extra_params.get('private_key', None)
            self.private_key_password = self.extra_params.get(
                'private_key_password', None)

    def get_conn(self):
        return snowflake.connector.connect(
            user=self.user,
            password=self.password,
            account=self.account,
            region=self.region,
            database=self.database,
            role=self.role,
            schema=self.schema,
            warehouse=self.warehouse
        )

    def pipe_insert_report(self, pipe, database=None, schema=None):
        database = database or self.database
        schema = schema or self.schema
        if self.private_key_password:
            cert = load_pem_private_key(
                force_bytes(
                    self.private_key), password=force_bytes(
                    self.private_key_password), backend=default_backend())
        else:
            cert = load_pem_private_key(
                force_bytes(
                    self.private_key),
                password=None,
                backend=default_backend())
        now = time.time()
        auth_token = jwt.encode({'iss': '{0}.{1}'.format(self.account.upper(
        ), self.user.upper()), 'exp': now + 3600, 'iat': now}, cert, algorithm='RS256')
        uri = 'https://{account}.{region}.snowflakecomputing.com/v1/' \
              'data/pipes/{database}.{schema}.{pipe}/insertReport'.format(account=self.account,
                                                                          region=self.region,
                                                                          pipe=pipe,
                                                                          database=database,
                                                                          schema=schema)
        headers = {
            'Authorization': 'Bearer {0}'.format(
                auth_token.decode('UTF-8')),
            'Accept': 'application/json',
            'Content-Type': 'application/json'}
        self.log.debug('executing pipe: {0}'.format(uri))
        self.log.debug('with headers {0}:'.format(headers))
        resp = requests.get(uri, headers=headers)
        self.log.info('pipe response: {0}'.format(resp.json()))
        return resp.json()

    def pipe_insert_files(self,
                          pipe,
                          files,
                          database=None,
                          schema=None,
                          request_id=None):
        database = database or self.database
        schema = schema or self.schema
        if self.private_key_password:
            cert = load_pem_private_key(
                force_bytes(
                    self.private_key), password=force_bytes(
                    self.private_key_password), backend=default_backend())
        else:
            cert = load_pem_private_key(
                force_bytes(
                    self.private_key),
                password=None,
                backend=default_backend())
        now = time.time()
        auth_token = jwt.encode({'iss': '{0}.{1}'.format(self.account.upper(
        ), self.user.upper()), 'exp': now + 3600, 'iat': now}, cert, algorithm='RS256')
        body = {'files': list(map(lambda f: {'path': f}, files))}
        uri = 'https://{account}.{region}.snowflakecomputing.com/v1/' \
              'data/pipes/{database}.{schema}.{pipe}/insertFiles'.format(account=self.account,
                                                                         region=self.region,
                                                                         pipe=pipe,
                                                                         database=database,
                                                                         schema=schema)
        if request_id is None:
            request_id = str(uuid.uuid4())
        params = {'requestId': request_id}

        headers = {
            'Authorization': 'Bearer {0}'.format(
                auth_token.decode('UTF-8')),
            'Accept': 'application/json',
            'Content-Type': 'application/json'}
        self.log.debug('executing pipe: {0}'.format(uri))
        self.log.debug('with headers {0}:'.format(headers))
        self.log.debug('with body: {0}'.format(body))
        resp = requests.post(uri,
                             data=json.dumps(body),
                             headers=headers,
                             params=params)
        if resp.status_code != 200:
            raise Exception(
                '''failed to execute pipe, exited with error {0}'''.format(
                    resp.text
                )
            )
        self.log.info('pipe response: {0}'.format(resp.json()))
        return resp.json()

    def execute_sql(self, query, database=None, role=None):
        cs = self.get_conn().cursor()
        cs.execute("USE WAREHOUSE {0}".format(self.warehouse))
        cs.execute("USE DATABASE {0}".format(database or self.database))
        cs.execute("USE ROLE {0}".format(role or self.role))
        if isinstance(query, list):
            query_sequence = query
        else:
            query_sequence = [query]

        for query in query_sequence:
            cs.execute(query)
