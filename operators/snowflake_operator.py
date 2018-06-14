from airflow.hooks.S3_hook import S3Hook
from airflow.plugins_manager import AirflowPlugin
from airflow.models import BaseOperator
from snowflake_plugin.hooks.snowflake_hook import SnowflakeHook


class SnowflakeOperator(BaseOperator):

    def __init__(self,
                 query=None,
                 query_file=None,
                 role='public',
                 snowflake_conn_id='snowflake_default',
                 *args, **kwargs):

        super(SnowflakeOperator, self).__init__(*args, **kwargs)
        self.snowflake_conn_id = snowflake_conn_id
        self.query_or_sequence = query 
        self.query_file= query_file
        self.role = role


    def execute(self, context):
        hook = SnowflakeHook(snowflake_conn_id=self.snowflake_conn_id).get_conn()
        cs = hook.cursor()
        cs.execute("USE WAREHOUSE {0}".format(hook.warehouse))
        cs.execute("USE DATABASE {0}".format(hook.database))
        cs.execute("USE ROLE {0}".format(self.role))
        
        if self.query_or_sequence is None and self.query_file is None:
            raise Exception('query or query_file must be supplied to the operator')

        if self.query_or_sequence is not None:
            if isinstance(self.query_or_sequence, list):
                query_sequence = self.query_or_sequence
            else:
                query_sequence = [self.query_or_sequence]

            for query in query_sequence:
                cs.execute(query)

        if self.query_file is not None:
            if isinstance(self.query_file, list):
                files_sequence = self.query_file
            else:
                files_sequence = [self.query_file]

            for path in files_sequence:
                query = open(path,'r').read()
                cs.execute(query)
