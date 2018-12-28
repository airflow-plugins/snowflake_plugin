from airflow.models import BaseOperator
from snowflake_plugin.hooks.snowflake_hook import SnowflakeHook


class SnowflakeOperator(BaseOperator):

    template_fields = ('query',)

    def __init__(self,
                 query,
                 snowflake_conn_id='snowflake_default',
                 role=None,
                 database=None,
                 *args, **kwargs):

        super(SnowflakeOperator, self).__init__(*args, **kwargs)
        self.snowflake_conn_id = snowflake_conn_id
        self.query = query
        self.role = role
        self.database = database

    def execute(self, context):
        hook = SnowflakeHook(snowflake_conn_id=self.snowflake_conn_id)
        if not self.query:
            raise Exception("missing requeired argument 'query'")
        hook.execute_sql(self.query)
