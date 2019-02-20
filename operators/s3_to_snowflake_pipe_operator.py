from airflow.hooks.S3_hook import S3Hook
from airflow.models import BaseOperator
from snowflake_plugin.hooks.snowflake_hook import SnowflakeHook


class S3ToSnowflakePipeOperator(BaseOperator):
    template_fields = ('file_prefix', 's3_bucket', 'pipe')

    def __init__(self,
                 pipe,
                 s3_bucket,
                 file_prefix,
                 file_path_mapper=None,
                 s3_conn_id='s3_default',
                 snowflake_conn_id='snowflake_default',
                 database=None,
                 schema=None,
                 *args, **kwargs):

        super(S3ToSnowflakePipeOperator, self).__init__(*args, **kwargs)
        self.pipe = pipe
        self.s3_bucket = s3_bucket
        self.file_prefix = file_prefix
        self.file_path_mapper = file_path_mapper
        self.s3_conn_id = s3_conn_id
        self.snowflake_conn_id = snowflake_conn_id
        self.database = database
        self.schema = schema

    def list_files_from_s3(self):
        hook = S3Hook(self.s3_conn_id)
        listing = hook.list_keys(
            bucket_name=self.s3_bucket,
            prefix=self.file_prefix)
        if self.file_path_mapper and listing:
            listing = list(map(self.file_path_mapper, listing))
        return listing

    def execute(self, context):
        hook = SnowflakeHook(snowflake_conn_id=self.snowflake_conn_id)
        files = self.list_files_from_s3()
        if files:
            self.log.debug(
                'sending {0} files to {1}'.format(
                    len(files), self.pipe)
            )
            hook.pipe_insert_files(self.pipe, files, schema=self.schema)
        else:
            msg = "didn't find any files like {0}/{1}".format(
                self.s3_bucket,
                self.file_prefix
            )
            self.log.info(msg)
