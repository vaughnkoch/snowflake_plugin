from airflow.hooks.S3_hook import S3Hook
from airflow.plugins_manager import AirflowPlugin
from airflow.models import BaseOperator
from snowflake_plugin.hooks.snowflake_hook import SnowflakeHook
from airflow.version import version as airflow_version
from airflow.exceptions import AirflowException


class S3ToSnowflakeOperator(BaseOperator):
    template_fields = ('s3_key', 'etl_batch_tag')

    base_delete_batch = """
      DELETE FROM {snowflake_destination}
      WHERE etl_batch_tag = '{etl_batch_tag}'
    """

    base_copy = """
        COPY INTO {snowflake_destination}
        FROM (SELECT '{etl_batch_tag}' as etl_batch_tag, {columns_sql}
          FROM '@{db_prefixes}{stage_name}/{s3_key}')
        FORCE=TRUE
    """

    custom_format = 'FORMAT_NAME={custom_format}'
    format_type = 'TYPE={_type}'

    def __init__(self,
                 s3_bucket,
                 s3_key,
                 etl_batch_tag,
                 database,
                 schema,
                 table,
                 s3_conn_id,
                 snowflake_conn_id,
                 file_format_name='JSON',
                 custom_format_name=None,
                 csv_column_count=None,
                 stage_name=None,
                 *args, **kwargs):

        super(S3ToSnowflakeOperator, self).__init__(*args, **kwargs)
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.etl_batch_tag = etl_batch_tag
        self.database = database
        self.schema = schema
        self.table = table
        self.s3_conn_id = s3_conn_id
        self.snowflake_conn_id = snowflake_conn_id
        self.file_format_name = file_format_name
        self.custom_format_name = custom_format_name
        self.csv_column_count = csv_column_count
        self.stage_name = stage_name


    def build_delete_batch(self):
      fmt_args = {
        'etl_batch_tag': self.etl_batch_tag,
        'snowflake_destination': self.get_snowflake_destination(),
      }

      return self.base_delete_batch.format(**fmt_args)

    def get_db_prefixes(self):
      prefixes = ''
      if self.database:
        prefixes += f'{self.database}.'

      if self.schema:
        prefixes += f'{self.schema}.'

      return prefixes

    def get_snowflake_destination(self):
      return self.get_db_prefixes() + self.table


    def build_copy(self):
        s3_hook = S3Hook(self.s3_conn_id)
        if hasattr(s3_hook,'get_credentials'):
            a_key , s_key, token = s3_hook.get_credentials()
        elif hasattr(s3_hook,'_get_credentials'):
            a_key , s_key, _ ,_= s3_hook._get_credentials(region_name=None)
        else:
            raise AirflowException('{0} does not support airflow v{1}'.format(self.__class__.__name__,airflow_version))


        file_format = None
        if self.custom_format_name:
          file_format = self.custom_format.format(custom_format=self.custom_format_name)
        else:
          file_format = self.format_type.format(_type=self.file_format_name)

        columns = [f'${i}' for i in range(1, self.csv_column_count + 1)]
        columns_sql = ', '.join(columns)

        fmt_args = {
            'snowflake_destination': self.get_snowflake_destination(),
            'etl_batch_tag': self.etl_batch_tag,
            'columns_sql': columns_sql,
            'stage_name': self.stage_name,
            's3_bucket': self.s3_bucket,
            's3_key': self.s3_key,
            'aws_access_key_id': a_key,
            'aws_secret_access_key': s_key,
            'file_format': file_format,
            'db_prefixes': self.get_db_prefixes(),
        }

        return self.base_copy.format(**fmt_args)

    def execute(self, context):
        sf_hook = SnowflakeHook(snowflake_conn_id=self.snowflake_conn_id).get_conn()
        # import ipdb; ipdb.set_trace()

        # import logging
        # logging.getLogger('snowflake.connector.connection').setLevel(logging.DEBUG)

        delete_batch_sql = self.build_delete_batch()
        print (delete_batch_sql)
        sf_hook.cursor().execute(delete_batch_sql)
        print (f'Finished deleting events with etl_batch_tag={self.etl_batch_tag}.')

        copy_sql = self.build_copy()
        print (copy_sql)
        sf_hook.cursor().execute(copy_sql)
        print (f'Finished copying events with etl_batch_tag={self.etl_batch_tag}.')
