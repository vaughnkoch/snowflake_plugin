from airflow.plugins_manager import AirflowPlugin
from airflow.models import BaseOperator
from snowflake_plugin.hooks.snowflake_hook import SnowflakeHook
from airflow.version import version as airflow_version
from airflow.exceptions import AirflowException


# If quote_object_identifiers is False,
# the dbname, schema and table will all be uppercased.
# Otherwise, the identifiers will be used as-is and the caller is responsible for uppercasing whichever ones are necessary.

# Note: this assumes that an S3 stage is already set in Snowflake.
class S3ToSnowflakeOperator(BaseOperator):
    template_fields = ()  # Needs to be a tuple or list, e.g. ('foobar', )

    base_delete_batch = """
      DELETE FROM {snowflake_destination}
      WHERE etl_batch_tag = '{etl_batch_tag}'
    """

    base_copy = """
        COPY INTO {snowflake_destination}
        FROM (SELECT '{etl_batch_tag}' as etl_batch_tag, {columns_sql}
          FROM '@{stage_name}/{s3_key}')
        FORCE=TRUE
    """

    custom_format = 'FORMAT_NAME={custom_format}'
    format_type = 'TYPE={_type}'

    def __init__(self,
                 database,
                 schema,
                 table,
                 s3_conn_id,
                 snowflake_conn_id,
                 csv_column_count=None,
                 stage_name=None,
                 quote_object_identifiers=False,
                 *args, **kwargs):

        super(S3ToSnowflakeOperator, self).__init__(*args, **kwargs)

        # print ('************ KWARGS **************')
        # print (kwargs)

        self.database = database
        self.schema = schema
        self.table = table
        self.s3_conn_id = s3_conn_id
        self.snowflake_conn_id = snowflake_conn_id
        self.csv_column_count = csv_column_count
        self.stage_name = stage_name

        # If quote_object_identifiers is false, we'll uppercase everything else.
        self.quote_object_identifiers = quote_object_identifiers
        self.quote_char = '"' if quote_object_identifiers else ''

    def build_delete_batch(self):
      fmt_args = {
        'etl_batch_tag': self.etl_batch_tag,
        'snowflake_destination': self.get_snowflake_destination(),
      }

      return self.base_delete_batch.format(**fmt_args)

    def get_db_prefixes(self):
      prefixes = ''

      # e.g. '"mydb."'. or 'mydb.'
      if self.database:
        prefixes += f'{self.quote_char}{self.database}{self.quote_char}.'

      # e.g. '"myschema"'. or 'myschema.'
      if self.schema:
        prefixes += f'{self.quote_char}{self.schema}{self.quote_char}.'

      return prefixes


    def get_snowflake_destination(self):
      destination = self.get_db_prefixes() + self.table

      if not self.quote_object_identifiers:
        destination = destination.upper()

      return destination


    def build_copy(self):
      columns = [f'${i}' for i in range(1, self.csv_column_count + 1)]
      columns_sql = ', '.join(columns)

      fmt_args = {
        'snowflake_destination': self.get_snowflake_destination(),
        'etl_batch_tag': self.etl_batch_tag,
        'columns_sql': columns_sql,
        'stage_name': self.stage_name,
        's3_key': self.s3_key,
      }

      return self.base_copy.format(**fmt_args)


    def execute(self, context):
      self.s3_key = context['task_instance'].xcom_pull(key='s3_key', task_ids='lambda_amplitude_to_s3')
      self.etl_batch_tag = self.s3_key.split('/')[-1]

      print (f'Executing S3 to Snowflake: s3_key={self.s3_key}, stage={self.stage_name}, destination={self.get_snowflake_destination()}, etl_batch_tag={self.etl_batch_tag}')

      sf_hook = SnowflakeHook(snowflake_conn_id=self.snowflake_conn_id).get_conn()

      delete_batch_sql = self.build_delete_batch()
      print (delete_batch_sql)
      sf_hook.cursor().execute(delete_batch_sql)
      print (f'Finished deleting events with etl_batch_tag={self.etl_batch_tag}.')

      copy_sql = self.build_copy()
      print (copy_sql)
      sf_hook.cursor().execute(copy_sql)
      print (f'Finished copying events with etl_batch_tag={self.etl_batch_tag}.')
