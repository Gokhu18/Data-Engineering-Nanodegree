from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageRedshiftFromS3Operator(BaseOperator):
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 target_table_name="",
                 s3_data_path="",
                 ignore_header=1,
                 delimiter=',',
                 *args, **kwargs):

        super(StageRedshiftFromS3Operator, self).__init__(*args, **kwargs)
        self.target_table_name = target_table_name
        self.redshift_conn_id = redshift_conn_id
        self.s3_data_path = s3_data_path
        self.aws_credentials_id = aws_credentials_id
        self.ignore_header = ignore_header
        self.delimiter = delimiter

    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info("Clearing data from destination Redshift table")
        redshift.run("DELETE FROM {}".format(self.target_table_name))

        self.log.info("Copying data from s3_to_redshift")
        copy_sql = (f"""
            copy {self.target_table_name} 
            from '{self.s3_data_path}'
            access_key_id '{credentials.access_key}'
            secret_access_key '{credentials.secret_key}'
            IGNOREHEADER {self.ignore_header}
            DELIMITER '{self.delimiter}'
        ; """)
        redshift.run(copy_sql)


