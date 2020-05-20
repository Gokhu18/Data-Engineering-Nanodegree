from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 sql_query="",
                 filter_expr="",
                 mode='append', # or 'delete'
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.sql_query = sql_query
        self.filter_expr = filter_expr
        self.mode = mode

    def execute(self, context):
        self.log.info('Establishing connection to redshift')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        if self.mode == 'delete':
            self.log.info("Mode set to 'delete'. Clearing previous data")
            redshift.run("DELETE FROM {}".format(self.target_table_name))

        self.log.info("Running query")
        redshift.run(self.sql_query.format(self.filter_expr))
