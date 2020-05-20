from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.hooks.postgres_hook import PostgresHook

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 sql_query="",
                 filter_expr="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.sql_query = sql_query
        self.filter_expr = filter_expr

    def execute(self, context):
        self.log.info('Establishing connection to redshift')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info("Running query")
        redshift.run(self.sql_query.format(self.filter_expr))
