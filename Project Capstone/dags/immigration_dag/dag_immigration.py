from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from operators.load_fact import LoadFactOperator
from operators.load_dimension import LoadDimensionOperator
from operators.stage_redshift_from_s3 import StageRedshiftFromS3Operator
from operators.data_quality import DataQualityOperator

from helpers.sql_queries_immigration import insert_queries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'veegaaa',
    'start_date': datetime(2019, 1, 12),
    # 'depends_on_past': False,
    # 'email_on_failure': False,
    # 'email_on_retry': False,
    # 'retries': 1,
    # 'retry_delay': timedelta(minutes=1),
    # 'catchup': False
}

dag = DAG('dag_immigration',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='@once'
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_immigration_to_redshift = StageRedshiftFromS3Operator(
    task_id='Stage_immigration',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    target_table_name="staging_immigration",
    s3_data_path="s3://dend-veegaaa-capstone/immigration_data_sample.csv",
    ignore_header=1,
    delimiter=',',
)

stage_demography_to_redshift = StageRedshiftFromS3Operator(
    task_id='Stage_demography',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    target_table_name="staging_demographics",
    s3_data_path="s3://dend-veegaaa-capstone/us-cities-demographics.csv",
    ignore_header=1,
    delimiter=';',
)

load_immigration_facts_table = LoadFactOperator(
    task_id='Load_immigration_facts_fact_table',
    dag=dag,
    redshift_conn_id="redshift",
    sql_query=insert_queries['immigration_facts'],
    filter_expr="WHERE cicid is not null",
)

load_states_dimension_table = LoadDimensionOperator(
    task_id='Load_states_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    sql_query=insert_queries['states'],
    filter_expr="",
    mode='append'
)

load_cities_dimension_table = LoadDimensionOperator(
    task_id='Load_cities_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    sql_query=insert_queries['cities'],
    filter_expr="",
    mode='append'
)

load_times_dimension_table = LoadDimensionOperator(
    task_id='Load_times_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    sql_query=insert_queries['times'],
    filter_expr="",
    mode='append'
)


run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",
    table_list = ['immigration_facts', 'states', 'cities', 'times']
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> (stage_immigration_to_redshift, stage_demography_to_redshift) >> load_immigration_facts_table
load_immigration_facts_table >> (load_states_dimension_table,
                                 load_cities_dimension_table,
                                 load_times_dimension_table) >> run_quality_checks
run_quality_checks >> end_operator

