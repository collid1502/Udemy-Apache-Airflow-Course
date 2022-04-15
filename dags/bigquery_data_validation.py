from airflow import DAG
from airflow.utils.dates import days_ago 

# now import the custom classes we have built in the plugins\biquery_lugin.py file 
# remember, a `plugins` folder under $AIRFLOW_HOME, will be picked up by airflow and imported via `airflow.operators`
from airflow.operators.bigquery_plugin import (
    BigQueryDataValidatorOperator,
    BigQueryDatasetSensor,
)

default_arguments = {'owner': 'Dan Collins', 'start_date': days_ago(1)}


# intialise DAG with context manager 
with DAG(
    'bigquery_data_balidation',
    schedule_interval='@daily',
    catchup=False,
    default_args=default_arguments,
    user_defined_macros={'project': 'DMC-airflow-tutorial-demo'}
) as dag:

    # use bigquery data validation operator to run simple count query 
    is_table_empty = BigQueryDataValidatorOperator(
        task_id='is_table_empty',
        sql="SELECT COUNT(*) FROM `{{ project }}.vehicle_analytics.history`", # uses the templated 'sql' field insert user defined macro 'project' 
        location="europe-west2" 
    )

    # task to see if dataset exists 
    dataset_exists = BigQueryDatasetSensor(
        task_id='dataset_exists',
        project_id='{{ project }}',
        dataset_id="vehicle_analytics"
    )


# task flow dependency 
dataset_exists >> is_table_empty 
