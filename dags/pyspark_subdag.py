"""
Here, we create a python function, that will return a DAG object

in this case, our sub-dag! 
"""

from airflow import DAG 
from airflow.contrib.operators.dataproc_operator import DataProcPySparkOperator


def weekday_subdag(parent_dag=None, task_id=None, schedule_interval=None, default_args=None):
    """
    Python function to build a sub-dag to run within our main DAG (bigquery_data_analytics) on airflow 
    """
    subdag = DAG(
        f'{parent_dag}.{task_id}',  # aka setting the name as parent_dag_id.sub_dag_id  
        schedule_interval=schedule_interval,
        default_args=default_args,
    )

    pyspark_files = ['avg_speed', 'avg_temperature', 'avg_tire_pressure'] 

    for file in pyspark_files:
        # create a task per PySpark file in the bucket location on GCP - then add it into the `subdag` DAG created above 
        DataProcPySparkOperator(
            task_id=f'{file}',
            main=f'gs://dmc-logistics-spark-bucket/pyspark/weekday/{file}.py',
            cluster_name='spark-cluster-{{ ds_nodash }}',   
            dataproc_pyspark_jars='gs://spark-lib/bigquery/spark-bigquery-latest.jar',
            dag=subdag,  # assigns this task to the DAG called `subdag` 
        )

    return subdag      # simply returning the DAG `subdag` once we have built it with the individual tasks 
