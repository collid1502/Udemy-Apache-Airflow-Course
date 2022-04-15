"""
## Example PySpark DAG 

This DAG covers an example of running PySpark jobs on GCP 

DAG High Lvl design 
- create a cluster >> check if weekday or weekend 
- IF weekday >> process weekday pyspark script
- IF weekend >> process weekend pyspark script 
- processed script >> delete cluster 
"""

from airflow import DAG 
from airflow.contrib.operators.dataproc_operator import (
    DataprocClusterCreateOperator,
    DataProcPySparkOperator,
    DataprocClusterDeleteOperator,
)

from airflow.operators.subdag_operator import SubDagOperator 
from airflow.utils.dates import days_ago 
from airflow.operators.python_operator import BranchPythonOperator 
from datetime import datetime 

from pyspark_subdag import weekday_subdag

default_arguments = {'owner': 'Dan Collins', 'start_date': days_ago(1)} 

# create a python function to check if the run date is a weekday or weekend 
def assess_day(execution_date=None):
    date = datetime.strptime(execution_date, '%Y-%m-%d') 
    if date.isoweekday() < 6:         # returns a number between 1 & 7 for the weekday. 1 being Monday, 7 being Sunday 
        return 'weekday_analytics'    # will be the task_id for the weekday pyspark script 
    else:
        return 'weekend_analytics'    # will be the task_id for the weekend pyspark script 


with DAG(
    'bigquery_data_analytics',
    schedule_interval='0 20 * * *',
    catchup=False,
    default_args=default_arguments
) as dag:

    # specify markdown which allows docstrings at the top to show in the UI 
    dag.doc_md = __doc__ 

    # next, use an operator to create a Data Proc Cluster 
    create_cluster = DataprocClusterCreateOperator(
        task_id='create_cluster',
        project_id='DMC-airflow-tutorial-demo',
        cluster_name='spark-cluster-{{ ds_nodash }}',   # uses a macro template to create a cluster name using timestamp 
        num_workers=2,
        storage_bucket='dmc-logistics-spark-bucket',
        zone='europe-west2-a'     # sets zone region. The cluster & bucket should be same regions  
    )

    # to create documentation for a Task in the UI, you can do
    create_cluster.doc_md = """ ## Create a Dataproc Cluster on GCP
    A cluster is created, to then process our PySpark jobs 
    """

    # determines the branch the DAG should follow based on a python function to determine weekday 
    weekday_or_weekend = BranchPythonOperator(
        task_id='weekday_or_weekend',
        python_callable=assess_day,
        op_kwargs={'execution_date': '{{ ds }}'}  # uses the jinja macro 'ds' which is the datastamp of execution 
    )

    # create a task for the weekend PySpark script 
    weekend_analytics = DataProcPySparkOperator(
        task_id='weekend_analytics',
        main='gs://dmc-logistics-spark-bucket/pyspark/weekend/gas_composition_count.py',   # this will be the bucket path to the PySpark Python file that it needs to process
        cluster_name='spark-cluster-{{ ds_nodash }}',   # cluster we would like to run this job on
        dataproc_pyspark_jars='gs://spark-lib/bigquery/spark-bigquery-latest.jar',  # finally we need to specify a PySpark jar file. Makes BQ connector available to use in PySpark
    )

    # create a task for the weekday PySpark scripts
    # because we have multiple scripts to execute, we can actually make use of a "sub-dag"
    # this approach is to have a dag within the main dag 
    # they have a strict naming rule of:  parent_dag_id.sub_dag_id 
    # WARNING - can cause deadlocks 
    weekday_analytics = SubDagOperator(
        task_id='weekday_analytics',
        subdag=weekday_subdag(
            parent_dag='bigquery_data_analytics',
            task_id='weekday_analytics',
            schedule_interval='0 20 * * *',
            default_args=default_arguments,
        )
    )

    # create a task to delete the cluster 
    delete_cluster = DataprocClusterDeleteOperator(
        task_id='delete_cluster',
        project_id='"DMC-airflow-tutorial-demo',
        cluster_name='spark-cluster-{{ ds_nodash }}',
        trigger_rule='all_done' 
    )



# task dependencies flow 
"""
Because usually dependent flows only execute if all upstream tasks succeed, we need to use trigger rules (all_success is by default) 
This is because, either weekday or weekend analytics task will not operate on a given day, one must be skipped

we will use the 'all_done' trigger option. Which triggers a task when all parents are done with execution 
This means, even if a failure, the task will still execute. This is good, as it will remove our cluster regardless, & not waste resources 

The custom trigger rule is added to the `delete_cluster` task above 
"""
create_cluster >> weekday_or_weekend >> [weekend_analytics, weekday_analytics] >> delete_cluster 
