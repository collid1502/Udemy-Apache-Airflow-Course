# Test DAGs

conda activate <your-virtual-env>

export AIRFLOW_HOME=/Users/Dan/airflow 

cd users/Dan/airflow/dags 

# to run the dag loader test, you just need to run the dag file as an oridinary python file 
python core_conecpts.py 

<<c
If no errors are thrown, it means there were no syntax errors or uninstalled dependencies in your DAG file 
c
 
# to test an indiviual task within a DAG, you can pass the DAG id & the task id like so:
# and provide the execution date for this DAG 
airflow test core_conecpts bash_command 2020-05-28 

# ---------------------------------------------------------------

# you can use <below> to list active DAG & sub-DAG IDs 
airflow list_dags 


# you can use <below> for tasks in a DAG (providing the DAG id) 
airflow list_tasks bigquery_data_analytics 