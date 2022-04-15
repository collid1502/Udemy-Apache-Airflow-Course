cat $AIRFLOW_HOME/airflow-webserver.pid | xargs kill -9

cat /dev/null >  $AIRFLOW_HOME/airflow-webserver.pid

# can now run "airflow webserver" in a new terminal 