# airflowRedditPysparkDag.py
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta
import os

sparkSubmit = '/home/centos/spark/spark-2.3.1-bin-hadoop2.7/bin/spark-submit'

## Define the DAG object
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2018, 9, 24),
    'retries': 5,
    'retry_delay': timedelta(minutes=1),
}
dag = DAG('sparktest1', default_args=default_args, schedule_interval=timedelta(1))


export_config = BashOperator(
    task_id='export-config',
    depends_on_past=False,
    bash_command='export KUBECONFIG=/etc/kubernetes/admin.conf',
    dag=dag)


#task to compute number of unique authors
numUniqueAuthors = BashOperator(
    task_id='run-spark-job1',
    bash_command=sparkSubmit + ' ' + '--master k8s://https://10.11.100.232:6443 \
    --deploy-mode cluster \
	--name spark-pi \
	--class org.apache.spark.examples.SparkPi \
	--conf spark.executor.instances=2 \
	--conf spark.app.name=spark-pi \
	--conf spark.kubernetes.authenticate.driver.serviceAccountName=spark \
	--conf spark.kubernetes.container.image=index.docker.io/chandanpal06/spark:latest \
        local:///opt/spark/examples/jars/spark-examples_2.11-2.3.1.jar',
    dag=dag)
#Specify that this task depends on the export_config task
numUniqueAuthors.set_upstream(export_config)

