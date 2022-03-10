from airflow.models import DAG
from airflow.utils.dates import days_ago
from airflow.operators.dummy_operator import DummyOperator
from datetime import timedelta
from airflow.models.baseoperator import chain
from anomalo_runchecks import AnomaloRunCheck
from anomalo_passfail import AnomaloPassFail

args = {
    'owner': 'AL',
    'start_date': days_ago(1),
}

with DAG(
    dag_id='AnomaloDAG',
    default_args=args,
    description='Simple Anomalo Airflow operator example',
    schedule_interval='@daily' 
) as dag:

    ingest_transform_data = DummyOperator(
        task_id='ingest_transform_data'
    )

    anomalo_run = AnomaloRunCheck(
        task_id='AnomaloRunCheck',
        tablename='public-bq.crypto_bitcoin.outputs',
    )

    anomalo_validate = AnomaloPassFail(
        task_id='AnomaloPassFail',
        tablename='public-bq.crypto_bitcoin.outputs',
        mustpass=['data_freshness','data_volume','metric','rule','missing_data','anomaly']
    )

    publish_data = DummyOperator(
        task_id='publish_data'
    )   

    ingest_transform_data >> anomalo_run >> anomalo_validate >> publish_data