import logging
import pendulum
from airflow import DAG
from airflow.decorators import dag, task
from airflow.models.variable import Variable
from lib import ConnectionBuilder

from examples.cdm.deployer.settlement_report import SettlementReportLoader

log = logging.getLogger(__name__)

with DAG(
    dag_id='cdm_dag',
    schedule_interval='0/30 * * * *',
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),
    catchup=False,
    tags=['sprint5', 'cdm', 'settlement'],
    is_paused_upon_creation=False
) as dag:
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    @task(task_id="cdm_load")
    def cdm_dag():
        rest_loader = SettlementReportLoader(dwh_pg_connect)
        rest_loader.load_report_by_days()

    settlement_daily_report_load = cdm_dag()

    settlement_daily_report_load


