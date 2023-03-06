import logging
import pendulum
from airflow import DAG
from airflow.models.variable import Variable
from airflow.decorators import task
from lib import ConnectionBuilder

from examples.dds.deployer.user_loader import UserLoader
from examples.dds.deployer.restaurant_loader import RestaurantLoader
from examples.dds.deployer.timestamp_loader import TimestampLoader
from examples.dds.deployer.product_loader import ProductLoader
from examples.dds.deployer.order_loader import OrderLoader
from examples.dds.deployer.fct_products_sales_loader import FctProductsLoader
from examples.dds.dds_settings_repository import DdsEtlSettingsRepository

log = logging.getLogger(__name__)

with DAG(
    dag_id='dds_dag',
    schedule_interval='0/30 * * * *',
    start_date=pendulum.datetime(2023, 1, 31, tz="UTC"),
    catchup=False,
    tags=['sprint5', 'dds', 'schema', 'ddl'],
    is_paused_upon_creation=False
) as dag:
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")
    settings_repository = DdsEtlSettingsRepository()


    @task(task_id="dm_users_load")
    def load_dm_users(ds=None, **kwargs):
        user_loader = UserLoader(dwh_pg_connect, settings_repository)
        user_loader.load_users()

    @task(task_id="dm_restaurants_load")
    def load_dm_restaurants(ds=None, **kwargs):
        rest_loader = RestaurantLoader(dwh_pg_connect, settings_repository)
        rest_loader.load_restaurants()


    @task(task_id="dm_timestamps_load")
    def load_dm_timestamps(ds=None, **kwargs):
        ts_loader = TimestampLoader(dwh_pg_connect, settings_repository)
        ts_loader.load_timestamps()


    @task(task_id="dm_products_load")
    def load_dm_products(ds=None, **kwargs):
        prod_loader = ProductLoader(dwh_pg_connect, settings_repository)
        prod_loader.load_products()


    @task(task_id="dm_orders_load")
    def load_dm_orders(ds=None, **kwargs):
        order_loader = OrderLoader(dwh_pg_connect, settings_repository)
        order_loader.load_orders()


    @task(task_id="fct_order_products_load")
    def load_fct_order_products(ds=None, **kwargs):
        fct_loader = FctProductsLoader(dwh_pg_connect, settings_repository)
        fct_loader.load_product_facts()


    dm_users = load_dm_users()
    dm_restaurants = load_dm_restaurants()
    dm_timestamps = load_dm_timestamps()
    dm_products = load_dm_products()
    dm_orders = load_dm_orders()
    fct_order_products = load_fct_order_products()

    dm_restaurants >> dm_products
    dm_restaurants >> dm_orders
    dm_timestamps >> dm_orders
    dm_users >> dm_orders
    dm_products >> fct_order_products
    dm_orders >> fct_order_products

    #dm_users, dm_restaurants >> dm_products, dm_timestamps
