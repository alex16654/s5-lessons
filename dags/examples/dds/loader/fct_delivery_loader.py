from lib import PgConnect
from logging import Logger

from examples.dds.loader.sql_for_fct_delivery_load import SQL_FOR_FCT_DELIVERY_LOAD


class BaseRepository:
    def __init__(self, pg: PgConnect, sql_update:str) -> None:
        self._sql_update = sql_update
        self._db = pg

    def load_delivery(self) -> None:
        with self._db.client() as conn:
            with conn.cursor() as cur:
                cur.execute(self._sql_update)

class DeliveryLoad:
    def __init__(self, pg: PgConnect, log: Logger) -> None:
        self.repository = BaseRepository(pg, SQL_FOR_FCT_DELIVERY_LOAD)
        self.log = log

    def fct_delivery_load(self):
        self.repository.load_delivery()
