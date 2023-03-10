from lib import PgConnect
from logging import Logger

from examples.cdm.loader.queries_sql_courier_ledger import SQL_COURIER_LEDGER


class BaseRepository:
    def __init__(self, pg: PgConnect, sql_update:str) -> None:
        self._sql_update = sql_update
        self._db = pg

    def load_courier_ledger(self) -> None:
        with self._db.client() as conn:
            with conn.cursor() as cur:
                cur.execute(self._sql_update)

class CourierLedgerLoad:
    def __init__(self, pg: PgConnect, log: Logger) -> None:
        self.repository = BaseRepository(pg, SQL_COURIER_LEDGER)
        self.log = log

    def load_courier_ledger(self):
        self.repository.load_courier_ledger()
