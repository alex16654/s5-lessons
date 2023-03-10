from lib import PgConnect
from examples.cdm.loader.queries_settlement import SETTLEMENT_REPORT


class BaseRepository:
    def __init__(self, pg: PgConnect, sql_update:str) -> None:
        self._sql_update = sql_update
        self._db = pg

    def load_settlement_by_days(self) -> None:
        with self._db.client() as conn:
            with conn.cursor() as cur:
                cur.execute(self._sql_update)
                conn.commit()

class SettlementLoader:
    def __init__(self, pg: PgConnect) -> None:
        self.repository = BaseRepository(pg, SETTLEMENT_REPORT)

    def load_report_by_days(self):
        self.repository.load_settlement_by_days()
