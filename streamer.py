"""
PostgreSQL Database Config:

    queries/audit_trigger.sql
"""

from utils import *
from config import *

# https://www.psycopg.org/articles/2010/12/01/postgresql-notifications-psycopg2-eventlet/?utm_source=twitterfeed&utm_medium=twitter


class LastEntryFetcher(NotifyHandler):
    """Custom handler which fetches las entry in a PostgreSQL audit table on receipt of a NOTIFY."""

    def __init__(self, pool, audit_table, key=1):

        super().__init__()

        self.pool = pool
        self.audit_table = audit_table
        self.key = key

    def on_notify(self):
        """Echo back entry that triggered the notify"""

        SQL = sql.SQL("SELECT * FROM {};").format(sql.Identifier(self.audit_table))
        last_entry = self.pool.select_rows(SQL, fetch_method=0, key=self.key)
        logger.debug(f"(Last entry in table: {last_entry})")

        return 1


if __name__ == "__main__":

    # Initialize database connection, which we will use to handle transactions with the NOTIFY results
    database = Database(Config())

    # Create a connection pool. Context manager ensures pool is closed at the end.
    with database.open(minconns=1) as pool:

        # Get individual connections from the pool. Context manager ensures connection [key] is returned to the pool.
        with pool.connect(key=1):

            # Create your custom handler: must have a .on_notify(Database) method implemented.
            handler = LastEntryFetcher(pool, audit_table='data_container_last', key=1)
            dumbo = Listener(pool, 'table_changed', handler, key=1)

            dumbo.run()
