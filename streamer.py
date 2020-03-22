"""
PostgreSQL Database Config:

    queries/audit_trigger.sql
"""

from utils import *
from config import *

# https://www.psycopg.org/articles/2010/12/01/postgresql-notifications-psycopg2-eventlet/?utm_source=twitterfeed&utm_medium=twitter


class LastEntryFetcher(NotifyHandler):
    """Custom handler which fetches las entry in a PostgreSQL audit table on receipt of a NOTIFY."""

    def __init__(self, connection, audit_table):

        super().__init__(connection)
        self.audit_table = audit_table

    def on_notify(self):
        """Echo back entry that triggered the notify"""

        SQL = sql.SQL("SELECT * FROM {};").format(sql.Identifier(self.audit_table))
        last_entry = self.connection.select_rows(SQL, fetch_method=0)
        logger.debug(f"(Last entry in table: {last_entry})")

        return 1


if __name__ == "__main__":

    # Initialize database connection, which we will use to handle transactions with the NOTIFY results
    database = Database(Config())

    with database.connect() as database_connection:

        # Create your custom handler: must have a .on_notify(Database) method implemented.
        handler = LastEntryFetcher(database_connection, audit_table='data_container_last')
        dumbo = Listener(database_connection, 'table_changed', handler)

        dumbo.run()
