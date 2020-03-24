from utils import *
from config import *

if __name__ == "__main__":

    # Initialize database
    database = Database(Config())

    # Create a connection pool. Context manager ensures pool is closed at the end.
    with database.open(minconns=1) as pool:

        # Get individual connections from the pool. Context manager ensures connection [key] is returned to the pool.
        with pool.connect(key=1):

            # Initialize the table to hold watched data
            SQL_CREATE_TABLE = "CREATE TABLE IF NOT EXISTS data_container" \
                                       "(ID FLOAT PRIMARY KEY NOT NULL);"  # remember to specify primary key column
            pool.create_table(SQL_CREATE_TABLE, key=1)

            # Template SQL command to inject table with entries from file
            SQL_INSERT_IN_TABLE = "INSERT INTO data_container (ID) VALUES (%s)"  # %s will be replaced with .txt line fields

            # Create watchdog
            fido = FileWatcher(pool, SQL_INSERT_IN_TABLE, 'data/', timeout=0.5, key=1)

            # Deploy watchdog
            fido.bark()

