from utils import *
from config import *

if __name__ == "__main__":

    # Initialize connection database connection
    database = Database(Config())

    # Context manager to properly open/close a single database connection
    with database.connect() as database_connection:

        # Initialize the table to hold watched data
        SQL_CREATE_TABLE = "CREATE TABLE IF NOT EXISTS data_container" \
                           "(ID FLOAT PRIMARY KEY NOT NULL);"  # remember to specify primary key column
        database_connection.create_table(SQL_CREATE_TABLE)

        # Template SQL command to inject table with entries from file
        SQL_INSERT_IN_TABLE = "INSERT INTO data_container (ID) VALUES (%s)"  # %s will be replaced with .txt line fields

        # Create watchdog
        fido = FileWatcher(database_connection, SQL_INSERT_IN_TABLE, 'data/', timeout=0.5)

        # Deploy watchdog
        fido.bark()
