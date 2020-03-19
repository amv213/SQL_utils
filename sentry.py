from utils import *
from config import *

from watchdog.observers.polling import PollingObserver
from pygtail import Pygtail


def process_event(event, database, query):
    """Function handling what happens to an event raised by the watchdog: here we write any file changes as new entries
    in a database table.

    Args:
        event:                  watchdog event
        database:               Database object containing the table we want to inject to
        query (string):         SQL query psycopg2 template whose %s will be filled by new file contents
    """

    logger.debug(f"Event detected: {event.event_type} \t {event.src_path}")

    # Use Pygtail to return unread (i.e.) new lines in modified file
    for line in Pygtail(event.src_path):
        # print(f"\t{line}")
        database.insert_rows(query, (line, ))  # remember tuple formatting (see psycopg2 docs)


if __name__ == "__main__":

    # Initialize connection database connection
    database = Database(Config())
    # Connect to database
    database.open_connection()

    # Initialize the table to hold watched data
    SQL_CREATE_TABLE = "CREATE TABLE IF NOT EXISTS data_container" \
                       "(ID FLOAT PRIMARY KEY NOT NULL);"  # remember to specify primary key column
    database.create_table(SQL_CREATE_TABLE)

    # -- CREATE WATCHDOG
    observer = PollingObserver(timeout=0.5)
    event_handler = InsertToSQL(patterns=["*.txt"], ignore_directories=False)

    # Schedule observer
    observer.schedule(event_handler, path='data/', recursive=True)

    # Start watchdog thread; can give it name with observer.set_name()
    observer.start()

    try:

        # Template SQL command to inject table with entries from file
        SQL_INSERT_IN_TABLE = "INSERT INTO data_container (ID) VALUES (%s)"

        while True:

            # Main Loop.
            # Watchdog is polling every TIMEOUT seconds
            if event_handler.triggered:
                process_event(event_handler.triggering_event, database, SQL_INSERT_IN_TABLE)
                event_handler.triggered = False

    except KeyboardInterrupt:
        observer.stop()
        
    finally:
        observer.join()
        database.close()
