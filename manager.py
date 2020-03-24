from utils import *
from config import *
import multiprocessing
import time
import numpy as np

from streamer import LastEntryFetcher


def op1():

    while True:

        # Need to open/close file on every write so that watchdog can see each change
        with open('data/fake_data.txt', "a") as f:
            # Write current timestamp
            number = time.time()
            f.write(str(number) + '\n')
            logger.debug(f"Wrote number {number} to file")

        # Wait a variable amount of time before writing next
        time.sleep(np.random.randint(10))


def op2(pool, key):

    # Initialize the table to hold watched data
    SQL_CREATE_TABLE = "CREATE TABLE IF NOT EXISTS data_container" \
                       "(ID FLOAT PRIMARY KEY NOT NULL);"  # remember to specify primary key column
    pool.create_table(SQL_CREATE_TABLE, key=key)

    # Template SQL command to inject table with entries from file
    SQL_INSERT_IN_TABLE = "INSERT INTO data_container (ID) VALUES (%s)"  # %s will be replaced with .txt line fields

    # Create watchdog
    fido = FileWatcher(pool, SQL_INSERT_IN_TABLE, 'data/', timeout=0.5, key=key)

    # Deploy watchdog
    fido.bark()


def op3(pool, key):

    # Create your custom handler: must have a .on_notify(Database) method implemented.
    handler = LastEntryFetcher(pool, audit_table='data_container_last', key=key)
    dumbo = Listener(pool, 'table_changed', handler, key=key)

    dumbo.run()


if __name__ == "__main__":

    # Initialize connection database connection
    database = Database(Config())

    # Create a connection pool. Context manager ensures pool is closed at the end.
    with database.open(minconns=2) as pool:

        # Get individual connections from the pool. Context manager ensures connection [key] is returned to the pool.
        with pool.connect(key=1), pool.connect(key=2):

            print(multiprocessing.cpu_count())

            """
            p1 = multiprocessing.Process(target=op1, daemon=False)
            p2 = multiprocessing.Process(target=op2, args=(pool, 1), daemon=False)
            p3 = multiprocessing.Process(target=op3, args=(pool, 2), daemon=False)

            processes = [p1, p2, p3]

            for p in processes:
                p.start()

            try:
                while True:

                    # Threads working

                    pass

            except KeyboardInterrupt:
                logger.error("Manager has been stopped via Keyboard Interrupt. Attempting to close threads...")
                for p in processes:
                    p.terminate()

            finally:

                for p in processes:
                    p.join()
                logger.success("Successfully closed all threads.")
                
            """
