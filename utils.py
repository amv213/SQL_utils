"""
https://bbengfort.github.io/observations/2017/12/06/psycopg2-transactions.html

"""

import eventlet
import io
import psycopg2
import sys

import pandas as pd

from contextlib import contextmanager
from eventlet.hubs import trampoline
from loguru import logger
from psycopg2 import sql
from psycopg2.extras import DictCursor
from psycopg2.pool import ThreadedConnectionPool
from pygtail import Pygtail
from sqlalchemy import create_engine
from watchdog.events import PatternMatchingEventHandler
from watchdog.observers.polling import PollingObserver


class Database:
    """PostgreSQL Database class."""

    def __init__(self, config):

        self.host = config.DATABASE_HOST
        self.username = config.DATABASE_USERNAME
        self.password = config.DATABASE_PASSWORD
        self.port = config.DATABASE_PORT
        self.dbname = config.DATABASE_NAME

        self.pool = None
        self.conns = {}  # active connections from the pool

    def open_pool(self, minconns=1, maxconns=None):
        """Creates a connection pool to the PostgreSQL database"""

        if self.pool is None:

            maxconns = maxconns if maxconns is not None else minconns
            self.pool = ThreadedConnectionPool(minconns, maxconns,
                                               host=self.host,
                                               user=self.username,
                                               password=self.password,
                                               port=self.port,
                                               dbname=self.dbname,
                                               sslmode='disable')

            logger.success(f"Connection pool created to PostgreSQL database: {maxconns} connections available.")

    def close_pool(self):
        """Closes all connections in the pool"""

        if self.pool:
            self.pool.closeall()
            self.pool = None
            logger.success("All connections in the pool have been closed successfully.")

    @contextmanager
    def open(self, minconns=1, maxconns=None):
        """Context manager for managing a connection pool to the database. Can then instantiate a pool as:

            with Database.open() as pool:

                ... # use pool (pool.get_connection(key))

        """
        try:
            # Create connection pool
            self.open_pool(minconns, maxconns)

            yield self

        finally:
            # Close all connections in the pool
            self.close_pool()

    @contextmanager
    def connect(self, key=1):
        """Context manager wrapper allows to call it in this fashion:

            with database.open_pool() as pool:

                with pool.get_connection(key):

                    ... # pool.send(key, *args)

        Args:
            key (int):  key to identify the connection being opened. Required for proper book keeping.

        """

        # Create connection pool
        self.get_connection(key=key)

        try:

            yield self

        except (Exception, KeyboardInterrupt) as e:
            logger.error(f"Error raised while managing connection from pool: {e}")

        finally:
            # Close all connections in the pool
            self.put_back_connection(key=key)

    def get_connection(self, key=1):
        """Connect to a Postgres database using available connection from pool.

        Args:
            key (int):  key to identify the connection being opened. Required for proper book keeping.
        """

        # If a pool has been created
        if self.pool:

            try:

                # If the specific connection hasn't been already opened
                if key not in self.conns:

                    # Connect to PostgreSQL database
                    conn = self.pool.getconn(key)
                    # add to dictionary of active connections
                    self.conns[key] = conn
                    logger.success(f"Connection retrieved successfully: pool connection [{key}] now in use.")
                    # perform connection Hello World
                    self.on_conn_retrieval(key)

                else:
                    logger.warning(f"Pool connection [{key}] is already in use by another client. Try a different key.")

            except psycopg2.pool.PoolError as error:
                logger.error(f"Error while retrieving connection from pool:\t{error}")
                sys.exit()

            except psycopg2.DatabaseError as error:
                logger.error(f"Error while connecting to PostgreSQL:\t{error}")
                sys.exit()

        else:
            logger.warning(f"No pool to the PostgreSQL database: cannot retrieve a connection. Try to .open() a pool.")

    def put_back_connection(self, key=1):
        """Put back connection to PostgreSQL database in the connection pool"""

        # If this specific connection has already been opened
        if key in self.conns:

            conn = self.conns[key]
            conn.reset()
            self.pool.putconn(conn, key)
            self.conns.pop(key)
            logger.success(f"Connection returned successfully: pool connection [{key}] now available again.")

        else:
            logger.warning(f"Pool connection [{key}] has never been opened: cannot put it back in the pool.")

    def on_conn_retrieval(self, key):
        """A small Hello World script to perform on retrieval of a PostgreSQL connection from the pool."""

        # Return connection info from database
        self.connection_info(key=key)

    def send(self, query, args, success_msg='Query Success', error_msg="Query Error", cur_method=0, file=None,
             fetch_method=2, key=1):
        """Send a generic SQL query to the Database.

        Args:
            query (string or Composed): SQL command string (can be template with %s fields), as required by psycopg2
            args (tuple or None):       tuple of args to substitute in SQL query template, as required by psycopg2
            success_msg (string):       message to log on successful execution of the SQL query
            error_msg (string):         message to log if error raised during execution of the SQL query
            cur_method (int):           code to select which psycopg2 cursor execution method to use for the SQL query:
                                        0:  cursor.execute()
                                        1:  cursor.copy_expert()
            file (file):                if cur_method == 1: a file-like object to read or write (according to sql).
            fetch_method (int):         code to select which psycopg2 result retrieval method to use (fetch*()):
                                        0: cur.fetchone()
                                        2: cur.fetchall()
            key (int):                  key to identify the connection in the pool being used for the transaction

        Returns:
            records (psycopg2.extras.DictRow): list of query results (if any). Can be accessed as dictionaries.
        """

        # If this specific connection has already been opened
        if key in self.conns:

            conn = self.conns[key]

            try:

                with conn.cursor(cursor_factory=DictCursor) as cur:
                    query = cur.mogrify(query, args) if args is not None else cur.mogrify(query)

                    # Execute query
                    if cur_method == 0:
                        cur.execute(query)
                    elif cur_method == 1:
                        cur.copy_expert(sql=query, file=file)

                    # Fetch query results
                    try:
                        if fetch_method == 0:
                            records = cur.fetchone()
                        elif fetch_method == 2:
                            records = cur.fetchall()
                    # Handle SQL queries that don't return any results
                    except psycopg2.ProgrammingError:
                        records = []
                        pass

                    conn.commit()

                    # Display success message
                    if cur.rowcount >= 0:
                        success_msg += f": {cur.rowcount} rows affected."
                    logger.success(success_msg)

                    return records  # dictionaries

            except (Exception, psycopg2.Error, psycopg2.DatabaseError) as e:
                conn.rollback()
                logger.error(error_msg + f":{e}. Transaction rolled-back.")

            # (Not sure if necessary) if conn has changed state while doing the above, update the entry in the dict
            finally:
                self.conns[key] = conn

        else:
            logger.warning(f"Pool connection [{key}] has never been opened: not available for transactions.")

    def select_rows(self, query, args=None, fetch_method=2, key=1):
        """Send a select SQL query to the Database. Expects returns."""

        success_msg = "Data fetched successfully from PostgreSQL"
        error_msg = "Error while fetching data from PostgreSQL"
        records = self.send(query, args, success_msg, error_msg, fetch_method=fetch_method, key=key)
        return records

    def update_rows(self, query, args=None, key=1):
        """Run a SQL query to update rows in table."""

        success_msg = "Database updated successfully"
        error_msg = "Error while updating data in PostgreSQL"
        self.send(query, args, success_msg, error_msg, key=key)

    def insert_rows(self, query, args=None, key=1):
        """Run a SQL query to insert rows in table."""

        success_msg = "Record inserted successfully into database"
        error_msg = "Error executing SQL query"
        self.send(query, args, success_msg, error_msg, key=key)

    def listen_on_channel(self, channel, key=1):
        """Run a LISTEN SQL query"""

        query = "LISTEN " + channel + ";"
        success_msg = f"Successfully listening on channel {channel} for NOTIFYs"
        error_msg = "Error executing SQL LISTEN query"
        self.send(query, None, success_msg, error_msg, key=key)

    def connection_info(self, key=1):
        """Run a SELECT version() SQL query"""

        query = "SELECT version();"
        success_msg = f"PostgreSQL version fetched successfully"
        error_msg = "Error while fetching PostgreSQL version"
        record = self.send(query, None, success_msg, error_msg, fetch_method=0, key=key)  # fetchone()

        logger.info(f"You are connected to - {record}")

    def create_table(self, query, args=None, key=1):
        """Run a SQL query to create a table."""

        success_msg = "Table created successfully in PostgreSQL"
        error_msg = "Error while creating PostgreSQL table"
        self.send(query, args, success_msg, error_msg, key=key)

    def copy_table(self, query, file, replace=True, db_table=None, key=1):
        """Run a SQL query to copy a table to/from file."""

        # Replace the table already existing in the database
        if replace:
            query_tmp = sql.SQL("TRUNCATE {};").format(sql.Identifier(db_table))
            success_msg = "Table truncated successfully in PostgreSQL database"
            error_msg = "Error while truncating PostgreSQL table"
            self.send(query_tmp, None, success_msg, error_msg, key=key)

        # Copy the table from file
        success_msg = "Table copied successfully to/from PostgreSQL"
        error_msg = "Error while copying PostgreSQL table"
        self.send(query, None, success_msg, error_msg, cur_method=1, file=file, key=key)

    def copy_df(self, df, db_table, replace=True, key=1):
        """Run a SQL query to copy efficiently copy a pandas dataframe to a database table

        Inspired by:
            https://stackoverflow.com/questions/23103962/how-to-write-dataframe-to-postgres-table
        """

        if key in self.conns:

            try:
                # Create headless csv from pandas dataframe
                io_file = io.StringIO()
                df.to_csv(io_file, sep='\t', header=False, index=False)
                io_file.seek(0)

                # Quickly create a table with correct number of columns / data types
                replacement_method = 'replace' if replace else 'append'
                engine = create_engine('postgresql+psycopg2://', creator=lambda: self.conns[key])  # create engine for it to work
                df.head(0).to_sql(db_table, engine, if_exists=replacement_method, index=False)

                # But then exploit postgreSQL COPY command instead of slow pandas .to_sql()
                sql_copy_expert = sql.SQL("COPY {} FROM STDIN WITH CSV DELIMITER '\t'").format(sql.Identifier(db_table))
                self.copy_table(sql_copy_expert, file=io_file, replace=False, key=key)  # need to keep the (header-only) table

                logger.success(f"DataFrame copied successfully to PostgreSQL table.")

            except (Exception, psycopg2.DatabaseError) as error:
                logger.error(f"Error while copying DataFrame to PostgreSQL table: {error}")

        else:
            logger.warning(f"Pool connection [{key}] has never been opened: cannot use it to copy Dataframe to database.")


def convert_to_df(query_results):
    """Make pandas dataframe out of SQL query results"""

    columns = [k for k in query_results[0].keys()]
    df = pd.DataFrame(query_results, columns=columns)
    logger.debug(f"Successful conversion to DataFrame:\n{df.head()}")

    return df


class InsertToSQL(PatternMatchingEventHandler):

    def __init__(self, pool, query, patterns=None, ignore_patterns=None, ignore_directories=True, case_sensitive=True, key=1):

        super().__init__(patterns, ignore_patterns, ignore_directories, case_sensitive)

        self.pool = pool
        self.key = key
        self.query = query

    # The following event_type exist:
    # 'moved', 'deleted', 'created', 'modified'

    # Here we handle only the default callback for 'modified' event
    # which will be triggered under the hood only for files matching pattern
    def on_modified(self, event):

        # And decide to only watch for file changes
        if not event.is_directory:

            # Process event (i.e send SQL)
            self.process_event(event)

    def process_event(self, event):
        """Function handling what happens to an event raised by the watchdog: here we write any file changes as new entries
        in a database table.

        Args:
            event:                  watchdog event
            database (Database):    Database object containing the table we want to inject to
            query (string):         SQL query psycopg2 template whose %s will be filled by new file contents
        """

        logger.debug(f"Event detected: {event.event_type} {event.src_path}")

        # Use Pygtail to return unread (i.e.) new lines in modified file
        for line in Pygtail(event.src_path):
            # print(f"\t{line}")
            self.pool.insert_rows(self.query, (line, ), key=self.key)  # remember tuple formatting (see psycopg2 docs)


class FileWatcher:

    def __init__(self, pool, query, src_path, patterns=None, ignore_directories=False, recursive=True, timeout=1, key=1):

        if patterns is None:
            patterns = ["*.txt"]

        self.src_path = src_path
        self.recursive = recursive
        self.event_observer = PollingObserver(timeout=timeout)
        self.event_handler = InsertToSQL(pool, query, patterns=patterns, ignore_directories=ignore_directories, key=key)

    def bark(self):

        self.start()

        try:
            while True:

                # Main Loop.
                # Watchdog is polling every TIMEOUT seconds

                pass

        except KeyboardInterrupt:
            self.stop()

    def start(self):
        # Schedule observer
        self.event_observer.schedule(self.event_handler, self.src_path, recursive=self.recursive)
        # Start watchdog thread; can give it name with observer.set_name()
        self.event_observer.start()

    def stop(self):
        self.event_observer.stop()
        self.event_observer.join()


class NotifyHandler:
    """Handler managing actions performed on reception of a NOTIFY from the database"""

    def __init__(self):

        pass

    def on_notify(self):
        """Procedure to execute once a NOTIFY is received. Overwrite as needed"""
        logger.debug(f"No actions taken on reception of NOTIFY.")
        return 1


class Listener:

    def __init__(self, pool, channel, handler, key=1):

        self.pool = pool
        self.channel = channel
        self.handler = handler
        self.key = key

    def run(self):

        queue = eventlet.Queue()  # multi-producer, multi-consumer queue that works across greenlets
        g = eventlet.spawn(self.subscribe, queue)  # spawn async greenthread in parallel

        while True:

            try:

                logger.debug(f"Waiting for a notification...")
                notify = queue.get()  # blocks until item available in queue
                # -------------%------------------%--------------------%-------------------#
                logger.success(f"Got NOTIFY: {notify.pid} {notify.channel} {notify.payload}")

                # do something once received the NOTIFY (n)
                self.handler.on_notify()

                queue.task_done()  # tell queue that this consumer has finished the task for which it asked q.get()

            except (Exception, KeyboardInterrupt):
                eventlet.kill(g)
                logger.error("Listener has been killed via Keyboard Interrupt. Greenthread garbage collected.")
                break

    def subscribe(self, q):
        """Green thread process waiting for NOTIFYs on the channel and feeding them to the queue"""

        # Subscribe to notification channel
        self.pool.listen_on_channel(self.channel, key=self.key)

        conn = self.pool.conns[self.key]
        while True:

            trampoline(conn, read=True)  # spawns a green thread and return control once there is a notification to read

            conn.poll()  # once there is a notification --> poll

            while conn.notifies:
                notify = conn.notifies.pop()  # extract notify
                q.put(notify)  # blocks until slot available in queue to insert Notify
                # -------------%------------------%--------------------%-------------------#