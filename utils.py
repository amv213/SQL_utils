import io
import sys
from loguru import logger

import psycopg2
from psycopg2 import sql
from psycopg2.extras import DictCursor

from watchdog.events import PatternMatchingEventHandler

import pandas as pd
from sqlalchemy import create_engine


class Database:
    """PostgreSQL Database class."""

    def __init__(self, config):

        self.host = config.DATABASE_HOST
        self.username = config.DATABASE_USERNAME
        self.password = config.DATABASE_PASSWORD
        self.port = config.DATABASE_PORT
        self.dbname = config.DATABASE_NAME

        self.conn = None

    def open_connection(self):
        """Connect to a Postgres database."""

        if self.conn is None:

            try:

                # Connect to PostgreSQL database
                self.conn = psycopg2.connect(host=self.host,
                                             user=self.username,
                                             password=self.password,
                                             port=self.port,
                                             dbname=self.dbname)

                # Print PostgreSQL version
                with self.conn.cursor() as cur:
                    cur.execute("SELECT version();")
                    record = cur.fetchone()
                    logger.info(f"You are connected to - {record}")
                    self.conn.commit()
                    cur.close()

            except psycopg2.DatabaseError as error:
                logger.error(f"Error while connecting to PostgreSQL: {error}")
                sys.exit()

            finally:
                logger.success('Connection opened successfully.')

    def close(self):
        """Terminate a connection to PostgreSQL database"""

        if self.conn:

            self.conn.close()
            logger.success("Connection closed successfully.")

    def create_table(self, query):
        """Run a SQL query to create a table."""

        self.open_connection()

        try:

            with self.conn.cursor() as cur:
                query = cur.mogrify(query)
                cur.execute(query)
                self.conn.commit()
                cur.close()

            logger.success("Table created successfully in PostgreSQL")

        except (Exception, psycopg2.DatabaseError) as error:
            logger.error(f"Error while creating PostgreSQL table: {error}")

    def copy_table(self, query, file, db_table, replace=True):
        """Run a SQL query to copy a table to/from file."""

        self.open_connection()

        try:

            with self.conn.cursor() as cur:
                if replace:
                    cur.execute(sql.SQL("TRUNCATE {};").format(sql.Identifier(db_table)))
                query = cur.mogrify(query)
                cur.copy_expert(sql=query, file=file)
                self.conn.commit()
                cur.close()

            logger.success("Table copied successfully to/from PostgreSQL")

        except (Exception, psycopg2.DatabaseError) as error:
            logger.error(f"Error while copying PostgreSQL table: {error}")

    def copy_df(self, df, db_table, replace=True):
        """Run a SQL query to copy efficiently copy a pandas dataframe to a database table

        Inspired by:
            https://stackoverflow.com/questions/23103962/how-to-write-dataframe-to-postgres-table
        """

        # Create headless csv from pandas dataframe
        copy_from = io.StringIO()
        df.to_csv(copy_from, sep='\t', header=False, index=False)
        copy_from.seek(0)

        self.open_connection()

        try:

            # Quickly create a table with correct number of columns / data types
            replacement_method = 'replace' if replace else 'append'
            engine = create_engine('postgresql+psycopg2://', creator=lambda: self.conn)  # create engine for it to work
            df.head(0).to_sql(db_table, engine, if_exists=replacement_method, index=False)

            # But then exploit postgreSQL COPY command instead of slow pandas .to_sql()
            SQL_COPY_EXPERT = sql.SQL("COPY {} FROM STDIN WITH CSV DELIMITER '\t'").format(
                sql.Identifier(db_table))
            self.copy_table(SQL_COPY_EXPERT, copy_from, db_table, replace=False)  # False or we are going to replace ^

        except (Exception, psycopg2.DatabaseError) as error:
            logger.error(f"Error while copying DataFrame to PostgreSQL table: {error}")

    def select_rows(self, query):
        """Run a SQL query to select rows from table."""

        self.open_connection()

        try:

            with self.conn.cursor(cursor_factory=DictCursor) as cur:
                query = cur.mogrify(query)
                cur.execute(query)
                records = cur.fetchall()  # dictionaries
                self.conn.commit()
                cur.close()

            logger.success(f"Data fetched successfully from PostgreSQL:\t{cur.rowcount} rows fetched.")
            return records

        except (Exception, psycopg2.Error) as error:
            logger.error(f"Error while fetching data from PostgreSQL: {error}")

    def update_rows(self, query):
        """Run a SQL query to update rows in table."""

        self.open_connection()

        try:

            with self.conn.cursor() as cur:
                query = cur.mogrify(query)
                cur.execute(query)
                logger.success(f"Database updated successfully:\t{cur.rowcount} rows affected.")
                self.conn.commit()
                cur.close()

        except (Exception, psycopg2.Error) as error:
            logger.error(f"Error while updating data in PostgreSQL: {error}")

    def insert_rows(self, query, args=None):
        """Run a SQL query to insert rows in table.

        Args:
            query (string): SQL command string (can be template with %s fields), as required by psycopg2
            args (tuple):   tuple of args to substitute in SQL query template, as required by psycopg2
        """

        self.open_connection()

        try:

            with self.conn.cursor() as cur:
                query = cur.mogrify(query, args) if args is not None else cur.mogrify(query)
                cur.execute(query)
                logger.success(f"Record inserted successfully into database:\t{cur.rowcount} rows inserted.")
                self.conn.commit()
                cur.close()

        except (Exception, psycopg2.Error) as error:
            logger.error(f"Error executing SQL query: {error}")

    def execute_generic(self, query, args=None):
        """Run a generic SQL query"""

        self.open_connection()

        try:

            with self.conn.cursor() as cur:
                query = cur.mogrify(query, args) if args is not None else cur.mogrify(query)
                cur.execute(query)
                logger.success(f"SQL query successfully sent to database.")
                self.conn.commit()
                cur.close()

        except (Exception, psycopg2.Error) as error:
            logger.error(f"Error executing SQL query: {error}")

    def listen_on_channel(self, channel):
        """Run a LISTEN SQL query"""

        self.open_connection()

        try:

            query = "LISTEN " + channel + ";"

            with self.conn.cursor() as cur:
                query = cur.mogrify(query)
                cur.execute(query)
                logger.success(f"Successfully listening on channel {channel} for NOTIFYs.")
                self.conn.commit()
                cur.close()

        except (Exception, psycopg2.Error) as error:
            logger.error(f"Error executing SQL LISTEN query: {error}")



class InsertToSQL(PatternMatchingEventHandler):

    def __init__(self, patterns=None, ignore_patterns=None, ignore_directories=True, case_sensitive=True):

        super().__init__(patterns, ignore_patterns, ignore_directories, case_sensitive)

        self.triggered = False
        self.triggering_event = None

    # The following event_type exist:
    # 'moved', 'deleted', 'created', 'modified'

    # Here we handle only the default callback for 'modified' event
    # which will be triggered under the hood only for files matching pattern
    def on_modified(self, event):

        # And decide to only watch for file changes
        if not event.is_directory:

            # Interface with outer world
            self.triggering_event = event
            self.triggered = True
