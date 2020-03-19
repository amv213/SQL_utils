from utils import *
from config import *
import eventlet
from eventlet.hubs import trampoline

# https://www.psycopg.org/articles/2010/12/01/postgresql-notifications-psycopg2-eventlet/?utm_source=twitterfeed&utm_medium=twitter


def dblisten(channel, q):

    # Initialize connection database connection
    database = Database(Config())
    # Connect to database
    database.open_connection()

    # Subscribe to notification channel
    database.listen_on_channel(channel)

    conn = database.conn
    while True:

        trampoline(conn, read=True)  # this will spawn a green thread and return control once there is a notification

        conn.poll()  # once there is, poll
        while conn.notifies:

            notify = conn.notifies.pop()
            q.put(notify)  # blocks until slot available in queue to insert Notify


if __name__ == "__main__":

    # Initialize database connection, which we will use to handle transactions with the NOTIFY results
    database = Database(Config())
    # Connect to database
    database.open_connection()
    # Listen for triggers on this database channel
    channel_name = 'table_changed'

    try:

        q = eventlet.Queue()  # multi-producer, multi-consumer queue that works across greenlets
        eventlet.spawn(dblisten, channel_name, q)  # spawn async greenthread in parallel

        while True:
            logger.debug(f"Waiting for a notification...")
            notify = q.get()  # blocks until item available in queue
            logger.success(f"Got NOTIFY: {notify.pid} {notify.channel} {notify.payload}")

            # do something once received the NOTIFY (n)

            SQL = 'SELECT * FROM data_container_last;'
            last_entry = database.select_rows(SQL)
            logger.info(f"Last entry in table: {last_entry}")

            q.task_done()  # tell queue that this consumer has finished the task for which it asked q.get()

    except KeyboardInterrupt:
        logger.error("Streamer has been stopped via Keyboard Interrupt.")

    finally:
        # conn.close()
        logger.success("Connection closed successfully")
