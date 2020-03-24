from utils import *
from config import *

if __name__ == "__main__":

    # Initialize connection database connection
    database = Database(Config())

    # Create a connection pool. Context manager ensures pool is closed at the end.
    with database.open(minconns=1) as pool:

        # Get individual connections from the pool. Context manager ensures connection [key] is returned to the pool.
        with pool.connect(key=1):

            # Pull data from database
            SQL_SELECT_FROM_TABLE = "SELECT * FROM data_container"
            results = pool.select_rows(SQL_SELECT_FROM_TABLE, key=1)

            # Convert results to a pandas DataFrame
            df = convert_to_df(results)

            # -- PROCESS DATA
            df = df.sort_values(by=['id'])
            df = df[['id']] + 5
            logger.info(f"Data processing results:\n{df.head()}")

            # Save results to PostgreSQL new table
            WRITE_BACK = False
            if WRITE_BACK:

                # COPY DataFrame to PostgreSQL table
                pool.copy_df(df, 'fiddled_data', replace=True, key=1)
