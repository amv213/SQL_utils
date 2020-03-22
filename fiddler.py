from utils import *
from config import *

if __name__ == "__main__":

    # Initialize connection database connection
    database = Database(Config())

    with database.connect() as database_connection:

        # Pull data from database
        SQL_SELECT_FROM_TABLE = "SELECT * FROM data_container LIMIT 50"
        results = database_connection.select_rows(SQL_SELECT_FROM_TABLE)

        # Convert results to a pandas DataFrame
        df = convert_to_df(results)

        # -- PROCESS DATA
        df = df.sort_values(by=['id'])
        df = df[['id']] + 5
        logger.info(f"Data processing results {df.head()}")

        # Save results to PostgreSQL new table
        #WRITE_BACK = True
        #if WRITE_BACK:

            # COPY DataFrame to PostgreSQL table
            # database_connection.copy_df(df, 'fiddled_data', replace=True)
