from utils import *
from config import *

import numpy as np
import pandas as pd

if __name__ == "__main__":

    # Initialize connection database connection
    database = Database(Config())
    # Connect to database
    database.open_connection()

    # Pull data from database
    SQL_SELECT_FROM_TABLE = "SELECT * FROM servo_data LIMIT 50"
    
    results = database.select_rows(SQL_SELECT_FROM_TABLE)  # returns list of psycopg2 dicts

    # Make pandas dataframe out of SQL query results
    columns = [k for k in results[0].keys()]
    df = pd.DataFrame(results, columns=columns)
    logger.debug(f"Data fetched:\n{df.head()}")

    # -- PROCESS DATA
    df = df.sort_values(by=['unix timestamp'])
    df = df[['unix timestamp', 'excitation freq']]

    # Save results to PostgreSQL
    WRITE_BACK = True
    if WRITE_BACK:

        # Initialize the table to hold processed data
        database.copy_df(df, 'fiddled_data', replace=True)

    # Close connection
    database.close()
