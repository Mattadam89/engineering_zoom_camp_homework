import pandas as pd
import os
from time import time
from sqlalchemy import create_engine

## Assign environment variables that are defined in the .env file
user = os.environ["USER"]
password = os.environ["PASSWORD"]
host = os.environ["HOST"]
port = os.environ["PORT"]
db = os.environ["DB"]
table_name = os.environ["TABLE_NAME"]
url = os.environ["URL"]


def main():
    """
    script to download a csv from a url, read that csv into pandas,
    connect to postgres database and write the data to that database in
    chunks
    """

    csv_name = 'output_green.csv'

    ## download the csv and output to the string assignd to csv_name
    os.system(f"wget {url} -O {csv_name}")

    ## connection to the postgres database
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')


    ## Read the data from the csv into a data frame as an iterator for chunking
    ## the data into the table
    df_iter = pd.read_csv(csv_name,
                          compression='gzip', 
                          iterator = True, 
                          chunksize = 100000)


    ## Write the data from the data frame into the postgres table in the chunks
    ## defined. Use some variables and print statements to keep track of what
    ## is happening

    ## variable to keep track of sum of rows written to postgress
    r_sum = 0 

    ## variable to keep track of total time taken
    t_total = 0 

    for df in df_iter:
        t_start = time() ## start time

        df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
        df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)

        ## write the dataframe to the postgres database
        df.to_sql(name = table_name, 
                con = engine, 
                if_exists = 'append',
                index = False)   
        ## add number of rows in chunk to sum
        r_sum += len(df) 

        ## end time
        t_end = time() 

        ## time taken to insert chunk
        t_taken = t_end - t_start

        ## add time taken to write chunk to total
        t_total += t_taken 
        print(f"Chunk of {len(df)} rows added... it took {t_taken:.2f} seconds")


        
    print (f"\n{r_sum} rows added to the table. " 
        f"This operation took {t_total:.2f} seconds.")


main()
