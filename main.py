# importing custom functions
from helpers import get_client, get_postgres_engine
from extract import fetch_data
from load import load_csv_to_postgres

query = '''
        select pickup_date, vendor_id, passenger_count, trip_distance, payment_type, fare_amount, tip_amount 
        from tripdata
        where year(pickup_date) = 2015 and month(pickup_date) = 1 and dayofMonth(pickup_date) = 1
        '''

client = get_client()
engine = get_postgres_engine

def main():
    '''
    main function to run data pipeline modules
    1.....
    2.....
    Parameters: None

    Returns: None
    '''

    # extract the data
    fetch_data(client=client, query=query)

    #load the data
    load_csv_to_postgres('tripdata.csv', 'tripdata', engine, 'STG')

    print('pipeline executed successfully')


    if __name__=='__main__':
        main()