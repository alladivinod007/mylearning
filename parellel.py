import pymongo
import pymysql
from pymongo import UpdateOne
from concurrent.futures import ThreadPoolExecutor

# Function to fetch column information from SQL Server table
def fetch_sql_column_info(sql_server_params, query):
    conn = pymysql.connect(**sql_server_params)
    cursor = conn.cursor(pymysql.cursors.DictCursor)

    cursor.execute(query)
    column_info = cursor.fetchall()

    cursor.close()
    conn.close()

    return column_info

# Function to fetch data from SQL Server
def fetch_data_from_sql(sql_server_params, query, start_id, chunk_size):
    conn = pymysql.connect(**sql_server_params)
    cursor = conn.cursor(pymysql.cursors.DictCursor)

    query_with_limits = f"{query} WHERE accnt_nmbr >= {start_id} LIMIT {chunk_size}"
    cursor.execute(query_with_limits)
    data = cursor.fetchall()

    cursor.close()
    conn.close()

    return data

# Function to process a chunk of data
def process_chunk(chunk_data, column_info, mongo_params):
    client = pymongo.MongoClient(**mongo_params)
    db = client['your_database']
    collection = db['loadData']

    bulk_operations = []
    for item in chunk_data:
        accnt_nmbr = item['accnt_nmbr']
        product_info = {'accnt_nmbr': accnt_nmbr}
        for column in column_info:
            if column['Field'] != 'accnt_nmbr':
                product_info[column['Field']] = item[column['Field']]

        filter_query = {'accnt_nmbr': accnt_nmbr}
        update_query = {'$set': {'product': product_info}}
        update_one = UpdateOne(filter_query, update_query, upsert=True)
        bulk_operations.append(update_one)

    if bulk_operations:
        collection.bulk_write(bulk_operations)

    client.close()

# Function to load data in chunks using ThreadPoolExecutor
def load_data_with_threads(sql_server_params, query, chunk_size, mongo_params, max_threads=5):
    column_info = fetch_sql_column_info(sql_server_params, query)
    total_records = fetch_total_records(sql_server_params, query)

    with ThreadPoolExecutor(max_workers=max_threads) as executor:
        futures = []
        for i in range(0, total_records, chunk_size):
            data = fetch_data_from_sql(sql_server_params, query, i, chunk_size)
            future = executor.submit(process_chunk, data, column_info, mongo_params)
            futures.append(future)

        for future in futures:
            future.result()

# Function to fetch total records from SQL Server
def fetch_total_records(sql_server_params, query):
    conn = pymysql.connect(**sql_server_params)
    cursor = conn.cursor()

    cursor.execute(f"SELECT COUNT(*) FROM ({query}) AS total")
    total_records = cursor.fetchone()[0]

    cursor.close()
    conn.close()

    return total_records

if __name__ == '__main__':
    sql_server_params = {
        'host': 'your_sql_server_host',
        'user': 'your_username',
        'password': 'your_password',
        'database': 'your_database'
    }
    query = 'SELECT * FROM your_table_name'
    chunk_size = 1000  # Adjust the chunk size as needed
    mongo_params = {
        'host': 'your_mongodb_uri'
    }
    max_threads = 5  # Adjust the maximum number of threads as needed

    load_data_with_threads(sql_server_params, query, chunk_size, mongo_params, max_threads)
