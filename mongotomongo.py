from pymongo import MongoClient, InsertOne, UpdateOne
import threading
import multiprocessing

# Define your MongoDB connection settings
mongodb_uri = 'mongodb://localhost:27017/'
database_name = 'your_database_name'
source_collection_name = 'your_source_collection'
target_collection_name = 'your_target_collection'

# Define the number of threads/processes to use (It will be automatically adjusted based on CPU cores)
num_threads_or_processes = multiprocessing.cpu_count()

# Define the chunk size for processing
chunk_size = 1000

# Function to process and update/insert documents
def process_documents(offset):
    client = MongoClient(mongodb_uri)
    source_collection = client[database_name][source_collection_name]
    target_collection = client[database_name][target_collection_name]
    documents = source_collection.find({}).skip(offset).limit(chunk_size)

    bulk_operations = []
    for document in documents:
        accnt_nmbr = document['accnt_nmbr']
        prdcd = document['prdcd']
        col1 = document['col1']
        col2 = document['col2']

        # Create the product document to be inserted or updated
        product = {
            'prdcd': prdcd,
            'col1': col1,
            'col2': col2
        }

        # Build the bulk write operation based on 'accnt_nmbr'
        bulk_operation = UpdateOne(
            {'accnt_nmbr': accnt_nmbr},
            {'$push': {'product': product}},
            upsert=True  # If document doesn't exist, insert it
        )
        bulk_operations.append(bulk_operation)

    # Execute the bulk write operation
    if bulk_operations:
        target_collection.bulk_write(bulk_operations)

    client.close()

# Function to distribute workload to threads/processes
def process_in_parallel():
    client = MongoClient(mongodb_uri)
    source_collection = client[database_name][source_collection_name]
    total_documents = source_collection.count_documents({})

    # Calculate the number of iterations and offsets based on chunk size
    num_iterations = (total_documents + chunk_size - 1) // chunk_size
    offsets = [i * chunk_size for i in range(num_iterations)]

    # Use either threading or multiprocessing based on the number of threads/processes specified
    if num_threads_or_processes > 1:
        workers = []
        for offset in offsets:
            worker = threading.Thread(target=process_documents, args=(offset,))
            workers.append(worker)

        for worker in workers:
            worker.start()

        for worker in workers:
            worker.join()
    else:
        with multiprocessing.Pool() as pool:
            pool.map(process_documents, offsets)

    client.close()

if __name__ == "__main__":
    process_in_parallel()
