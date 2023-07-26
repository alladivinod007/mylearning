import threading
import pymysql
import pymongo

# Define the chunk size
chunk_size = 100000

# Connect to SQL Server
conn = pymysql.connect(server='localhost', database='mydb', user='sa', password='mypassword')

# Create a cursor
cursor = conn.cursor()

# Create a list of threads
threads = []

# Iterate over the data in chunks
for i in range(0, len(cursor), chunk_size):
  # Get the chunk of data
  chunk = cursor.fetchmany(chunk_size)

  # Create a thread to process the chunk
  def process_chunk(chunk):
    # Connect to MongoDB
    client = pymongo.MongoClient('localhost', 27017)
    db = client['mydb']
    collection = db['polymorphic_dataset']

    # Iterate over the chunk and insert/update the data in MongoDB
    for row in chunk:
      document = {
        'accntnmbr': row[0],
        'product': [
          {
            'prdcd': row[1],
            'col1': 'ABC',
            'col2': 'tyu',
            'accntnmbr': row[0]
          }
        ]
      }

      # Check if the document already exists in MongoDB
      doc_exists = collection.find_one({'accntnmbr': row[0]})
      if doc_exists:
        # Update the document
        collection.update_one({'accntnmbr': row[0]}, {'$set': document})
      else:
        # Insert the document
        collection.insert_one(document)

  # Start the thread
  thread = threading.Thread(target=process_chunk, args=(chunk,))
  threads.append(thread)

# Start all the threads
for thread in threads:
  thread.start()

# Wait for all the threads to finish
for thread in threads:
  thread.join()

# Close the connection to SQL Server
cursor.close()
conn.close()
