from flask import Flask, jsonify
from pymongo import MongoClient
import concurrent.futures

app = Flask(__name__)

# MongoDB connection
client = MongoClient('mongodb://localhost:27017')
db = client['your_database']

# Define the aggregation pipelines for each collection
pipelines = {
    'collection1': [
        {"$match": {"field": "value1"}},
        {"$limit": 10}
    ],
    'collection2': [
        {"$match": {"field": "value2"}},
        {"$limit": 10}
    ],
    'collection3': [
        {"$match": {"field": "value3"}},
        {"$limit": 10}
    ],
    'collection4': [
        {"$match": {"field": "value4"}},
        {"$limit": 10}
    ],
    'collection5': [
        {"$match": {"field": "value5"}},
        {"$limit": 10}
    ]
}

# Define a function to execute the pipeline for a collection
def execute_pipeline(collection_name, pipeline):
    result = list(db[collection_name].aggregate(pipeline))
    return result

# Define a route to perform parallel matching
@app.route('/match_parallel')
def match_parallel():
    matched_records = []
    
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = []
        
        for collection_name, pipeline in pipelines.items():
            future = executor.submit(execute_pipeline, collection_name, pipeline)
            futures.append(future)
        
        for future in concurrent.futures.as_completed(futures):
            matching_records = future.result()
            matched_records.extend(matching_records)
            
            if len(matched_records) >= 10:
                break
    
    return jsonify(matched_records[:10])  # Limit to 10 documents

# Run the Flask app
if __name__ == '__main__':
    app.run()
