from pymongo import MongoClient

# MongoDB Connection Details
mongo_client = MongoClient('localhost', 27017)
mongo_db = mongo_client['your_mongodb_database_name']
mongo_collection = mongo_db['your_mongodb_collection_name']

def update_or_insert(accnt_nmbr, prdcd):
    find_query = {"accnt_nmbr": accnt_nmbr}

    # Create the update operation with $cond and $filter to remove existing product with the same prdcd
    update_query = {
        "$set": {
            "products": {
                "$cond": {
                    "if": {"$in": [prdcd, "$products.prdcd"]},
                    "then": {"$concatArrays": [
                        {"$filter": {
                            "input": "$products",
                            "as": "item",
                            "cond": {"$ne": ["$$item.prdcd", prdcd]}
                        }},
                        [{"prdcd": prdcd, "col1": "abc"}]
                    ]},
                    "else": {"$concatArrays": ["$products", [{"prdcd": prdcd, "col1": "abc"}]]}
                }
            }
        }
    }

    # Use upsert=True to insert if not found
    mongo_collection.update_one(find_query, update_query, upsert=True)

# Example usage
accnt_nmbr = 12345
prdcd = "ds1"
update_or_insert(accnt_nmbr, prdcd)
