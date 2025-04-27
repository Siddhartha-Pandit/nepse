import csv
from datetime import datetime
from pymongo import MongoClient, errors

# Configuration
MONGO_URI = (
   "mongodb+srv://devsiddharthapandit:6uIGmpW91jslljIv@cluster0.s6bi7.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
)
DATABASE_NAME = "Nepse"  # Case-sensitive!
COLLECTION_NAME = "StockData"
CSV_FILE_PATH = "your_data.csv"
BATCH_SIZE = 5000  # Adjust based on your system's RAM (1k-10k is safe)

def convert_types(row):
    """Convert CSV string values to appropriate data types."""
    try:
        # Example conversions - modify based on your CSV schema
        row["BUSINESS_DATE"] = datetime.strptime(row["BUSINESS_DATE"], "%Y-%m-%d")
        row["OPEN_PRICE"] = float(row["OPEN_PRICE"])
        row["TOTAL_TRADED_QUANTITY"] = int(row["TOTAL_TRADED_QUANTITY"])
        # Add more fields as needed
    except (ValueError, KeyError) as e:
        print(f"⚠️ Conversion error in row: {row}\nError: {e}")
        return None
    return row

def batch_insert():
    client = MongoClient(MONGO_URI)
    db = client[DATABASE_NAME]
    collection = db[COLLECTION_NAME]

    total_inserted = 0
    batch = []

    with open(CSV_FILE_PATH, "r", encoding="utf-8") as csvfile:
        reader = csv.DictReader(csvfile)
        
        for row in reader:
            # Data type conversion
            processed_row = convert_types(row)
            if not processed_row:
                continue  # Skip invalid rows

            batch.append(processed_row)

            if len(batch) >= BATCH_SIZE:
                try:
                    result = collection.insert_many(batch, ordered=False)
                    total_inserted += len(result.inserted_ids)
                    batch = []
                    print(f"✅ Inserted {total_inserted} rows so far...")
                except errors.BulkWriteError as e:
                    print(f"❌ Batch error: {e.details['writeErrors'][0]['errmsg']}")
                    batch = []

        # Insert remaining documents in the final batch
        if batch:
            try:
                result = collection.insert_many(batch, ordered=False)
                total_inserted += len(result.inserted_ids)
            except errors.BulkWriteError as e:
                print(f"❌ Final batch error: {e.details}")

    print(f"✨ Total documents inserted: {total_inserted}")
    client.close()

if __name__ == "__main__":
    batch_insert()