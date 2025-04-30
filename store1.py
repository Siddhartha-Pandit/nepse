import csv
from pymongo import MongoClient, errors
from datetime import datetime
import ssl

# Print OpenSSL version for debugging TLS
print("OpenSSL version:", ssl.OPENSSL_VERSION)

# MongoDB configuration
MONGO_URI = (
    "mongodb+srv://devsiddharthapandit:6uIGmpW91jslljIv"
    "@cluster0.s6bi7.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
)
DATABASE = "Nepse"
COLLECTION = "dailyprice"
CSV_PATH = "12.csv"

# How many documents per batch insert
BATCH_SIZE = 500  # tune between 100–1000 for optimal throughput :contentReference[oaicite:0]{index=0}

def test_mongo_connection(uri: str, timeout_ms: int = 5000) -> bool:
    """
    Attempts to connect to MongoDB over TLS/SSL and run a ping command.
    Returns True if successful, False otherwise.
    """
    try:
        client = MongoClient(
            uri,
            serverSelectionTimeoutMS=timeout_ms,
            tls=True,
        )
        client.admin.command("ping")
        print("✅ Successfully connected to MongoDB.")
        return True
    except errors.ServerSelectionTimeoutError as err:
        print(f"❌ Connection timed out: {err}")
    except errors.ConfigurationError as err:
        print(f"❌ Configuration error: {err}")
    except Exception as err:
        print(f"❌ Unexpected error: {err}")
    return False

def safe_float(val):
    try:
        return float(val)
    except (ValueError, TypeError):
        return None

def safe_int(val):
    try:
        return int(float(val))
    except (ValueError, TypeError):
        return None

def safe_date(val):
    try:
        return datetime.strptime(val, "%m/%d/%Y")
    except (ValueError, TypeError):
        return None

def read_csv_and_store_in_batches(
    uri: str, db_name: str, coll_name: str, csv_path: str, batch_size: int
):
    # Establish client and get collection
    client = MongoClient(uri, tls=True)
    db = client[db_name]
    coll = db[coll_name]
    coll.with_options(write_concern=client.write_concern)  # inherit default write concern

    batch = []
    total_inserted = 0

    with open(csv_path, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            # Map CSV columns to document fields
            doc = {
                "BusinessDate": safe_date(row.get("Business Date")),
                "SecuriryId": row.get("Security Id"),
                "Symbol": row.get("Symbol"),
                "SecurityName": row.get("Security Name"),
                "OpenPrice": safe_float(row.get("Open Price")),
                "HighPrice": safe_float(row.get("High Price")),
                "LowPrice": safe_float(row.get("Low Price")),
                "ClosePrice": safe_float(row.get("Close Price")),
                "TotalTradedQuantity": safe_float(row.get("Total Traded Quantity")),
                "TotalTradedValue": safe_float(row.get("Total Traded Value")),
                "PreviousDayClosePrice": safe_float(row.get("Previous Day Close Price")),
                "FiftyTwoWeekHigh": safe_float(row.get("Fifty Two Week High")),
                "FiftyTwoWeekLow": safe_float(row.get("Fifty Two Week Low")),
                "LastUpdatedPrice": safe_float(row.get("Last Updated Price")),
                "TotalTrades": safe_int(row.get("Total Trades")),
                "AverageTradedPrice": safe_float(row.get("Average Traded Price")),
                "MarketCapitalization": safe_float(row.get("Market Capitalization")),
            }
            batch.append(doc)

            # Once batch_size is reached, insert and reset
            if len(batch) >= batch_size:
                coll.insert_many(batch, ordered=False)  # unordered for speed :contentReference[oaicite:1]{index=1}
                total_inserted += len(batch)
                print(f"Inserted batch of {len(batch)} documents (total {total_inserted})")
                batch.clear()

        # Insert any remaining documents
        if batch:
            coll.insert_many(batch, ordered=False)
            total_inserted += len(batch)
            print(f"Inserted final batch of {len(batch)} documents (total {total_inserted})")

    client.close()
    print("All done. Total documents inserted:", total_inserted)

if __name__ == "__main__":
    if test_mongo_connection(MONGO_URI):
        read_csv_and_store_in_batches(MONGO_URI, DATABASE, COLLECTION, CSV_PATH, BATCH_SIZE)
