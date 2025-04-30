import csv
from pymongo import MongoClient, errors
from datetime import datetime

# MongoDB configuration (no TLS for localhost)
MONGO_URI = "mongodb://localhost:27017/"
DATABASE    = "nepse"
COLLECTION  = "dailyprice"
CSV_PATH    = "12.csv"

# Tune between 100–1000 for best throughput
BATCH_SIZE = 500  # batch insert size

def test_mongo_connection(uri: str, timeout_ms: int = 5000) -> bool:
    """
    Attempts to connect to a local MongoDB and run a ping command.
    Returns True if successful, False otherwise.
    """
    try:
        # No TLS flags needed for localhost :contentReference[oaicite:2]{index=2}
        client = MongoClient(uri, serverSelectionTimeoutMS=timeout_ms)
        client.admin.command("ping")
        print("✅ Successfully connected to MongoDB.")
        client.close()
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

def read_csv_and_store_in_batches(uri, db_name, coll_name, csv_path, batch_size):
    client = MongoClient(uri)  # simple localhost client
    db = client[db_name]
    coll = db[coll_name]

    batch = []
    total = 0

    with open(csv_path, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            doc = {
                "BusinessDate": safe_date(row.get("Business Date")),
                "SecurityId":   row.get("Security Id"),         # fixed typo
                "Symbol":       row.get("Symbol"),
                "SecurityName": row.get("Security Name"),
                "OpenPrice":    safe_float(row.get("Open Price")),
                "HighPrice":    safe_float(row.get("High Price")),
                "LowPrice":     safe_float(row.get("Low Price")),
                "ClosePrice":   safe_float(row.get("Close Price")),
                "TotalTradedQuantity": safe_float(row.get("Total Traded Quantity")),
                "TotalTradedValue":    safe_float(row.get("Total Traded Value")),
                "PreviousDayClosePrice": safe_float(row.get("Previous Day Close Price")),
                "FiftyTwoWeekHigh":      safe_float(row.get("Fifty Two Week High")),
                "FiftyTwoWeekLow":       safe_float(row.get("Fifty Two Week Low")),
                "LastUpdatedPrice":      safe_float(row.get("Last Updated Price")),
                "TotalTrades":           safe_int(row.get("Total Trades")),
                "AverageTradedPrice":    safe_float(row.get("Average Traded Price")),
                "MarketCapitalization":  safe_float(row.get("Market Capitalization")),
            }
            batch.append(doc)

            if len(batch) >= batch_size:
                coll.insert_many(batch, ordered=False)  # unordered for speed :contentReference[oaicite:3]{index=3}
                total += len(batch)
                print(f"Inserted batch of {len(batch)} documents (total {total})")
                batch.clear()

        # final leftover batch
        if batch:
            coll.insert_many(batch, ordered=False)
            total += len(batch)
            print(f"Inserted final batch of {len(batch)} documents (total {total})")

    client.close()
    print("All done. Total inserted:", total)

if __name__ == "__main__":
    if test_mongo_connection(MONGO_URI):
        read_csv_and_store_in_batches(
            MONGO_URI, DATABASE, COLLECTION, CSV_PATH, BATCH_SIZE
        )
