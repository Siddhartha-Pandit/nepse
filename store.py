import csv
from pymongo import MongoClient
from datetime import datetime
import ssl
print(ssl.OPENSSL_VERSION)


# MongoDB configuration
# MONGO_URI = "mongodb+srv://devsiddharthapandit:CzUaLHtIfZ9eeWNF@cluster0.sq0xe.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"  # Update with your MongoDB Atlas URI
# MONGO_URI = (
#     "mongodb+srv://devsiddharthapandit:CzUaLHtIfZ9eeWNF@cluster0.sq0xe.mongodb.net/"
#     "?retryWrites=true&w=majority&tls=true&tlsAllowInvalidCertificates=true"
# )
MONGO_URI = "mongodb://localhost:27017/"  # Update if needed
database_name = "nepse"
# database_name = "nepse1"
collection_name = "dailyprice"

# CSV file configuration
csv_file_path = "1.csv"  # Replace with your CSV file path




def safe_float(value):
    try:
        return float(value)
    except (ValueError, TypeError):
        return None

def safe_int(value):
    try:
        return int(float(value))  # Handles cases like "30.0"
    except (ValueError, TypeError):
        return None

def safe_date(value):
    try:
        return datetime.strptime(value, "%m/%d/%Y")  # Adjust format if needed
    except (ValueError, TypeError):
        return None

def read_csv_and_store_in_mongodb():
    try:
        # Connect to MongoDB
        print("Connecting to MongoDB...")
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        db = client[database_name]
        collection = db[collection_name]

        # Test MongoDB connection
        client.server_info()
        print("MongoDB connection successful.")

        # Read CSV file
        with open(csv_file_path, mode='r') as file:
            csv_reader = csv.DictReader(file)

            # Insert data into MongoDB
            data_to_insert = []
            for row in csv_reader:
                # Clean and transform the row to handle blank fields
                data_to_insert.append({
                    "BUSINESS_DATE": safe_date(row.get("BUSINESS_DATE")),
                    "SECURITY_ID": row.get("SECURITY_ID", None),
                    "SYMBOL": row.get("SYMBOL", None),
                    "SECURITY_NAME": row.get("SECURITY_NAME", None),
                    "OPEN_PRICE": safe_float(row.get("OPEN_PRICE")),
                    "HIGH_PRICE": safe_float(row.get("HIGH_PRICE")),
                    "LOW_PRICE": safe_float(row.get("LOW_PRICE")),
                    "CLOSE_PRICE": safe_float(row.get("CLOSE_PRICE")),
                    "TOTAL_TRADED_QUANTITY": safe_float(row.get("TOTAL_TRADED_QUANTITY")),
                    "TOTAL_TRADED_VALUE": safe_float(row.get("TOTAL_TRADED_VALUE")),
                    "PREVIOUS_DAY_CLOSE_PRICE": safe_float(row.get("PREVIOUS_DAY_CLOSE_PRICE")),
                    "FIFTY_TWO_WEEKS_HIGH": safe_float(row.get("FIFTY_TWO_WEEKS_HIGH")),
                    "FIFTY_TWO_WEEKS_LOW": safe_float(row.get("FIFTY_TWO_WEEKS_LOW")),
                    "LAST_UPDATED_PRICE": safe_float(row.get("LAST_UPDATED_PRICE")),
                    "TOTAL_TRADES": safe_int(row.get("TOTAL_TRADES")),
                    "AVERAGE_TRADED_PRICE": safe_float(row.get("AVERAGE_TRADED_PRICE")),
                    "MARKET_CAPITALIZATION": safe_float(row.get("MARKET_CAPITALIZATION")),
                })

            if data_to_insert:
                collection.insert_many(data_to_insert)
                print(f"Inserted {len(data_to_insert)} records into MongoDB.")
            else:
                print("No data to insert.")

        # Close MongoDB connection
        client.close()

    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    read_csv_and_store_in_mongodb()