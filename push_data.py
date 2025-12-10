# push_data.py

import os
import sys

from dotenv import load_dotenv
load_dotenv()

MONGO_DB_URL = os.getenv("MONGO_DB_URL")
print(MONGO_DB_URL)

import pandas as pd
import numpy as np
import pymongo

from networksecurity.exception.exception import NetworkSecurityException
from networksecurity.logging.logger import logging


class NetworkDataExtract:
    def __init__(self):
        try:
            if not MONGO_DB_URL:
                raise ValueError("MONGO_DB_URL is not set in environment variables.")

            # Minimal MongoClient for MongoDB Atlas (TLS is handled automatically for mongodb+srv)
            self.mongo_client = pymongo.MongoClient(
                MONGO_DB_URL,
                serverSelectionTimeoutMS=5000,
            )

            # Fail fast if connection is not working
            self.mongo_client.admin.command("ping")
            logging.info("Successfully connected to MongoDB from push_data.py")

        except Exception as e:
            # Ensure the client is closed if partially initialized
            if hasattr(self, "mongo_client"):
                try:
                    self.mongo_client.close()
                except Exception:
                    pass
            raise NetworkSecurityException(f"MongoDB connection failed: {e}", sys)

    def __del__(self):
        """Ensure the MongoDB connection is closed when the object is garbage collected."""
        if hasattr(self, "mongo_client") and self.mongo_client:
            try:
                self.mongo_client.close()
            except Exception:
                # Ignore errors during closing if the client was already closed or invalid
                pass

    def csv_to_json_converter(self, file_path: str):
        try:
            data = pd.read_csv(file_path)
            data.reset_index(drop=True, inplace=True)
            records = data.to_dict("records")
            return records

        except Exception as e:
            raise NetworkSecurityException(e, sys)

    def insert_data_mongodb(self, records, database: str, collection: str) -> int:
        try:
            db = self.mongo_client[database]
            coll = db[collection]

            result = coll.insert_many(records)
            inserted_count = len(result.inserted_ids)

            # Close connection after successful insert (optional but fine for script)
            self.mongo_client.close()

            logging.info(f"Inserted {inserted_count} records into {database}.{collection}")
            return inserted_count

        except Exception as e:
            if hasattr(self, "mongo_client"):
                try:
                    self.mongo_client.close()
                except Exception:
                    pass
            raise NetworkSecurityException(e, sys)


if __name__ == "__main__":
    FILE_PATH = "Network_Data/phisingData.csv"
    DATABASE = "CHIRAGAI"
    COLLECTION = "NetworkData"

    try:
        networkobj = NetworkDataExtract()

        records = networkobj.csv_to_json_converter(file_path=FILE_PATH)
        print(f"Successfully converted {len(records)} records from CSV.")

        no_of_records = networkobj.insert_data_mongodb(records, DATABASE, COLLECTION)
        print(f"Total number of records inserted into MongoDB: {no_of_records}")

    except NetworkSecurityException as e:
        print(f"Data extraction process failed: {e}")
    except Exception as e:
        print(f"An unexpected error occurred in the main execution block: {e}")
