from db_connection import DatabaseConnection
from ingestion import DataIngestion

if __name__ == "__main__":
    # Connection strings for the source and target databases
    source_connection_string = "postgresql://postgres:pass@localhost:5432/store"
    target_connection_string = "postgresql://postgres:pass@localhost:15432/store"

    # Create database connection objects
    source_db = DatabaseConnection(source_connection_string)
    target_db = DatabaseConnection(target_connection_string)

    # Connect to the databases
    source_db.connect()
    target_db.connect()

    # Create the DataIngestion object
    data_ingestion = DataIngestion(source_db, target_db)

    # Define the source and target tablesource_tabl
    source_tables = ["brands", "products", "order_details", "orders"]

    # Execute the data ingestion process
    for source_table in source_tables:
        data_ingestion.execute(source_table)

    # Close the database connections
    source_db.close()
    target_db.close()
