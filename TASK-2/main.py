import pandas as pd
from fastparquet import ParquetFile
from sqlalchemy import create_engine


class Extraction:
    def __init__(self, file_path):
        self.file_path = file_path
        self.df = None

    # Load the parquet file to a DataFrame with fastparquet library.
    def __read_parquet(self):
        print(f"Extracting data from {self.file_path}.")
        pf = ParquetFile(self.file_path)
        self.df = pf.to_pandas()
        print("Extracting data successfully.")

    def __snake_case_column(self):
        columns = [
            "vendor_id",
            "tpep_pickup_datetime",
            "tpep_dropoff_datetime",
            "passenger_count",
            "trip_distance",
            "ratecode_id",
            "store_and_fwd_flag",
            "pu_location_id",
            "do_location_id",
            "payment_type",
            "fare_amount",
            "extra",
            "mta_tax",
            "tip_amount",
            "tolls_amount",
            "improvement_surcharge",
            "total_amount",
            "congestion_surcharge",
            "airport_fee",
        ]
        self.df.columns = columns
        print("Columns renamed successfully.")

    # Clean the Yellow Trip dataset.
    def __cast_data(self):
        # file csv and parquet cast data handler
        self.df["passenger_count"] = self.df["passenger_count"].astype("Int8")

        self.df["store_and_fwd_flag"] = self.df["store_and_fwd_flag"].replace(
            ["N", "Y"], [False, True]
        )
        self.df["store_and_fwd_flag"] = self.df["store_and_fwd_flag"].astype("boolean")

        self.df["tpep_pickup_datetime"] = pd.to_datetime(
            self.df["tpep_pickup_datetime"]
        )
        self.df["tpep_dropoff_datetime"] = pd.to_datetime(
            self.df["tpep_dropoff_datetime"]
        )
        print("Data types casted successfully.")

    def local_file(self):
        self.__read_parquet()
        self.__snake_case_column()
        self.__cast_data()

        return self.df


class Load:
    def __init__(self, df: pd.DataFrame, chunksize=1000):
        self.chunksize = chunksize
        self.connection: str
        self.engine = None
        self.tb_name: str
        self.df = df

    def to_postgres(self, connection: str, tb_name: str):
        from sqlalchemy.types import BigInteger, DateTime, Boolean, Float, Integer
        from sqlalchemy.exc import SQLAlchemyError

        # Define the data type schema when using to_sql method.
        df_schema = {
            "vendor_id": BigInteger,
            "tpep_pickup_datetime:": DateTime,
            "tpep_dropoff_datetime": DateTime,
            "passenger_count": BigInteger,
            "trip_distance": Float,
            "ratecode_id": Float,
            "store_and_fwd_flag": Boolean,
            "pu_location_id": Integer,
            "do_location_id": Integer,
            "payment_type": Integer,
            "fare_amount": Float,
            "extra": Float,
            "mta_tax": Float,
            "tip_amount": Float,
            "tolls_amount": Float,
            "improvement_surcharge": Float,
            "total_amount": Float,
            "congestion_surcharge": Float,
            "airport_fee": Float,
        }

        self.connection = connection
        self.engine = create_engine(connection)

        # Ingest the Yellow Trip dataset to PostgreSQL
        total_rows = self.df.shape[0]
        try:
            for start in range(0, total_rows, self.chunksize):
                end = start + self.chunksize
                chunk = self.df.iloc[start:end]

                chunk.to_sql(
                    name=tb_name,
                    con=self.engine,
                    if_exists="append",
                    index=False,
                    schema="public",
                    dtype=df_schema,
                    method=None,
                )
                print(f"Ingested rows {start + 1} to {end} into table {tb_name}.")

            print(f"Data ingestion complete. Total rows ingested: {total_rows}")
        except SQLAlchemyError as err:
            print("error >> ", err.__cause__)


def main():
    file_path = "../dataset/yellow_tripdata_2023-01.parquet"
    extract = Extraction(file_path)
    df_result = extract.local_file()

    user, password, host, port, database = (
        "postgres",
        "admin",
        "localhost",
        "5432",
        "mydb",
    )
    connection = f"postgresql://{user}:{password}@{host}:{port}/{database}"
    tb_name = "data_parquet"
    load_data = Load(df_result, chunksize=5000)
    load_data.to_postgres(connection, tb_name)


if __name__ == "__main__":
    main()
