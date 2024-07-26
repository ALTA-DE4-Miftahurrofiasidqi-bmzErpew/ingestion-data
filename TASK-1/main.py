import pandas as pd

file_path = "../dataset/sample.csv"
names = [
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
]

# 1. Membaca data dari file CSV
# 2. Mengubah nama kolom menjadi format snake_case
df = pd.read_csv(file_path, sep=",", header=0, names=names)


selected_columns = [
    "vendor_id",
    "passenger_count",
    "trip_distance",
    "payment_type",
    "fare_amount",
    "extra",
    "mta_tax",
    "tip_amount",
    "tolls_amount",
    "improvement_surcharge",
    "total_amount",
    "congestion_surcharge",
]

# 3. Memilih 10 baris teratas berdasarkan passenger_count
top_10_df = df.nlargest(10, "passenger_count")[selected_columns]

# 4. [Extra] Mengubah tipe data kolom ke tipe data yang sesuai
top_10_df = top_10_df.astype(
    {
        "vendor_id": "int64",
        "passenger_count": "int64",
        "trip_distance": "float64",
        "payment_type": "int64",
        "fare_amount": "float64",
        "extra": "float64",
        "mta_tax": "float64",
        "tip_amount": "float64",
        "tolls_amount": "float64",
        "improvement_surcharge": "float64",
        "total_amount": "float64",
        "congestion_surcharge": "float64",
    }
)

# # Menampilkan DataFrame hasil
print(top_10_df)
