import pandas as pd
import sqlite3
import hashlib

df0 = pd.read_csv("data/shipping_data_0.csv")
df1 = pd.read_csv("data/shipping_data_1.csv")
df2 = pd.read_csv("data/shipping_data_2.csv")

df0["shipment_identifier"] = (
    df0["origin_warehouse"].astype(str) + "_" +
    df0["destination_store"].astype(str) + "_" +
    df0["driver_identifier"].astype(str)
)

df1["shipment_identifier"] = df1["shipment_identifier"].astype(str)
df2["shipment_identifier"] = df2["shipment_identifier"].astype(str)

merged_df = pd.merge(df1, df2, on="shipment_identifier", how="left")
merged_df["product_quantity"] = 1

merged_df["shipment_identifier"] = merged_df.apply(
    lambda row: hashlib.md5(
        f"{row['origin_warehouse']}_{row['destination_store']}_{row['driver_identifier']}".encode()
    ).hexdigest() if pd.notnull(row["origin_warehouse"]) else row["shipment_identifier"],
    axis=1
)

df0["shipment_identifier"] = df0["shipment_identifier"].astype(str)

combined_df = pd.concat([df0, merged_df], ignore_index=True)

combined_df["on_time"] = combined_df["on_time"].astype(bool)
combined_df["product_quantity"] = combined_df["product_quantity"].astype(int)

columns = [
    "shipment_identifier",
    "origin_warehouse",
    "destination_store",
    "driver_identifier",
    "product",
    "product_quantity",
    "on_time"
]

combined_df = combined_df[columns]

conn = sqlite3.connect("shipment_database.db")
combined_df.to_sql("shipments", conn, if_exists="append", index=False)
conn.close()

print("Database populated successfully.")
