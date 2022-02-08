import argparse

import pandas as pd

import hsfs

VERSION = 1
NAME = "weekly_sales_target"
PK = ["store", "dept"]
DESCRIPTION = "containing the latest weekly sales of each store/department"

parser = argparse.ArgumentParser()

parser.add_argument("-d", "--data", help="Data location", type=str, default="hdfs:///Projects/workshop_production/Resources/sales-set.csv")

args = parser.parse_args()

def read_data(path: str) -> pd.DataFrame:
    return pd.read_csv(path)

def engineer_features(df: pd.DataFrame) -> pd.DataFrame:
    df["date"] = pd.to_datetime(df["date"])
    df.sort_values(["store", "dept", "date"], inplace=True)
    return df.groupby(["store", "dept"]).last().reset_index()

df = read_data(args.data)
feature_upsert_df = engineer_features(df)

conn = hsfs.connection()
fs = conn.get_feature_store()

try:
    fg = fs.get_feature_group(NAME, VERSION)
    fg.insert(feature_upsert_df)
except:
    fg = fs.create_feature_group(NAME, VERSION, statistics_config=False, primary_key=PK)
    fg.save(feature_upsert_df)
