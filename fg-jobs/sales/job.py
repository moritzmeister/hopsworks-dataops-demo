import argparse

import pandas as pd
import numpy as np

import hsfs

VERSION = 1
NAME = "weekly_sales"
PK = ["store", "dept"]
DESCRIPTION = "containing the latest weekly sales of each store/department"

parser = argparse.ArgumentParser()

parser.add_argument("-d", "--data", help="Data location", type=str, default="hdfs:///Projects/workshop_production/Resources/sales-set.csv")

args = parser.parse_args()


def read_data(path: str) -> pd.DataFrame:
    return pd.read_csv(path)

def engineer_features(df: pd.DataFrame) -> pd.DataFrame:
    print(df.head())
    df["date"] = pd.to_datetime(df["date"])
    df.sort_values(["store", "dept", "date"], inplace=True)
    target_df = df.groupby(["store", "dept"]).last().reset_index()

    merge_df = pd.merge(df, target_df[["store", "dept", "date"]], on=["store", "dept"], how="left")
    hist_df = merge_df[merge_df["date_x"] != merge_df["date_y"]]
    hist_df["holiday_flag"] = merge_df['is_holiday'].apply(lambda x: 1 if x else 0) 
    hist_df["non_holiday_flag"] = merge_df['is_holiday'].apply(lambda x: 0 if x else 1)
    hist_df["holiday_week_sales"] = hist_df["holiday_flag"] * hist_df["weekly_sales"]
    hist_df["non_holiday_week_sales"] = hist_df["non_holiday_flag"] * hist_df["weekly_sales"]
    total_features = hist_df.groupby(["store", "dept"]).agg(
        {"weekly_sales": [sum, np.mean],
        "date_x": pd.Series.nunique,
        "holiday_week_sales": sum,
        "non_holiday_week_sales": sum})
    total_features.columns = ['_'.join(col).strip() for col in total_features.columns.values]
    total_features.reset_index(inplace=True)
    return total_features

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
