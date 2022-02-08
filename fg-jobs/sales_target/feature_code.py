import pandas as pd

def read_data(path: str) -> pd.DataFrame:
    return pd.read_csv(path)

def engineer_features(df: pd.DataFrame) -> pd.DataFrame:
    df["date"] = pd.to_datetime(df["date"])
    df.sort_values(["store", "dept", "date"], inplace=True)
    return df.groupby(["store", "dept"]).last().reset_index()
