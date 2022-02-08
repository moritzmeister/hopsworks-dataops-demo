import argparse

import config
import feature_code

import hsfs

FG_version = config.__version__

parser = argparse.ArgumentParser()

parser.add_argument("-d", "--data", help="Data location", type=str, default="hdfs:///Projects/workshop_production/Resources/sales-set.csv")

args = parser.parse_args()

feature_upsert_df = feature_code.engineer_features(args.data)

conn = hsfs.connection()
fs = conn.get_feature_store()

try:
    fg = fs.get_feature_group(config.__name__, config.__name__)
    fg.insert(feature_upsert_df)
except:
    fg = fs.create_feature_group(config.__name__, config.__version__, statistics_config=False, primary_key=config.__pk__)
    fg.save(feature_upsert_df)
