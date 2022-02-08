import argparse

from hops import project, jobs

parser = argparse.ArgumentParser()

parser.add_argument("-k", "--key", help="Hopsworks api key", type=str)

args = parser.parse_args()

project.connect("workshop_production", "114069f0-882e-11ec-9fd1-b1f1c14cb903.cloud.hopsworks.ai", port=443, api_key=args.key)

jobs.start_job("sales_fg")
jobs.start_job("sales_target_fg")
