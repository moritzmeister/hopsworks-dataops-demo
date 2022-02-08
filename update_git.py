import json
import argparse
import time

import hsfs
from hsfs import client

parser = argparse.ArgumentParser()

parser.add_argument("-k", "--key", help="Hopsworks api key", type=str)

args = parser.parse_args()

conn = hsfs.connection(
    host="114069f0-882e-11ec-9fd1-b1f1c14cb903.cloud.hopsworks.ai",
    project="workshop_production",
    api_key_value=args.key,
    engine="training"
)

fs = conn.get_feature_store()
cl = client.get_instance()

path_params = [
    "project",
    cl._project_id,
    "git",
    "repository",
    3086,
]

headers = {"content-type": "application/json"}
payload = {
    "branchName": "production",
    "remoteName": "origin",
    "type": "pullCommandConfiguration",
}

json_value = json.dumps(payload)
query_params = {"action": "PULL"}

print(cl._send_request("POST", path_params, query_params=query_params, headers=headers, data=json_value))

# should be polling the git endpoint instead
time.sleep(30)
