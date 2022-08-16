import json
import argparse
import os
from string import Template
import wandb

import hopsworks

parser = argparse.ArgumentParser()

parser.add_argument("-k", "--key", help="Hopsworks api key", type=str)
parser.add_argument("-w", "--wandb", help="WandB api key", type=str)

args = parser.parse_args()

conn = hopsworks.connection(
    host="c.app.hopsworks.ai",
    project="webinar",
    api_key_value=args.key,
)

project = conn.get_project("webinar")
fs = project.get_feature_store()

models_list = os.listdir("models")

for model in models_list:
    with open("models/" + model, "r") as file:
        d = file.read()
        d = json.loads(d)
    d["api_key"] = args.key

    fv = fs.get_feature_view(d["fv"], d["fv_version"])
    tag = fv.get_tag("wandb")

    with open('predictor_template.txt', 'r') as file:
        data = Template(file.read())
        result = data.substitute(d)

    wandb.login(key=args.wandb)

    with wandb.init(project="fraud_batch", job_type="deployment", name="deploy_model") as run:
        model_artifact = run.use_artifact('moritzmeister/fraud_batch/{}:{}'.format(tag["name"],tag["version"]), type=tag["type"])
        model_dir = model_artifact.download()
    print("Initialization Complete")

    print(model_dir)
    with open(model_dir + "/predictor.py", "w") as file:
        file.write(result)

    mr = project.get_model_registry()
    model = mr.sklearn.create_model("other_model", input_example=[4440515374959168])
    model.save(model_dir)

    depl = model.deploy(
        name="githubaction", 
        model_server="PYTHON",
        serving_tool="KSERVE",
        script_file=model.model_path + "/" + str(model.version) + "/predictor.py"
    )

    depl.start()
