import argparse
import subprocess
import json
from typing import Dict
import logging


logging.basicConfig(level=logging.DEBUG)


"""
This script is used to deploy OpenAI models, since some models that we need cannot be procured using Terraform.

Define the required models in the MODELS global variable below

Example usage
python3 pipelines/scripts/deploy_openai_model.py -e "dev"

"""
"""
# Not used while we figure out which model to use
MODELS = [
    {
        "model-format": "OpenAI",
        "model-name": "gpt-4.1",
        "model-version": "2025-04-14",
        "deployment-name": "gpt-4.1",
        "capacity": "25",
        "sku": "ProvisionedManaged",
        "scale-capacity": "",
        "scale-type": "Standard"
    }
]
"""
MODELS = [
    {
        "model-format": "OpenAI",
        "model-name": "gpt-4.1",
        "model-version": "2025-04-14",
        "deployment-name": "gpt-4.1",
        "capacity": "250",
        "sku": "GlobalStandard",
        "scale-capacity": None,
        "scale-type": None,
    }
]


class FoundryModelDeployer:
    def __init__(self, resource_group_name: str, foundry_name: str):
        self.resource_group = resource_group_name
        self.foundry_name = foundry_name

    def _run_command(self, command: str):
        """
        Run a cli command
        """
        command_split = command.split(" ")
        try:
            return subprocess.check_output(command_split)
        except subprocess.CalledProcessError as e:
            exception = e
        raise RuntimeError(
            f"The command '{command}' failed with the following message: {exception}"
        )

    def get_all_model_deployments(self):
        """
        Return all deployed models in the foundry instance
        """
        command = f"az cognitiveservices account deployment list -n {self.foundry_name} -g {self.resource_group}"
        command_output = self._run_command(command)
        command_output_json = json.loads(command_output)
        return command_output_json

    def deploy_model(self, model_json: Dict[str, str]):
        """
        Deploy a mdel using the supplied config.
        """
        model_json_cleaned = {k: v for k, v in model_json.items() if v}
        model_json_cleaned["name"] = self.foundry_name
        model_json_cleaned["resource-group"] = self.resource_group
        model_args = " ".join(
            [
                f"--{subitem}" if subitem == item[0] else subitem
                for item in model_json_cleaned.items()
                for subitem in item
            ]
        )
        command = f"az cognitiveservices account deployment create {model_args}"
        command_output = self._run_command(command)
        command_output_json = json.loads(command_output)
        return command_output_json

    def deploy_models(self):
        all_deployed_models = self.get_all_model_deployments()
        all_deployed_model_names = [x["name"] for x in all_deployed_models]
        models_map = {model["model-name"]: model for model in MODELS}
        model_names_in_config = set(models_map.keys())
        model_names_to_create = {
            model
            for model in model_names_in_config
            if model not in all_deployed_model_names
        }
        model_names_to_update = {
            model["name"]
            for model in all_deployed_models
            if model["name"] in model_names_in_config
        }
        models_to_deploy = [
            models_map[model_name]
            for model_name in model_names_to_create | model_names_to_update
        ]
        logging.info("The following models have already been deployed")
        logging.info(json.dumps(all_deployed_models, indent=4))
        logging.info("The following deployed models will be created")
        logging.info(json.dumps(list(model_names_to_create), indent=4))
        logging.info("The following deployed models will be updated")
        logging.info(json.dumps(list(model_names_to_update), indent=4))
        for model in models_to_deploy:
            self.deploy_model(model)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument("-e", "--env", help="Environment to run against")
    args = parser.parse_args()
    env = str(args.env).lower()
    resource_group_name = f"pins-rg-redaction-system-{env}-uks"
    foundry_account_name = f"pins-openai-redaction-system-{env}-uks"
    deployer = FoundryModelDeployer(resource_group_name, foundry_account_name)
    deployed_models = deployer.get_all_model_deployments()
    deployer.deploy_models()
