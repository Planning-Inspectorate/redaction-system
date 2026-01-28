import subprocess
import argparse
from time import sleep
import json
import logging

"""
Utility script to wait for a function app to be running

Example usage

python3 pipelines/scripts/wait_for_function_app_to_be_running.py -e dev
"""

logging.basicConfig(level=logging.DEBUG)


class FunctionAppUtil:
    def __init__(self, function_app_name: str, resource_group: str):
        self.function_app_name = function_app_name
        self.resource_group = resource_group

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

    def get_function_app_status(self):
        """
        Get the status of the function app
        """
        command = (
            f"az functionapp show -n {self.function_app_name} -g {self.resource_group}"
        )
        command_output = self._run_command(command)
        command_output_json = json.loads(command_output)
        return command_output_json["state"]

    def wait_for_function_app_to_be_running(self):
        """
        Wait for the function app's status to be 'Running'. Raises an exception if waiting longer than 60s
        """
        logging.info("Waiting for the function app to be running before proceeding")
        max_wait_time_seconds = 60
        current_wait_time_seconds = 0
        retry_delay_seconds = 1
        while current_wait_time_seconds < max_wait_time_seconds:
            status = self.get_function_app_status()
            logging.info(
                f"    Polled the function app '{self.function_app_name}' in resource group '{self.resource_group}'. Current status: '{status}'"
            )
            if status == "Running":
                logging.info(f"Function app '{self.function_app_name}' is running")
                return
            logging.info(
                f"    Waiting {retry_delay_seconds} seconds before checking again"
            )
            current_wait_time_seconds += retry_delay_seconds
            sleep(current_wait_time_seconds)
        raise RuntimeError(
            f"Exceeded max wait time of {max_wait_time_seconds} seconds for function app {self.function_app_name} to be running. "
            f"Currently in state '{status}'"
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument("-e", "--env", help="Environment to run against")
    args = parser.parse_args()
    env = str(args.env).lower()
    resource_group_name = f"pins-rg-redaction-system-{env}-uks"
    function_app_name = f"pins-func-redaction-system-{env}-uks"
    FunctionAppUtil(
        function_app_name, resource_group_name
    ).wait_for_function_app_to_be_running()
