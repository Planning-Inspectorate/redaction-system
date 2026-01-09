"""
The redaction function is implemented using the async-http pattern using Azure Durable Functions. 
Information about this pattern can be found in the below documentation
https://learn.microsoft.com/en-us/azure/azure-functions/durable/durable-functions-overview?tabs=in-process%2Cnodejs-v3%2Cv2-model&pivots=python#async-http
https://learn.microsoft.com/en-us/azure/azure-functions/durable/quickstart-python-vscode
"""

import logging
import azure.functions as func
import azure.durable_functions as df
import json
from typing import Dict, Any


app = df.DFApp(http_auth_level=func.AuthLevel.FUNCTION)


# An HTTP-triggered function with a Durable Functions client binding
@app.route(route="redact", methods=["POST"])
@app.durable_client_input(client_name="client")
async def trigger_redaction(
    req: func.HttpRequest, client: df.DurableOrchestrationClient
):
    """
    This function is called via HTTP post and triggers the redaction process.
    This asynchronously triggers the process, and returns a response object containing callback info
    for the caller to check the status via the `statusQueryGetUri` property of the json response
    """
    try:
        request_params = req.get_json()
    except ValueError:
        return func.HttpResponse(
            json.dumps(
                {
                    "error": "The json payload is missing from the request - unable to trigger the redaction process"
                }
            )
        )
    logging.info("request params: ", request_params)
    run_id = await client.start_new(
        "redaction_orchestrator", client_input=request_params
    )
    response = client.create_check_status_response(req, run_id)
    return response


# Orchestrator
@app.orchestration_trigger(context_name="context")
def redaction_orchestrator(context: df.DurableOrchestrationContext):
    """
    Orchestrator of the redaction process
    """
    input_params = context.get_input() | {"job_id": context.instance_id}
    result = yield context.call_activity("redact_task", input_params)
    return [result]


# Activity
@app.activity_trigger(input_name="params")
def redact_task(params: Dict[str, Any]):
    """
    Task which completes the redaction process
    """
    # Import inside this function so that the function app has a chance to start
    # Exceptions will instead be raised when this function is trigger
    from core.redaction_manager import RedactionManager  # noqa: F402

    job_id = params.pop("job_id")
    return RedactionManager(job_id).try_redact(params)
