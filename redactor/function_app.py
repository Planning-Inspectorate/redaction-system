#from redactor.core.redaction_manager import RedactionManager
import azure.functions as func
import azure.durable_functions as df
from typing import Dict, Any

app = df.DFApp(http_auth_level=func.AuthLevel.FUNCTION)

"""
The redaction function is implemented using the async-http pattern using Azure Durable Functions. 

Information about this pattern can be found in the below documentation

https://learn.microsoft.com/en-us/azure/azure-functions/durable/durable-functions-overview?tabs=in-process%2Cnodejs-v3%2Cv2-model&pivots=python#async-http
https://learn.microsoft.com/en-us/azure/azure-functions/durable/quickstart-python-vscode

"""

# An HTTP-triggered function with a Durable Functions client binding
@app.route(route="orchestrators", methods=["POST"])
@app.durable_client_input(client_name="client")
async def trigger_redaction(req: func.HttpRequest, client: df.DurableOrchestrationClient):
    """
    This function is called via HTTP post and triggers the redaction process.

    This asynchronously triggers the process, and returns a response object containing callback info
    for the caller to check the status via the `statusQueryGetUri` property of the json response
    """
    instance_id = await client.start_new("redaction_orchestrator", client_input=req.get_json())
    response = client.create_check_status_response(req, instance_id)
    return response

# Orchestrator
@app.orchestration_trigger(context_name="context")
def _redaction_orchestrator(context: df.DurableOrchestrationContext):
    """
    Private orchestrator of the redaction process
    """
    input_params = context.get_input()
    result = yield context.call_activity("_redact_task", input_params)

    return [result]

# Activity
@app.activity_trigger(input_name="params")
def _redact_task(params: Dict[str, Any]):
    """
    Private task which completes the redaction process
    """
    return f"Redact task called with {params}"
