#from redactor.core.redaction_manager import RedactionManager
import azure.functions as func
import azure.durable_functions as df
import json
from uuid import uuid4
import time
from typing import Dict, Any

# Based on https://learn.microsoft.com/en-us/azure/azure-functions/durable/quickstart-python-vscode?tabs=macos
app = df.DFApp(http_auth_level=func.AuthLevel.FUNCTION)


# An HTTP-triggered function with a Durable Functions client binding
@app.route(route="orchestrators/{functionName}")
@app.durable_client_input(client_name="client")
async def http_start(req: func.HttpRequest, client: df.DurableOrchestrationClient):
    function_name = req.route_params.get('functionName')
    run_id = await client.start_new(function_name, client_input=req.params)
    response = client.create_check_status_response(req, run_id)
    return response

# Orchestrator
@app.orchestration_trigger(context_name="context")
def redact_orchestrator(context: df.DurableOrchestrationContext):
    result = yield context.call_activity("redact", context.get_input())
    return [result]

# Activity
@app.activity_trigger(input_name="params")
def redact(params: Dict[str, Any]):
    time.sleep(30)
    return params
