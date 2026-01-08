#from redactor.core.redaction_manager import RedactionManager
import azure.functions as func
import azure.durable_functions as df
import json
from uuid import uuid4

app = df.DFApp(http_auth_level=func.AuthLevel.FUNCTION)


# An HTTP-triggered function with a Durable Functions client binding
@app.route(route="orchestrators/{functionName}")
@app.durable_client_input(client_name="client")
async def http_start(req: func.HttpRequest, client: df.DurableOrchestrationClient):
    function_name = req.route_params.get('functionName')
    print("req params: ", req.params)
    instance_id = await client.start_new(function_name, client_input=req.params)
    response = client.create_check_status_response(req, instance_id)
    return response

# Orchestrator
@app.orchestration_trigger(context_name="context")
def hello_orchestrator(context: df.DurableOrchestrationContext):
    input_params = context.get_input()
    print("input_params: ", input_params)
    result1 = yield context.call_activity("hello", "Seattle")
    result2 = yield context.call_activity("hello", "Tokyo")
    result3 = yield context.call_activity("hello", "London")

    return [result1, result2, result3]

# Activity
@app.activity_trigger(input_name="city")
def hello(city: str):
    return f"Hello {city}"
