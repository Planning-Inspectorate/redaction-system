import azure.functions as func
import json

app = func.FunctionApp()


# An HTTP-triggered function with a Durable Functions client binding
@app.route(route="redact", methods=["POST"])
async def trigger_redaction(req: func.HttpRequest):
    """
    This function is called via HTTP post and adds redaction analysis requests to the service bus queue
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
    return f"Request received with parameters {request_params}"


# An HTTP-triggered function with a Durable Functions client binding
@app.route(route="apply", methods=["POST"])
async def trigger_apply(req: func.HttpRequest):
    """
    This function is called via HTTP post and adds redaction application requests to the service bus queue
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
    return f"Request received with parameters {request_params}"
