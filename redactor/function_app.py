import azure.functions as func
import datetime
import json
import logging
import json

app = func.FunctionApp()


@app.route(route="redact", methods=["POST"], auth_level=func.AuthLevel.FUNCTION)
def redact(req: func.HttpRequest) -> func.HttpResponse:
    """
    Redact HTTP POST method, which allows the redaction system to be interacted with by the user
    """
    return func.HttpResponse(
        json.dumps(
            {
                "message": f"The redaction function was successfully called with the parameters {req}"
            }
        ),
        mimetype="application/json",
        status_code=200,
    )


@app.route(route="ping", auth_level=func.AuthLevel.FUNCTION)
def ping(req: func.HttpRequest) -> func.HttpResponse:
    """
    Function for testing connectivity to the redaction system. Returns a simple json response
    """
    return func.HttpResponse(
        json.dumps(
            {"message": f"You have successfully interacted with the redaction system!"}
        ),
        mimetype="application/json",
        status_code=200,
    )
