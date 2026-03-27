from azure.servicebus.aio import ServiceBusClient
from azure.servicebus import ServiceBusMessage
from azure.identity.aio import (
    AzureCliCredential,
    ManagedIdentityCredential,
    ChainedTokenCredential,
)
import azure.functions as func
import logging
import json
import os
from uuid import uuid4
from typing import Dict, Any

app = func.FunctionApp()


async def _add_message_to_service_bus_queue(stage: str, req: func.HttpRequest):
    try:
        request_params: Dict[str, Any] = req.get_json()
    except ValueError:
        logging.error("Request had no valid json content")
        return func.HttpResponse(
            json.dumps(
                {
                    "error": "The json payload is missing from the request - unable to trigger the redaction process"
                }
            )
        )
    logging.info(f"Request added to queue with parameters {request_params}")
    request_params["stage"] = stage
    job_id = str(uuid4())
    if "overrideId" in request_params:
        job_id = str(request_params.pop("overrideId"))
    request_params["job_id"] = job_id
    service_bus_name = os.environ.get("AZURE_SERVICE_BUS_NAMESPACE", None)
    if not service_bus_name:
        logging.error(
            "AZURE_SERVICE_BUS_NAMESPACE variable not set in the function app"
        )
        raise RuntimeError(
            "No 'AZURE_SERVICE_BUS_NAMESPACE' environment variable is defined"
        )
    try:
        credential = ChainedTokenCredential(
            ManagedIdentityCredential(), AzureCliCredential()
        )
        async with ServiceBusClient(
            fully_qualified_namespace=f"{service_bus_name}.servicebus.windows.net",
            credential=credential,
            logging_enable=True,
        ) as service_bus_client:
            logging.info("Adding message to service bus queue")
            async with service_bus_client.get_queue_sender(
                "redaction-internal-queue"
            ) as sender:
                message = ServiceBusMessage(
                    json.dumps(request_params),
                )
                await sender.send_messages([message])
    except Exception as e:
        logging.error(f"Failed to send the new message to the service bus queue with the following exception: {e}")
    return func.HttpResponse(json.dumps({"id": job_id}), status_code=200)


# An HTTP-triggered function with a Durable Functions client binding
@app.route(route="redact", methods=["POST"])
async def trigger_redaction(req: func.HttpRequest):
    """
    This function is called via HTTP post and adds redaction analysis requests to the service bus queue
    """
    return await _add_message_to_service_bus_queue("ANALYSE", req)


# An HTTP-triggered function with a Durable Functions client binding
@app.route(route="apply", methods=["POST"])
async def trigger_apply(req: func.HttpRequest):
    """
    This function is called via HTTP post and adds redaction application requests to the service bus queue
    """
    return await _add_message_to_service_bus_queue("REDACT", req)
