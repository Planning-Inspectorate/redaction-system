import json
import logging
import os
from datetime import timedelta
from typing import Any
from uuid import uuid4

import azure.functions as func
from azure.identity.aio import (
    AzureCliCredential,
    ChainedTokenCredential,
    ManagedIdentityCredential,
)
from azure.servicebus import ServiceBusMessage
from azure.servicebus.aio import ServiceBusClient
from azure.servicebus.exceptions import (
    MessageSizeExceededError,
    OperationTimeoutError,
    ServiceBusError,
)

app = func.FunctionApp()


async def _add_message_to_service_bus_queue(stage: str, req: func.HttpRequest):
    logger = logging.getLogger(__name__)
    try:
        request_params: dict[str, Any] = req.get_json()
    except ValueError:
        logger.info("Request had no valid json content")
        return func.HttpResponse(
            json.dumps(
                {
                    "error": "The json payload is missing from the request - unable to trigger the redaction process"
                }
            )
        )
    logger.info(f"Request added to queue with parameters {request_params}")
    request_params["stage"] = stage
    job_id = str(request_params.pop("overrideId", uuid4()))
    request_params["job_id"] = job_id
    service_bus_name = os.environ.get("AZURE_SERVICE_BUS_NAMESPACE", None)
    if not service_bus_name:
        logger.error("AZURE_SERVICE_BUS_NAMESPACE variable not set in the function app")
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
            logger.info("Adding message to service bus queue")
            async with service_bus_client.get_queue_sender(
                "redaction-internal-queue"
            ) as sender:
                message = ServiceBusMessage(
                    json.dumps(request_params), time_to_live=timedelta(days=1)
                )
                await sender.send_messages([message])
    except (OperationTimeoutError, MessageSizeExceededError, ServiceBusError) as e:
        logger.error(
            f"Failed to send the new message to the service bus queue with the following exception: {e}"
        )
        return func.HttpResponse(json.dumps({"message": str(e)}), status_code=500)
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
