import asyncio
import json
import os
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from typing import Any

from azure.identity.aio import (
    AzureCliCredential,
    ChainedTokenCredential,
    ManagedIdentityCredential,
)
from azure.servicebus import ServiceBusMessage
from azure.servicebus.aio import ServiceBusClient
from strenum import StrEnum


class PINSService(StrEnum):
    """
    Represents a service in PINS
    """

    CBOS = "CBOS"
    REDACTION_SYSTEM = "REDACTION_SYSTEM"


class ServiceBusUtil:
    """
    Utility class for sending messages to the service bus
    """

    async def _send_message(
        self, topic_name: str, pins_service: PINSService, payload: dict[str, Any]
    ):
        """
        Asynchronously send messages to the service bus
        """
        # Note: The sync API/SDKs do not seem to work - this must be done asynchronously
        # This is based on https://learn.microsoft.com/en-us/azure/service-bus-messaging/service-bus-python-how-to-use-topics-subscriptions?tabs=passwordless#send-messages-to-a-topic
        service_name = (
            pins_service.value.lower().replace("_", "-") if pins_service else None
        )
        if not service_name:
            raise ValueError("No valid 'pins_service' provided")
        service_bus_name = os.environ.get("AZURE_SERVICE_BUS_NAMESPACE", None)
        if not service_bus_name:
            raise RuntimeError(
                "No 'AZURE_SERVICE_BUS_NAMESPACE' environment variable is defined"
            )
        credential = ChainedTokenCredential(
            ManagedIdentityCredential(), AzureCliCredential()
        )
        async with ServiceBusClient(
            fully_qualified_namespace=f"{service_bus_name}.servicebus.windows.net",
            credential=credential,
            logging_enable=True,
        ) as service_bus_client:
            try:
                # Get a Topic Sender object to send messages to the topic
                sender = service_bus_client.get_topic_sender(topic_name)
                async with sender:
                    message = ServiceBusMessage(
                        json.dumps(payload),
                        subject=service_name,
                        time_to_live=timedelta(days=1),
                    )
                    await sender.send_messages([message])
                # Close credential when no longer needed.
            except ValueError as e:
                raise RuntimeError(
                    f"Failed to send message to service bus topic '{topic_name}' for "
                    f"service '{service_name}': {e}"
                ) from e
            finally:
                await credential.close()

    def send_redaction_process_complete_message(
        self, pins_service: PINSService, payload: dict[str, Any]
    ):
        """
        Send a message to the `redaction-process-complete` topic on the service bus
        """

        # Azure functions has an active event loop, so call the function in a worker thread
        def inner_wrapper():
            return asyncio.run(
                self._send_message("redaction-process-complete", pins_service, payload)
            )

        with ThreadPoolExecutor(max_workers=1) as executor:
            response = executor.submit(inner_wrapper)
            return response.result()
