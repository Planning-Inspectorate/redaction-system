import os
from azure.servicebus.aio import ServiceBusClient
from azure.servicebus import ServiceBusReceiveMode
from azure.identity.aio import (
    AzureCliCredential,
    ManagedIdentityCredential,
    ChainedTokenCredential,
)
import asyncio
import json


class ServiceBusUtil:
    async def _extract_service_bus_messages(self, topic_name: str, subscription: str):
        """
        Asynchronously receive messages from the service bus
        """
        # Note: The sync API/SDKs do not seem to work - this must be done asynchronously
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
                receiver = service_bus_client.get_subscription_receiver(
                    topic_name, subscription
                )
                async with receiver:
                    new_messages = await receiver.peek_messages(max_message_count=500)
                    all_messages = new_messages
                    while new_messages:
                        from_seq_num = new_messages[-1].sequence_number + 1
                        new_messages = await receiver.peek_messages(
                            max_message_count=100, sequence_number=from_seq_num
                        )
                        if new_messages:
                            all_messages.extend(new_messages)
                    return all_messages
                # Close credential when no longer needed.
            except Exception:
                raise
            finally:
                await credential.close()

    async def _consume_service_bus_messages(self, topic_name: str, subscription: str):
        # Note: The sync API/SDKs do not seem to work - this must be done asynchronously
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
                receiver = service_bus_client.get_subscription_receiver(
                    topic_name,
                    subscription,
                    receive_mode=ServiceBusReceiveMode.RECEIVE_AND_DELETE,
                )
                async with receiver:
                    new_messages = await receiver.receive_messages(
                        max_message_count=100
                    )
                    for message in new_messages:
                        await receiver.complete_message(message)
            except Exception:
                raise
            finally:
                await credential.close()

    def extract_service_bus_complete_messages(self):
        return asyncio.run(
            self._extract_service_bus_messages(
                "redaction-process-complete", "redaction-system"
            )
        )

    def receive_service_bus_complete_messages(self):
        return asyncio.run(
            self._consume_service_bus_messages(
                "redaction-process-complete", "redaction-system"
            )
        )


def compare_unashable_lists(expected_results, actual_results):
    """
    Compare two lists. This is used for comparing unhashable elements when you do not care about the order
    """
    matches = [val in actual_results for val in expected_results]
    in_expected_but_not_actual = [
        val for val in expected_results if val not in actual_results
    ]
    in_actual_but_not_expected = [
        val for val in actual_results if val not in expected_results
    ]
    message = (
        "The following values were expected but could not be found:"
        f" {json.dumps(in_expected_but_not_actual, indent=4, default=str)}. The "
        f"following values were found but were not expected {json.dumps(in_actual_but_not_expected, indent=4, default=str)}\n"
        f"Expected value: {json.dumps(expected_results, indent=4, default=str)}\n"
        f"Actual value: {json.dumps(actual_results, indent=4, default=str)}\n"
    )
    assert len(expected_results) == len(actual_results), message
    assert all(matches), message
