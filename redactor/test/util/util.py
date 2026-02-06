import os
from azure.servicebus.aio import ServiceBusClient
from azure.servicebus import ServiceBusReceiveMode
from azure.identity.aio import (
    AzureCliCredential,
    ManagedIdentityCredential,
    ChainedTokenCredential,
)
import asyncio


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
