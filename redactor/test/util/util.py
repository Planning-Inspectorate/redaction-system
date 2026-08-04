import asyncio
import json
import os
from math import isclose

from azure.identity.aio import (
    AzureCliCredential,
    ChainedTokenCredential,
    ManagedIdentityCredential,
)
from azure.servicebus import ServiceBusReceiveMode
from azure.servicebus.aio import ServiceBusClient
from pymupdf import Rect


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
                await credential.close()
                raise
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
                await credential.close()
                raise
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


def assert_rect_approx_equal(
    actual: Rect,
    expected: Rect,
    *,
    rel_tol: float = 1e-2,
    abs_tol: float = 1e-2,
):
    """
    Assert that two pymupdf.Rect objects are approximately equal.

    Args:
        actual: The actual Rect value.
        expected: The expected Rect value.
        rel_tol: Relative tolerance for float comparison.
        abs_tol: Absolute tolerance for float comparison.
    """
    for attr in ("x0", "y0", "x1", "y1"):
        a = getattr(actual, attr)
        e = getattr(expected, attr)
        assert isclose(a, e, rel_tol=rel_tol, abs_tol=abs_tol), (
            f"Rect.{attr} mismatch: {a} != {e} (tolerance rel={rel_tol}, abs={abs_tol})\n"
            f"  actual:   {actual}\n"
            f"  expected: {expected}"
        )


def assert_instances_to_redact_approx_equal(
    actual_instances: list[tuple[int, Rect, str]],
    expected_instances: list[tuple[int, Rect, str]],
):
    for actual, expected in zip(actual_instances, expected_instances):
        assert actual[0] == expected[0], (
            f"Page number mismatch: {actual[0]} != {expected[0]}"
        )
        assert actual[2] == expected[2], (
            f"Redaction term mismatch: {actual[2]} != {expected[2]}"
        )
        (
            assert_rect_approx_equal(actual[1], expected[1]),
            (f"Rect mismatch: {actual[1]} != {expected[1]}"),
        )
