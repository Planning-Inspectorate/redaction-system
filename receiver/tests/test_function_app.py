import asyncio
import json
from types import ModuleType

import azure.functions as func
import pytest
from azure.servicebus.exceptions import (
    MessageSizeExceededError,
    OperationTimeoutError,
    ServiceBusError,
)
from conftest import FakeServiceBus


def _request() -> func.HttpRequest:
    return func.HttpRequest(
        method="POST",
        url="/api/redact",
        body=json.dumps({"overrideId": "job-1"}).encode(),
    )


async def _post(receiver: ModuleType) -> func.HttpResponse:
    return await receiver._add_message_to_service_bus_queue("ANALYSE", _request())


def test_shared_resources_are_reused_across_requests(
    receiver: ModuleType, bus: FakeServiceBus
):
    async def run():
        return [await _post(receiver) for _ in range(3)]

    responses = asyncio.run(run())

    assert [r.status_code for r in responses] == [200, 200, 200]

    # Check that only one credential and one client were created and reused across requests
    assert len(bus.credentials) == 1
    assert len(bus.clients) == 1
    assert bus.clients[0].sender.send_messages.await_count == 3

    # Check the shared resources were not closed after handling the requests
    bus.clients[0].sender.close.assert_not_awaited()
    bus.clients[0].close.assert_not_awaited()
    bus.credentials[0].close.assert_not_awaited()


def test_concurrent_requests_are_serialised_by_the_lock(
    receiver: ModuleType, bus: FakeServiceBus
):
    """Only one request should be in flight at a time, enforced by the lock"""
    in_flight = 0
    max_in_flight = 0

    async def slow_send(_messages):
        nonlocal in_flight, max_in_flight  # modifies the counter defined in the outer scope
        in_flight += 1
        max_in_flight = max(max_in_flight, in_flight)
        await asyncio.sleep(0.01)
        in_flight -= 1

    async def run():
        # Create the client first so the sender can be given the tracking behaviour
        await _post(receiver)
        bus.clients[0].sender.send_messages.side_effect = slow_send
        return await asyncio.gather(*(_post(receiver) for _ in range(5)))

    responses = asyncio.run(run())

    assert all(r.status_code == 200 for r in responses)
    assert max_in_flight == 1
    assert len(bus.clients) == 1


@pytest.mark.parametrize("error", [OperationTimeoutError, ServiceBusError])
def test_connection_failure_resets_shared_resources(
    error: type[ServiceBusError], receiver: ModuleType, bus: FakeServiceBus
):
    async def run():
        await _post(receiver)
        bus.clients[0].sender.send_messages.side_effect = error(
            message="connection lost"
        )
        failed = await _post(receiver)
        recovered = await _post(receiver)
        return failed, recovered

    failed, recovered = asyncio.run(run())

    assert failed.status_code == 500
    assert json.loads(failed.get_body()) == {"message": "connection lost"}
    first_client = bus.clients[0]
    first_client.sender.close.assert_awaited_once()
    first_client.close.assert_awaited_once()
    bus.credentials[0].close.assert_awaited_once()

    assert recovered.status_code == 200
    # Check that the new client and credential were created for recovery
    assert len(bus.clients) == 2
    assert len(bus.credentials) == 2
    bus.clients[1].sender.send_messages.assert_awaited_once()


def test_failure_while_closing_does_not_stop_recovery(
    receiver: ModuleType, bus: FakeServiceBus
):
    async def run():
        await _post(receiver)
        first_client = bus.clients[0]
        first_client.sender.send_messages.side_effect = ServiceBusError("lost")
        first_client.sender.close.side_effect = RuntimeError("close failed")
        failed = await _post(receiver)
        recovered = await _post(receiver)
        return failed, recovered

    failed, recovered = asyncio.run(run())

    assert failed.status_code == 500
    assert recovered.status_code == 200
    assert len(bus.clients) == 2


def test_message_size_error_returns_500_without_resetting(
    receiver: ModuleType, bus: FakeServiceBus
):
    async def run():
        await _post(receiver)
        bus.clients[0].sender.send_messages.side_effect = MessageSizeExceededError(
            message="message too large"
        )
        too_large = await _post(receiver)
        bus.clients[0].sender.send_messages.side_effect = None
        next_response = await _post(receiver)
        return too_large, next_response

    too_large, next_response = asyncio.run(run())

    assert too_large.status_code == 500
    assert json.loads(too_large.get_body()) == {"message": "message too large"}
    assert next_response.status_code == 200
    assert len(bus.clients) == 1
    assert len(bus.credentials) == 1
    bus.clients[0].sender.close.assert_not_awaited()
    bus.clients[0].close.assert_not_awaited()
    bus.credentials[0].close.assert_not_awaited()
