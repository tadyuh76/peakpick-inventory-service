import pytest
from fastapi.testclient import TestClient

from services.inventory_service.main import app, handle_order_paid
from shared.event_bus import InMemoryEventBus
from shared.events import EventType, new_event


client = TestClient(app)


def test_stock_lookup_returns_single_sku() -> None:
    response = client.get("/stock/coffee")

    assert response.status_code == 200
    assert response.json()["sku"] == "coffee"
    assert response.json()["quantity"] >= 0


def test_low_stock_endpoint_uses_threshold() -> None:
    response = client.get("/stock/low?threshold=30")

    assert response.status_code == 200
    assert all(item["quantity"] <= 30 for item in response.json())


@pytest.mark.asyncio
async def test_shortage_does_not_change_stock() -> None:
    bus = InMemoryEventBus()
    await bus.connect()
    stock = {"coffee": 1}
    reservations = {}
    shortage_events = []

    async def capture_shortage(event) -> None:
        shortage_events.append(event)

    await bus.subscribe(EventType.INVENTORY_SHORTAGE_DETECTED, capture_shortage)

    await handle_order_paid(
        new_event(
            EventType.ORDER_PAID,
            aggregate_id="order-short",
            source="order-service",
            payload={"items": [{"sku": "coffee", "quantity": 2}], "pickup_window": "12:00-12:15"},
        ),
        bus,
        stock,
        reservations,
    )

    assert stock["coffee"] == 1
    assert reservations == {}
    assert shortage_events


@pytest.mark.asyncio
async def test_duplicate_order_paid_does_not_double_decrement_stock() -> None:
    bus = InMemoryEventBus()
    await bus.connect()
    stock = {"coffee": 5}
    reservations = {}
    event = new_event(
        EventType.ORDER_PAID,
        aggregate_id="order-repeat",
        source="order-service",
        payload={"items": [{"sku": "coffee", "quantity": 2}], "pickup_window": "12:00-12:15"},
        correlation_id="44444444-4444-4444-4444-444444444444",
    )

    await handle_order_paid(event, bus, stock, reservations)
    await handle_order_paid(event, bus, stock, reservations)

    assert stock["coffee"] == 3
    assert reservations["order-repeat"]["status"] == "Reserved"
    assert reservations["order-repeat"]["correlation_id"] == event.correlation_id
