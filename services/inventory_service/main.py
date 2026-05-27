from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI, Request

from shared.event_bus import InMemoryEventBus, RabbitMQEventBus, build_event_bus
from shared.events import EventEnvelope, EventType, new_event
from shared.logging import configure_logging, log_event
from shared.settings import get_settings


settings = get_settings("inventory-service")
logger = configure_logging(settings.service_name)
stock: dict[str, int] = {
    "coffee": 30,
    "water": 50,
    "tea": 25,
    "sandwich": 18,
    "snack": 40,
}
reservations: dict[str, list[dict[str, object]]] = {}


async def handle_order_paid(
    event: EventEnvelope,
    event_bus: InMemoryEventBus | RabbitMQEventBus,
    stock_state: dict[str, int] = stock,
    reservation_state: dict[str, list[dict[str, object]]] = reservations,
) -> None:
    if event.aggregate_id in reservation_state:
        return

    items = event.payload["items"]
    shortages = [
        {"sku": item["sku"], "requested": item["quantity"], "available": stock_state.get(item["sku"], 0)}
        for item in items
        if stock_state.get(item["sku"], 0) < item["quantity"]
    ]
    if shortages:
        await event_bus.publish(
            new_event(
                EventType.INVENTORY_SHORTAGE_DETECTED,
                aggregate_id=event.aggregate_id,
                source=settings.service_name,
                payload={"order_id": event.aggregate_id, "shortages": shortages},
                correlation_id=event.correlation_id,
            )
        )
        log_event(logger, settings.service_name, "inventory shortage", order_id=event.aggregate_id)
        return

    reserved_items = []
    for item in items:
        sku = item["sku"]
        quantity = item["quantity"]
        stock_state[sku] -= quantity
        reserved_items.append({"sku": sku, "quantity": quantity})

    reservation_state[event.aggregate_id] = reserved_items
    await event_bus.publish(
        new_event(
            EventType.INVENTORY_RESERVED,
            aggregate_id=event.aggregate_id,
            source=settings.service_name,
            payload={"order_id": event.aggregate_id, "items": reserved_items},
            correlation_id=event.correlation_id,
        )
    )
    log_event(logger, settings.service_name, "inventory reserved", order_id=event.aggregate_id)


@asynccontextmanager
async def lifespan(app: FastAPI):
    event_bus = build_event_bus(settings)
    await event_bus.connect()
    await event_bus.subscribe(
        EventType.ORDER_PAID,
        lambda event: handle_order_paid(event, event_bus),
        queue_name=f"{settings.service_name}.order-paid",
    )
    app.state.event_bus = event_bus
    log_event(logger, settings.service_name, "event subscriptions ready", bus=settings.event_bus)
    try:
        yield
    finally:
        await event_bus.close()


app = FastAPI(
    title="PeakPick Inventory Service",
    version="0.1.0",
    description="Stock reservation and shortage events.",
    lifespan=lifespan,
)


@app.get("/health")
async def health(request: Request) -> dict[str, object]:
    return {
        "status": "ok",
        "service": settings.service_name,
        "event_bus_connected": request.app.state.event_bus.is_connected,
    }


@app.get("/stock")
async def get_stock() -> dict[str, int]:
    return stock


@app.get("/reservations")
async def list_inventory_reservations() -> dict[str, list[dict[str, object]]]:
    return reservations

