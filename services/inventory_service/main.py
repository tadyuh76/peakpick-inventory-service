from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request

from shared.event_bus import InMemoryEventBus, RabbitMQEventBus, build_event_bus
from shared.events import EventEnvelope, EventType, new_event
from shared.logging import configure_logging, log_event
from shared.settings import get_settings


settings = get_settings("inventory-service")
logger = configure_logging(settings.service_name)
INITIAL_STOCK: dict[str, int] = {
    "coffee": 30,
    "water": 50,
    "tea": 25,
    "sandwich": 18,
    "snack": 40,
}
stock: dict[str, int] = INITIAL_STOCK.copy()
reservations: dict[str, list[dict[str, object]]] = {}


def _database_enabled() -> bool:
    return bool(settings.database_url)


async def _has_inventory_decision(order_id: str) -> bool:
    if order_id in reservations:
        return True
    if not _database_enabled():
        return False
    return await asyncio.to_thread(_has_inventory_decision_sync, order_id)


def _has_inventory_decision_sync(order_id: str) -> bool:
    import psycopg

    with psycopg.connect(settings.database_url) as conn:
        row = conn.execute(
            """
            SELECT event_id
            FROM event_log
            WHERE aggregate_id = %s
              AND event_type IN (%s, %s)
            LIMIT 1
            """,
            (order_id, str(EventType.INVENTORY_RESERVED), str(EventType.INVENTORY_SHORTAGE_DETECTED)),
        ).fetchone()
    return row is not None


async def _stock_from_event_log() -> dict[str, int]:
    if not _database_enabled():
        return stock
    return await asyncio.to_thread(_stock_from_event_log_sync)


def _stock_from_event_log_sync() -> dict[str, int]:
    current = INITIAL_STOCK.copy()
    for items in _inventory_reservations_from_event_log_sync().values():
        for item in items:
            sku = str(item["sku"])
            current[sku] = current.get(sku, 0) - int(item["quantity"])
    return current


async def _inventory_reservations_from_event_log() -> dict[str, list[dict[str, object]]]:
    if not _database_enabled():
        return reservations
    return await asyncio.to_thread(_inventory_reservations_from_event_log_sync)


def _inventory_reservations_from_event_log_sync() -> dict[str, list[dict[str, object]]]:
    import psycopg
    from psycopg.rows import dict_row

    with psycopg.connect(settings.database_url, row_factory=dict_row) as conn:
        rows = conn.execute(
            """
            SELECT aggregate_id, payload
            FROM event_log
            WHERE event_type = %s
            ORDER BY occurred_at ASC, created_at ASC
            """,
            (str(EventType.INVENTORY_RESERVED),),
        ).fetchall()

    return {
        str(row["aggregate_id"]): list(row["payload"].get("items", []))
        for row in rows
        if isinstance(row["payload"], dict)
    }


async def handle_order_paid(
    event: EventEnvelope,
    event_bus: InMemoryEventBus | RabbitMQEventBus,
    stock_state: dict[str, int] = stock,
    reservation_state: dict[str, list[dict[str, object]]] = reservations,
) -> None:
    if event.aggregate_id in reservation_state or await _has_inventory_decision(event.aggregate_id):
        return

    items = event.payload["items"]
    current_stock = await _stock_from_event_log() if _database_enabled() else stock_state
    shortages = [
        {"sku": item["sku"], "requested": item["quantity"], "available": current_stock.get(item["sku"], 0)}
        for item in items
        if current_stock.get(item["sku"], 0) < item["quantity"]
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
        current_stock[sku] -= quantity
        reserved_items.append({"sku": sku, "quantity": quantity})

    if current_stock is not stock_state:
        stock_state.clear()
        stock_state.update(current_stock)
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
    return await _stock_from_event_log()


@app.get("/reservations")
async def list_inventory_reservations() -> dict[str, list[dict[str, object]]]:
    return await _inventory_reservations_from_event_log()
