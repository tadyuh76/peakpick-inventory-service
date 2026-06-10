# PeakPick Inventory Service

Owns inventory reservation decisions. It consumes `OrderPaid` and publishes either `InventoryReserved` or `InventoryShortageDetected`.

Owned database tables:

- local `event_log`

Run locally:

```bash
pip install -r requirements.txt
uvicorn services.inventory_service.main:app --reload --port 8005
```
