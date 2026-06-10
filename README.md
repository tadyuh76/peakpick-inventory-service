# PeakPick Inventory Service

Inventory Service là microservice quyết định giữ hàng và trừ tồn cho đơn đã thanh toán.

## Database Riêng

Service này sở hữu database `peakpick_inventory` với bảng:

- `event_log`

Trong prototype, tồn kho được mô phỏng đủ đơn giản để dễ demo và giải thích.

## Event

Nhận event:

- `OrderPaid`
- `OrderPickedUp`

Phát event:

- `InventoryReserved`
- `InventoryShortageDetected`

## Chạy Local

```bash
pip install -r requirements.txt
uvicorn services.inventory_service.main:app --reload --port 8005
```
