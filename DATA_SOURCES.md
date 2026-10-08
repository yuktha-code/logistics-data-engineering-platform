# Provenance, usage rights and scope

## Default synthetic simulation

`src/synthetic.py` generates all local raw CSVs from a fixed `random.Random(seed)` with publicly readable code and no embedded private records. City coordinates are approximate centers shifted with random offsets. Names such as `O00000001` and `DDEL_00001` are invented IDs. **No Porter records, customers or drivers are involved.**

Generated tables: 8 Indian illustrative cities, 2,000 simulated customers, 560 simulated driver IDs, 6,000 simulated orders, delivery events with out-of-order ingestion and 60 injected identical duplicates at default settings. Subscription, vehicle positioning and route ETA telemetry are not modeled.

## Optional Olist public source

Source: [Brazilian E-Commerce Public Dataset by Olist](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce). Required externally obtained files:

- `olist_orders_dataset.csv`
- `olist_customers_dataset.csv`

This repository does not redistribute Olist data or assert download/publication rights beyond the linked dataset source; review the publisher's license before redistributing.

Source-specific assumptions:

- `customer_city`, `customer_state`: customer geographic context; not necessarily depot or seller location.
- `order_purchase_timestamp`: created date; `order_delivered_carrier_date`: courier handoff, not equivalent to driver tracking.
- `order_delivered_customer_date`: delivery timestamp.
- `order_estimated_delivery_date`: **date-level** estimate converted to end-of-day; records without this date are excluded instead of assigning a fabricated deadline for on-time comparison.
- Unavailable/unknown driver, fare and GPS fields remain NULL.
- Derived order milestone event rows are **synthetic reconstructions from existing Olist order fields**, not separately observed events.
- PENDING orders are excluded from delivered/on-time denominator; Olist order statuses not covered by delivered/cancelled are marked pending.
- Analyses must not claim their outcomes are Indian shipping performance or Porter KPIs.

## Why Olist is optional

An offline self-contained simulation runs without download, login, Kaggle API keys, Docker or proprietary logistics data. The Olist adapter lets recruiters inspect what changes when historical third-party operational data is substituted, while explicitly preserving unavailable fields rather than inventing driver-level utilization or GPS readings.
