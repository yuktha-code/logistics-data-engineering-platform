"""Deterministic, fully simulated logistics data. No Porter/customer records used."""
from __future__ import annotations
import csv
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path

CITIES = [
    ("BLR", "Bengaluru", "Karnataka", 12.9716, 77.5946),
    ("HYD", "Hyderabad", "Telangana", 17.3850, 78.4867),
    ("DEL", "Delhi", "Delhi", 28.6139, 77.2090),
    ("MUM", "Mumbai", "Maharashtra", 19.0760, 72.8777),
    ("PUN", "Pune", "Maharashtra", 18.5204, 73.8567),
    ("CHE", "Chennai", "Tamil Nadu", 13.0827, 80.2707),
    ("KOL", "Kolkata", "West Bengal", 22.5726, 88.3639),
    ("AHM", "Ahmedabad", "Gujarat", 23.0225, 72.5714),
]


def _stamp(ts: datetime | None) -> str:
    return ts.strftime("%Y-%m-%dT%H:%M:%SZ") if ts else ""


def _csv(path: Path, columns: list[str], rows: list[dict]) -> None:
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=columns)
        w.writeheader()
        w.writerows(rows)


def generate(raw_dir: Path, n_orders: int = 6000, seed: int = 42) -> dict:
    """Generate joined customer, driver, order and event tables with stable IDs."""
    if n_orders < 1:
        raise ValueError("n_orders must be positive")
    raw_dir.mkdir(parents=True, exist_ok=True)
    rng = random.Random(seed)
    cities = [dict(city_id=c[0], city=c[1], state=c[2], latitude=c[3], longitude=c[4]) for c in CITIES]
    n_customers = 2000  # fixed size preserves existing IDs across incremental expansions
    customers = [dict(customer_id=f"C{i:06d}", city_id=rng.choice(CITIES)[0]) for i in range(n_customers)]
    drivers = [dict(driver_id=f"D{c[0]}_{i:05d}", city_id=c[0]) for c in CITIES for i in range(1, 71)]
    city_drivers: dict[str, list[str]] = {c[0]: [d["driver_id"] for d in drivers if d["city_id"] == c[0]] for c in CITIES}
    base = datetime(2026, 1, 1, tzinfo=timezone.utc)
    orders: list[dict] = []
    events: list[dict] = []
    for i in range(n_orders):
        customer = rng.choice(customers)
        city_id = customer["city_id"]
        cid = f"O{i+1:08d}"
        driver = rng.choice(city_drivers[city_id])
        t0 = base + timedelta(days=rng.randrange(90), minutes=rng.randint(0, 1439))
        pickup = t0 + timedelta(minutes=rng.randint(5, 40))
        travel_mins = rng.randint(20, 170)
        promised = t0 + timedelta(minutes=rng.randint(75, 160))
        state_roll = rng.random()
        if state_roll < 0.055:
            status = "CANCELLED"
            pickup = None
            arrival = None
        elif state_roll < 0.095:
            status = "FAILED"
            arrival = None
        else:
            status = "DELIVERED"
            arrival = (pickup + timedelta(minutes=travel_mins))
        distance = round(rng.uniform(1.5, 26.0), 2)
        fare = round(48 + distance * rng.uniform(13, 23), 2)
        orders.append(dict(order_id=cid, customer_id=customer["customer_id"], driver_id=driver,
                           city_id=city_id, created_at=_stamp(t0), picked_up_at=_stamp(pickup),
                           promised_at=_stamp(promised), delivered_at=_stamp(arrival),
                           status=status, distance_km=distance, fare_inr=fare, source="synthetic"))
        sequence = [("CREATED", t0)]
        if pickup:
            sequence.append(("PICKED_UP", pickup))
            if status == "DELIVERED":
                sequence.append(("IN_TRANSIT", pickup + (arrival - pickup) / 2))
                sequence.append(("DELIVERED", arrival))
            else:
                sequence.append(("FAILED", pickup + timedelta(minutes=travel_mins)))
        else:
            sequence.append(("CANCELLED", t0 + timedelta(minutes=rng.randint(1, 12))))
        geo = next(c for c in CITIES if c[0] == city_id)
        for j, (event_type, t) in enumerate(sequence):
            ingest = t + timedelta(minutes=rng.randint(0, 120))
            events.append(dict(event_id=f"E{i+1:08d}_{j}", order_id=cid, event_type=event_type,
                               event_at=_stamp(t), ingested_at=_stamp(ingest),
                               latitude=round(geo[3] + rng.uniform(-0.055, 0.055), 5),
                               longitude=round(geo[4] + rng.uniform(-0.055, 0.055), 5),
                               source="synthetic"))
    # Deliberately duplicate 1% of complete event records to exercise stable-key deduplication.
    duplicates = min(len(events), max(1, n_orders // 100))
    events.extend(dict(e) for e in events[:duplicates])
    rng.shuffle(events)  # Event arrival order does not equal event time order.
    _csv(raw_dir / "cities.csv", list(cities[0]), cities)
    _csv(raw_dir / "customers.csv", list(customers[0]), customers)
    _csv(raw_dir / "drivers.csv", list(drivers[0]), drivers)
    _csv(raw_dir / "orders.csv", list(orders[0]), orders)
    _csv(raw_dir / "events.csv", list(events[0]), events)
    return {"orders": len(orders), "events_raw": len(events), "events_duplicates": duplicates,
            "customers": len(customers), "drivers": len(drivers), "cities": len(cities)}
