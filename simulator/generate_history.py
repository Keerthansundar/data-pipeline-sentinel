"""Generate 30 days of seeded synthetic historical data into source_archive/."""

import argparse
import csv
import json
import os
import random
from datetime import date, datetime, timedelta
from pathlib import Path

from faker import Faker

from pipeline.config import get_project_root
from pipeline.logging_config import get_logger

logger = get_logger(__name__)


def generate_history(
    days: int = 30,
    orders_per_day: int = 5000,
    seed: int = 42,
    start_date: str = "2026-10-01",
    target_dir: Path | None = None,
) -> None:
    """Generate historical orders and customers files.

    Args:
        days: Number of days of history to create.
        orders_per_day: Base number of daily orders.
        seed: Random seed for reproducibility.
        start_date: Starting batch date in YYYY-MM-DD format.
        target_dir: Output directory (defaults to data/source_archive).
    """
    if target_dir is None:
        target_dir = get_project_root() / "data" / "source_archive"
    target_dir.mkdir(parents=True, exist_ok=True)

    # Initialize seeds
    random.seed(seed)
    Faker.seed(seed)
    fake = Faker()

    # Read ORDERS_PER_DAY env var if present
    env_orders = os.getenv("ORDERS_PER_DAY")
    if env_orders and env_orders.isdigit():
        orders_per_day = int(env_orders)

    logger.info(
        "Starting synthetic data generation: days=%d, orders_per_day=%d, seed=%d, start_date=%s",
        days,
        orders_per_day,
        seed,
        start_date,
    )

    # 1. Pre-generate reusable customer pool
    customer_pool_size = 1500
    customers = []
    segments = ["Consumer", "Corporate", "Small Business"]
    start_dt = datetime.strptime(start_date, "%Y-%m-%d").date()

    for idx in range(1, customer_pool_size + 1):
        cust_id = f"CUST-{idx:05d}"
        signup = start_dt - timedelta(days=random.randint(10, 365))
        customers.append(
            {
                "customer_id": cust_id,
                "name": fake.name(),
                "email": fake.email(),
                "city": fake.city(),
                "segment": random.choice(segments),
                "signup_date": signup.strftime("%Y-%m-%d"),
            }
        )

    statuses = ["placed", "shipped", "cancelled", "returned"]
    status_weights = [0.60, 0.25, 0.10, 0.05]
    channels = ["web", "mobile", "store", "partner"]

    order_fieldnames = [
        "order_id",
        "customer_id",
        "order_date",
        "amount",
        "status",
        "channel",
    ]

    total_orders_generated = 0
    total_quarantine_candidates = 0

    for day_idx in range(days):
        current_date: date = start_dt + timedelta(days=day_idx)
        date_str = current_date.strftime("%Y-%m-%d")

        # A. Write customers_<date>.json (JSON lines)
        cust_file = target_dir / f"customers_{date_str}.json"
        # Sample a subset of active customers for the day (e.g., 200-400 updates)
        daily_cust_sample = random.sample(customers, min(len(customers), random.randint(250, 400)))
        with open(cust_file, "w", encoding="utf-8") as f:
            for cust in daily_cust_sample:
                f.write(json.dumps(cust) + "\n")

        # B. Write orders_<date>.csv
        orders_file = target_dir / f"orders_{date_str}.csv"
        day_quarantine_count = 0

        with open(orders_file, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=order_fieldnames, lineterminator="\n")
            writer.writeheader()

            for order_idx in range(1, orders_per_day + 1):
                order_id = f"ORD-{current_date.strftime('%Y%m%d')}-{order_idx:05d}"
                cust = random.choice(customers)
                customer_id = cust["customer_id"]
                amount = round(random.uniform(10.0, 500.0), 2)
                status = random.choices(statuses, weights=status_weights, k=1)[0]
                channel = random.choice(channels)

                # Inject ~0.5% natural noise (null customer_id or negative amount)
                noise_roll = random.random()
                if noise_roll < 0.0025:
                    customer_id = ""  # null customer_id
                    day_quarantine_count += 1
                elif noise_roll < 0.0050:
                    amount = -round(random.uniform(5.0, 50.0), 2)  # negative amount
                    day_quarantine_count += 1

                writer.writerow(
                    {
                        "order_id": order_id,
                        "customer_id": customer_id,
                        "order_date": date_str,
                        "amount": f"{amount:.2f}",
                        "status": status,
                        "channel": channel,
                    }
                )

        total_orders_generated += orders_per_day
        total_quarantine_candidates += day_quarantine_count

    logger.info(
        "Successfully generated %d days of data in %s (total_orders=%d, quarantine_noise=%d)",
        days,
        target_dir,
        total_orders_generated,
        total_quarantine_candidates,
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate synthetic data history")
    parser.add_argument("--days", type=int, default=30)
    parser.add_argument("--orders-per-day", type=int, default=5000)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--start-date", type=str, default="2026-10-01")
    args = parser.parse_args()
    generate_history(args.days, args.orders_per_day, args.seed, args.start_date)
