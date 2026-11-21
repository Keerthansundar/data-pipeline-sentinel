"""Generate 30 days of seeded synthetic historical data into source_archive/."""

import argparse


def generate_history(days: int = 30, orders_per_day: int = 5000, seed: int = 42) -> None:
    """Generate historical orders and customers files.

    Args:
        days: Number of days of history to create.
        orders_per_day: Base number of daily orders.
        seed: Random seed for reproducibility.
    """
    pass


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate synthetic data history")
    parser.add_argument("--days", type=int, default=30)
    parser.add_argument("--orders-per-day", type=int, default=5000)
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()
    generate_history(args.days, args.orders_per_day, args.seed)
