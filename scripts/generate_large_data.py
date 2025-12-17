#!/usr/bin/env python3
"""Generate large-scale sample datasets for the Azure Medallion Pipeline.

Generates:
  - data/sample_products.csv  (~500 rows)
  - data/sample_customers.csv (~200,000 rows)
  - data/sample_orders.csv    (~7,000,000 rows)

All generated data uses Spanish locale (es_ES) via Faker.
CSVs are written in chunks to keep memory usage under 2 GB.

Usage:
    python scripts/generate_large_data.py                          # defaults: 7M orders, 200K customers
    python scripts/generate_large_data.py --sample                 # quick test: 10K orders, 1K customers
    python scripts/generate_large_data.py --orders 5000000         # custom order count
    python scripts/generate_large_data.py --customers 100000       # custom customer count
"""

import argparse
import csv
import os
import random
import sys
from datetime import date, timedelta

from faker import Faker
from tqdm import tqdm

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
DATA_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data"
)
CHUNK_SIZE = 500_000

REGIONS = [
    "Madrid",
    "Barcelona",
    "Valencia",
    "Bilbao",
    "Sevilla",
    "Málaga",
    "Zaragoza",
    "Alicante",
    "Murcia",
]

CATEGORIES = {
    "Electrónica": ["Smartphones", "Portátiles", "Auriculares", "Monitores", "Tablets"],
    "Ropa": ["Camisetas", "Pantalones", "Vestidos", "Chaquetas", "Zapatos"],
    "Hogar": ["Muebles", "Decoración", "Cocina", "Iluminación", "Jardín"],
    "Deportes": ["Fútbol", "Running", "Ciclismo", "Natación", "Gimnasio"],
    "Alimentación": ["Bebidas", "Snacks", "Congelados", "Frescos", "Ecológicos"],
}

PRODUCT_ADJECTIVES = [
    "Premium",
    "Eco",
    "Smart",
    "Pro",
    "Ultra",
    "Max",
    "Lite",
    "Plus",
    "Compact",
    "Digital",
    "Turbo",
    "Elite",
    "Classic",
    "Fresh",
    "Speed",
]

SUBCATEGORY_NAMES = []
for cat, subs in CATEGORIES.items():
    for sub in subs:
        SUBCATEGORY_NAMES.append((cat, sub))

LOYALTY_TIERS = ["bronze", "silver", "gold", "platinum"]

SUPPLIER_COUNTRIES = [
    "España",
    "Francia",
    "Alemania",
    "Italia",
    "Portugal",
    "China",
    "Japón",
    "Estados Unidos",
    "Reino Unido",
    "Países Bajos",
]

DATE_START = date(2023, 1, 1)
DATE_END = date(2026, 3, 21)
DATE_SPAN = (DATE_END - DATE_START).days


# ---------------------------------------------------------------------------
# Generators
# ---------------------------------------------------------------------------


def generate_products(num_products: int = 500) -> list[dict]:
    """Return a list of product dicts with stable product_ids."""
    fake = Faker("es_ES")
    products = []
    used_names = set()
    for i in range(1, num_products + 1):
        cat_idx = (i - 1) % len(SUBCATEGORY_NAMES)
        category, subcategory = SUBCATEGORY_NAMES[cat_idx]
        adj = random.choice(PRODUCT_ADJECTIVES)
        base_name = f"{adj} {subcategory}"
        # Deduplicate names
        name = base_name
        suffix = 2
        while name in used_names:
            name = f"{base_name} v{suffix}"
            suffix += 1
        used_names.add(name)

        unit_price = round(random.uniform(5.0, 999.99), 2)
        products.append(
            {
                "product_id": f"PROD-{i:03d}",
                "product_name": name,
                "category": category,
                "subcategory": subcategory,
                "unit_price": unit_price,
                "supplier_country": random.choice(SUPPLIER_COUNTRIES),
            }
        )
    return products


def write_products_csv(products: list[dict]) -> str:
    path = os.path.join(DATA_DIR, "sample_products.csv")
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "product_id",
                "product_name",
                "category",
                "subcategory",
                "unit_price",
                "supplier_country",
            ],
        )
        writer.writeheader()
        writer.writerows(products)
    return path


def write_customers_csv(num_customers: int) -> tuple[str, list[int]]:
    """Write customers CSV and return (path, list of customer IDs)."""
    path = os.path.join(DATA_DIR, "sample_customers.csv")
    fake = Faker("es_ES")
    fieldnames = [
        "customer_id",
        "customer_name",
        "email",
        "country",
        "signup_date",
        "loyalty_tier",
    ]

    # Assign a default unit_price per product_id (price from orders comes from
    # the order row itself, not the product table, to keep the pipeline simple).
    tier_weights = [0.50, 0.30, 0.15, 0.05]  # bronze/silver/gold/platinum

    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        for i in tqdm(
            range(1, num_customers + 1), desc="Generating customers", unit="rows"
        ):
            signup = DATE_START + timedelta(days=random.randint(0, DATE_SPAN))
            tier = random.choices(LOYALTY_TIERS, weights=tier_weights, k=1)[0]
            writer.writerow(
                {
                    "customer_id": f"CUST-{i:05d}",
                    "customer_name": fake.name(),
                    "email": fake.email(),
                    "country": "España",
                    "signup_date": signup.isoformat(),
                    "loyalty_tier": tier,
                }
            )

    customer_ids = list(range(1, num_customers + 1))
    return path, customer_ids


def write_orders_csv(
    num_orders: int,
    num_customers: int,
    product_price_map: dict[int, float],
) -> str:
    """Write orders CSV in chunks to avoid memory issues.

    Each chunk of CHUNK_SIZE rows is appended to the file.
    Product unit_price in the order row is randomly chosen (5.00-999.99)
    independent of the product catalog price — this matches the original
    sample CSV schema where unit_price is a per-order value.
    """
    path = os.path.join(DATA_DIR, "sample_orders.csv")
    fieldnames = [
        "order_id",
        "customer_id",
        "product_id",
        "quantity",
        "unit_price",
        "order_date",
        "region",
    ]

    num_chunks = (num_orders + CHUNK_SIZE - 1) // CHUNK_SIZE

    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        written = 0
        with tqdm(total=num_orders, desc="Generating orders", unit="rows") as pbar:
            for chunk_idx in range(num_chunks):
                chunk_rows = min(CHUNK_SIZE, num_orders - written)
                rows = []
                for _ in range(chunk_rows):
                    written += 1
                    customer_idx = random.randint(1, num_customers)
                    product_idx = random.randint(1, len(product_price_map))
                    order_date = DATE_START + timedelta(
                        days=random.randint(0, DATE_SPAN)
                    )
                    rows.append(
                        {
                            "order_id": f"ORD-{written:07d}",
                            "customer_id": f"CUST-{customer_idx:05d}",
                            "product_id": f"PROD-{product_idx:03d}",
                            "quantity": random.randint(1, 20),
                            "unit_price": round(random.uniform(5.0, 999.99), 2),
                            "order_date": order_date.isoformat(),
                            "region": random.choice(REGIONS),
                        }
                    )
                writer.writerows(rows)
                pbar.update(chunk_rows)

    return path


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def parse_args():
    parser = argparse.ArgumentParser(
        description="Generate large-scale sample data for the Azure Medallion Pipeline."
    )
    parser.add_argument(
        "--orders",
        type=int,
        default=7_000_000,
        help="Number of order rows to generate (default: 7,000,000)",
    )
    parser.add_argument(
        "--customers",
        type=int,
        default=200_000,
        help="Number of customer rows to generate (default: 200,000)",
    )
    parser.add_argument(
        "--products",
        type=int,
        default=500,
        help="Number of product rows to generate (default: 500)",
    )
    parser.add_argument(
        "--sample",
        action="store_true",
        help="Generate a small sample (10K orders, 1K customers) for quick testing",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="Random seed for reproducibility (default: 42)",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    random.seed(args.seed)
    Faker.seed(args.seed)

    if args.sample:
        num_orders = 10_000
        num_customers = 1_000
        num_products = 50
    else:
        num_orders = args.orders
        num_customers = args.customers
        num_products = args.products

    os.makedirs(DATA_DIR, exist_ok=True)

    print(f"Generating data in: {DATA_DIR}")
    print(f"  Products : {num_products:,}")
    print(f"  Customers: {num_customers:,}")
    print(f"  Orders   : {num_orders:,}")
    print()

    # Step 1: Products (small — keep in memory)
    products = generate_products(num_products)
    product_price_map = {i: p["unit_price"] for i, p in enumerate(products, 1)}
    p_path = write_products_csv(products)
    print(f"Wrote {len(products):,} products → {p_path}")

    # Step 2: Customers (keep IDs in memory for referential integrity)
    c_path, customer_ids = write_customers_csv(num_customers)
    print(f"Wrote {len(customer_ids):,} customers → {c_path}")

    # Step 3: Orders (streamed in chunks)
    o_path = write_orders_csv(num_orders, len(customer_ids), product_price_map)
    print(f"Wrote {num_orders:,} orders → {o_path}")

    print()
    print("Done. All referential integrity preserved:")
    print(f"  customer_id range: CUST-00001 to CUST-{num_customers:05d}")
    print(f"  product_id range : PROD-001 to PROD-{num_products:03d}")
    print(f"  order_id range   : ORD-0000001 to ORD-{num_orders:07d}")


if __name__ == "__main__":
    main()
