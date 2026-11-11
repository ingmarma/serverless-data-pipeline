"""
Generate realistic sample CSV data for the pipeline.

Usage:
    python generate_sample_data.py --records 1000000 --output ./sample-data/
    python generate_sample_data.py --records 100000000 --output ./sample-data/ --files 100

Author: @ingmarma | MarmaTech
"""

import argparse
import csv
import os
import random
import uuid
from datetime import datetime, timedelta
from pathlib import Path

# ============================================================
# DATA GENERATORS
# ============================================================

CATEGORIES = [
    "electronics", "clothing", "food_delivery", "groceries",
    "entertainment", "travel", "healthcare", "education",
    "software", "automotive", "home_garden", "sports",
    "beauty", "books", "gaming", "financial_services",
]

MERCHANTS = {
    "electronics": ["TechStore", "GadgetWorld", "CircuitCity", "MegaElectro"],
    "clothing": ["FashionHub", "UrbanWear", "StyleCo", "ThreadBarn"],
    "food_delivery": ["QuickBite", "FoodDash", "EatNow", "DeliverEats"],
    "groceries": ["FreshMart", "SuperSave", "GreenGrocer", "DailyNeeds"],
    "entertainment": ["StreamFlix", "GameZone", "CinePlex", "MusicHub"],
    "travel": ["FlyAway", "BookStay", "RoadTrip", "VoyageNow"],
    "healthcare": ["MedPlus", "HealthFirst", "CarePoint", "WellnessHub"],
    "education": ["LearnPro", "SkillUp", "EduPath", "CodeAcademy"],
    "software": ["CloudSoft", "DevTools", "SaaSPro", "AppForge"],
    "automotive": ["AutoParts", "CarCare", "DriveShop", "MotorHub"],
    "home_garden": ["HomeDepot", "GardenPro", "DecorHub", "FixItAll"],
    "sports": ["SportGear", "FitZone", "ActiveLife", "ProAthlete"],
    "beauty": ["GlowUp", "BeautyBar", "SkinLab", "StyleSalon"],
    "books": ["BookNook", "ReadMore", "PageTurner", "LitHub"],
    "gaming": ["GameVault", "PixelStore", "PlayZone", "ConsolePro"],
    "financial_services": ["PayFast", "InsureCo", "InvestPro", "BankPlus"],
}

COUNTRIES = ["US", "BR", "MX", "AR", "CO", "CL", "PE", "PY", "UY", "EC"]
CURRENCIES = {"US": "USD", "BR": "BRL", "MX": "MXN", "AR": "ARS", "CO": "COP",
              "CL": "CLP", "PE": "PEN", "PY": "PYG", "UY": "UYU", "EC": "USD"}
STATUSES = ["completed", "completed", "completed", "completed",
            "pending", "failed", "refunded"]  # weighted toward completed
PAYMENT_METHODS = ["credit_card", "debit_card", "digital_wallet",
                   "bank_transfer", "crypto"]

HEADERS = [
    "transaction_id", "transaction_date", "customer_id", "amount",
    "currency", "category", "merchant", "status", "country", "payment_method"
]


def generate_amount(category: str) -> float:
    """Generate realistic amount based on category."""
    ranges = {
        "electronics": (15.0, 2500.0),
        "clothing": (10.0, 500.0),
        "food_delivery": (8.0, 80.0),
        "groceries": (5.0, 300.0),
        "entertainment": (5.0, 100.0),
        "travel": (50.0, 5000.0),
        "healthcare": (20.0, 1000.0),
        "education": (10.0, 2000.0),
        "software": (5.0, 500.0),
        "automotive": (20.0, 3000.0),
        "home_garden": (10.0, 1500.0),
        "sports": (15.0, 800.0),
        "beauty": (10.0, 300.0),
        "books": (5.0, 80.0),
        "gaming": (5.0, 100.0),
        "financial_services": (10.0, 10000.0),
    }
    low, high = ranges.get(category, (5.0, 500.0))
    # Log-normal distribution for realistic skew
    amount = random.lognormvariate(0, 1) * (high - low) / 10 + low
    return round(min(amount, high * 1.5), 2)


def generate_record(base_date: datetime, date_range_days: int) -> list:
    """Generate a single transaction record."""
    country = random.choice(COUNTRIES)
    category = random.choice(CATEGORIES)
    merchant = random.choice(MERCHANTS[category])
    tx_date = base_date + timedelta(
        days=random.randint(0, date_range_days),
        hours=random.randint(0, 23),
        minutes=random.randint(0, 59),
        seconds=random.randint(0, 59),
    )

    # ~0.5% chance of invalid data (for testing validation)
    inject_bad = random.random() < 0.005
    amount = -random.uniform(1, 100) if inject_bad else generate_amount(category)
    status = "INVALID_STATUS" if (inject_bad and random.random() < 0.3) else random.choice(STATUSES)

    return [
        str(uuid.uuid4()),                     # transaction_id
        tx_date.strftime("%Y-%m-%d %H:%M:%S"), # transaction_date
        f"cust_{random.randint(1, 500000):07d}",# customer_id
        amount,                                 # amount
        CURRENCIES[country],                    # currency
        category,                               # category
        merchant,                               # merchant
        status,                                 # status
        country,                                # country
        random.choice(PAYMENT_METHODS),         # payment_method
    ]


def generate_csv(output_dir: str, total_records: int, num_files: int):
    """Generate CSV files with sample data."""
    Path(output_dir).mkdir(parents=True, exist_ok=True)

    records_per_file = total_records // num_files
    base_date = datetime(2025, 1, 1)
    date_range = 365  # 1 year of data

    print(f"Generating {total_records:,} records across {num_files} file(s)...")
    print(f"Date range: {base_date.date()} to {(base_date + timedelta(days=date_range)).date()}")
    print(f"Output: {output_dir}")
    print()

    total_written = 0

    for file_idx in range(num_files):
        filename = f"transactions_{file_idx:04d}.csv"
        filepath = os.path.join(output_dir, filename)

        # Last file gets remaining records
        records_this_file = records_per_file if file_idx < num_files - 1 else total_records - total_written

        with open(filepath, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(HEADERS)

            for _ in range(records_this_file):
                writer.writerow(generate_record(base_date, date_range))

            total_written += records_this_file

        size_mb = os.path.getsize(filepath) / (1024 * 1024)
        print(f"  [{file_idx + 1}/{num_files}] {filename}: {records_this_file:,} records ({size_mb:.1f} MB)")

    total_size = sum(
        os.path.getsize(os.path.join(output_dir, f))
        for f in os.listdir(output_dir) if f.endswith('.csv')
    ) / (1024 * 1024)

    print()
    print(f"Done! {total_written:,} records, {total_size:.1f} MB total")


# ============================================================
# CLI
# ============================================================

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate sample transaction data")
    parser.add_argument("--records", type=int, default=1_000_000,
                        help="Total number of records (default: 1M)")
    parser.add_argument("--files", type=int, default=1,
                        help="Number of CSV files to split into (default: 1)")
    parser.add_argument("--output", type=str, default="./sample-data/",
                        help="Output directory (default: ./sample-data/)")
    parser.add_argument("--seed", type=int, default=42,
                        help="Random seed for reproducibility (default: 42)")

    args = parser.parse_args()
    random.seed(args.seed)
    generate_csv(args.output, args.records, args.files)
