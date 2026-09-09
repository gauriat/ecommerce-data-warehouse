"""
Generates synthetic e-commerce data with the exact same column shapes as the
real Olist CSVs, so the whole pipeline (load -> dbt -> Airflow) can be
demoed/tested without a Kaggle account.

Usage:
    python scripts/generate_sample_data.py --n-orders 5000
"""
import argparse
import random
import string
import uuid
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd

DATA_DIR = Path(__file__).resolve().parent.parent / "data"

CATEGORIES = [
    ("cama_mesa_banho", "bed_bath_table"),
    ("beleza_saude", "health_beauty"),
    ("esporte_lazer", "sports_leisure"),
    ("informatica_acessorios", "computers_accessories"),
    ("moveis_decoracao", "furniture_decor"),
    ("brinquedos", "toys"),
    ("telefonia", "telephony"),
    ("automotivo", "auto"),
    ("cool_stuff", "cool_stuff"),
    ("papelaria", "stationery"),
]
STATES = ["SP", "RJ", "MG", "RS", "PR", "SC", "BA", "GO", "PE", "CE"]
CITIES = {
    "SP": "sao paulo", "RJ": "rio de janeiro", "MG": "belo horizonte",
    "RS": "porto alegre", "PR": "curitiba", "SC": "florianopolis",
    "BA": "salvador", "GO": "goiania", "PE": "recife", "CE": "fortaleza",
}
PAYMENT_TYPES = ["credit_card", "boleto", "voucher", "debit_card"]
ORDER_STATUSES = ["delivered", "shipped", "processing", "canceled", "invoiced", "delivered", "delivered"]


def rid() -> str:
    return uuid.uuid4().hex


def rand_zip() -> str:
    return "".join(random.choices(string.digits, k=5))


def main(n_orders: int, n_customers: int, n_products: int, n_sellers: int, seed: int) -> None:
    random.seed(seed)
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    customers = []
    for _ in range(n_customers):
        state = random.choice(STATES)
        customers.append({
            "customer_id": rid(),
            "customer_unique_id": rid(),
            "customer_zip_code_prefix": rand_zip(),
            "customer_city": CITIES[state],
            "customer_state": state,
        })
    customers_df = pd.DataFrame(customers)

    sellers = []
    for _ in range(n_sellers):
        state = random.choice(STATES)
        sellers.append({
            "seller_id": rid(),
            "seller_zip_code_prefix": rand_zip(),
            "seller_city": CITIES[state],
            "seller_state": state,
        })
    sellers_df = pd.DataFrame(sellers)

    products, translations = [], []
    seen_cats = set()
    for _ in range(n_products):
        cat_pt, cat_en = random.choice(CATEGORIES)
        if cat_pt not in seen_cats:
            translations.append({"product_category_name": cat_pt, "product_category_name_english": cat_en})
            seen_cats.add(cat_pt)
        products.append({
            "product_id": rid(),
            "product_category_name": cat_pt,
            "product_name_lenght": random.randint(10, 60),
            "product_description_lenght": random.randint(50, 2000),
            "product_photos_qty": random.randint(1, 8),
            "product_weight_g": random.randint(100, 15000),
            "product_length_cm": random.randint(5, 100),
            "product_height_cm": random.randint(5, 100),
            "product_width_cm": random.randint(5, 100),
        })
    products_df = pd.DataFrame(products)
    translation_df = pd.DataFrame(translations)

    orders, items, payments, reviews = [], [], [], []
    start_date = datetime(2023, 1, 1)
    for i in range(n_orders):
        order_id = rid()
        customer = random.choice(customers_df["customer_id"].tolist())
        purchase_ts = start_date + timedelta(
            days=random.randint(0, 550), seconds=random.randint(0, 86399)
        )
        status = random.choice(ORDER_STATUSES)
        approved_ts = purchase_ts + timedelta(hours=random.randint(1, 48))
        delivered_carrier_ts = approved_ts + timedelta(days=random.randint(1, 5))
        delivered_customer_ts = delivered_carrier_ts + timedelta(days=random.randint(1, 10))
        estimated_ts = purchase_ts + timedelta(days=random.randint(7, 25))

        orders.append({
            "order_id": order_id,
            "customer_id": customer,
            "order_status": status,
            "order_purchase_timestamp": purchase_ts,
            "order_approved_at": approved_ts if status != "canceled" else None,
            "order_delivered_carrier_date": delivered_carrier_ts if status == "delivered" else None,
            "order_delivered_customer_date": delivered_customer_ts if status == "delivered" else None,
            "order_estimated_delivery_date": estimated_ts,
        })

        n_items = random.randint(1, 3)
        order_value = 0.0
        for item_idx in range(1, n_items + 1):
            product_id = random.choice(products_df["product_id"].tolist())
            seller_id = random.choice(sellers_df["seller_id"].tolist())
            price = round(random.uniform(15, 500), 2)
            freight = round(random.uniform(5, 60), 2)
            order_value += price + freight
            items.append({
                "order_id": order_id,
                "order_item_id": item_idx,
                "product_id": product_id,
                "seller_id": seller_id,
                "shipping_limit_date": approved_ts + timedelta(days=random.randint(1, 7)),
                "price": price,
                "freight_value": freight,
            })

        payments.append({
            "order_id": order_id,
            "payment_sequential": 1,
            "payment_type": random.choice(PAYMENT_TYPES),
            "payment_installments": random.randint(1, 10),
            "payment_value": round(order_value, 2),
        })

        if status == "delivered" and random.random() < 0.7:
            reviews.append({
                "review_id": rid(),
                "order_id": order_id,
                "review_score": random.randint(1, 5),
                "review_comment_title": None,
                "review_comment_message": None,
                "review_creation_date": delivered_customer_ts + timedelta(days=random.randint(0, 3)),
                "review_answer_timestamp": delivered_customer_ts + timedelta(days=random.randint(3, 6)),
            })

    orders_df = pd.DataFrame(orders)
    items_df = pd.DataFrame(items)
    payments_df = pd.DataFrame(payments)
    reviews_df = pd.DataFrame(reviews)

    geo = []
    for zp in set(customers_df["customer_zip_code_prefix"]).union(sellers_df["seller_zip_code_prefix"]):
        state = random.choice(STATES)
        geo.append({
            "geolocation_zip_code_prefix": zp,
            "geolocation_lat": round(random.uniform(-33, 5), 6),
            "geolocation_lng": round(random.uniform(-73, -35), 6),
            "geolocation_city": CITIES[state],
            "geolocation_state": state,
        })
    geo_df = pd.DataFrame(geo)

    outputs = {
        "olist_customers_dataset.csv": customers_df,
        "olist_sellers_dataset.csv": sellers_df,
        "olist_products_dataset.csv": products_df,
        "product_category_name_translation.csv": translation_df,
        "olist_orders_dataset.csv": orders_df,
        "olist_order_items_dataset.csv": items_df,
        "olist_order_payments_dataset.csv": payments_df,
        "olist_order_reviews_dataset.csv": reviews_df,
        "olist_geolocation_dataset.csv": geo_df,
    }
    for filename, df in outputs.items():
        path = DATA_DIR / filename
        df.to_csv(path, index=False)
        print(f"wrote {len(df):>6} rows -> {path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--n-orders", type=int, default=5000)
    parser.add_argument("--n-customers", type=int, default=1500)
    parser.add_argument("--n-products", type=int, default=300)
    parser.add_argument("--n-sellers", type=int, default=150)
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()
    main(args.n_orders, args.n_customers, args.n_products, args.n_sellers, args.seed)