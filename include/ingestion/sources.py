"""Source registry: one entry per upstream file.

Each source declares the exact header it is expected to have (its schema
contract) and how it is loaded:

* ``reference``     - small/slow-changing tables, full refresh when the file checksum changes
* ``orders``        - the driving transactional table, loaded by purchase-date interval
* ``order_child``   - rows that belong to an order, loaded for the orders in the same batch
* ``order_customer``- customers referenced by the orders in the batch
"""
from dataclasses import dataclass

BASE_URL = "https://raw.githubusercontent.com/olist/work-at-olist-data/master/datasets"


@dataclass(frozen=True)
class Source:
    name: str            # raw table name
    file: str            # upstream file name
    columns: tuple       # expected header, in order (schema contract)
    load_type: str       # reference | orders | order_child | order_customer


SOURCES = [
    Source(
        "orders", "olist_orders_dataset.csv",
        ("order_id", "customer_id", "order_status", "order_purchase_timestamp",
         "order_approved_at", "order_delivered_carrier_date",
         "order_delivered_customer_date", "order_estimated_delivery_date"),
        "orders",
    ),
    Source(
        "order_items", "olist_order_items_dataset.csv",
        ("order_id", "order_item_id", "product_id", "seller_id",
         "shipping_limit_date", "price", "freight_value"),
        "order_child",
    ),
    Source(
        "order_payments", "olist_order_payments_dataset.csv",
        ("order_id", "payment_sequential", "payment_type",
         "payment_installments", "payment_value"),
        "order_child",
    ),
    Source(
        "order_reviews", "olist_order_reviews_dataset.csv",
        ("review_id", "order_id", "review_score", "review_comment_title",
         "review_comment_message", "review_creation_date", "review_answer_timestamp"),
        "order_child",
    ),
    Source(
        "customers", "olist_customers_dataset.csv",
        ("customer_id", "customer_unique_id", "customer_zip_code_prefix",
         "customer_city", "customer_state"),
        "order_customer",
    ),
    Source(
        "products", "olist_products_dataset.csv",
        ("product_id", "product_category_name", "product_name_lenght",
         "product_description_lenght", "product_photos_qty", "product_weight_g",
         "product_length_cm", "product_height_cm", "product_width_cm"),
        "reference",
    ),
    Source(
        "sellers", "olist_sellers_dataset.csv",
        ("seller_id", "seller_zip_code_prefix", "seller_city", "seller_state"),
        "reference",
    ),
    Source(
        "category_translation", "product_category_name_translation.csv",
        ("product_category_name", "product_category_name_english"),
        "reference",
    ),
    Source(
        "geolocation", "olist_geolocation_dataset.csv",
        ("geolocation_zip_code_prefix", "geolocation_lat", "geolocation_lng",
         "geolocation_city", "geolocation_state"),
        "reference",
    ),
]

SOURCES_BY_NAME = {s.name: s for s in SOURCES}
