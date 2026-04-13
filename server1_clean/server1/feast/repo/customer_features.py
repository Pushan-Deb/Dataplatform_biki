from datetime import timedelta

from feast import Entity, FeatureService, FeatureView, Field, FileSource
from feast.types import Float32, Int64, String
from feast.value_type import ValueType


customer = Entity(name="customer", join_keys=["customer_id"], value_type=ValueType.INT64)

customer_source = FileSource(
    name="customer_source",
    path="/opt/feast/repo/data/customer_features.parquet",
    timestamp_field="event_timestamp",
    created_timestamp_column="created_timestamp",
)

customer_profile_features = FeatureView(
    name="customer_profile_features",
    entities=[customer],
    ttl=timedelta(days=3650),
    schema=[
        Field(name="account_age_days", dtype=Int64),
        Field(name="avg_order_value", dtype=Float32),
        Field(name="segment", dtype=String),
    ],
    online=True,
    source=customer_source,
    tags={"owner": "dataplatform", "purpose": "demo bootstrap"},
)

customer_profile_service = FeatureService(
    name="customer_profile_service",
    features=[customer_profile_features],
)
