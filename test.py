import os
import pandas as pd

from datetime import timedelta
from feast import (
    Entity,
    FeatureView,
    Field,
    FileSource,
)
from feast.feature_store import FeatureStore
from feast.types import Float64, Int64

import logging
logging.basicConfig(level=logging.DEBUG)

from memory_profiler import profile

def add_customer_id(df):
    df['customer_id'] = 1

def add_timestamps(df):
    # Create time series: one entry every 1H, up to now
    timestamps = pd.date_range(
        end=pd.Timestamp.now().replace(microsecond=0), 
        periods=len(df), 
        freq='1H').to_frame(name="ts", index=False)

    timestamps['created'] = timestamps['ts']
    df = pd.concat(objs=[df, timestamps], axis=1)
    columns = df.columns.tolist()
    columns.insert(0, columns.pop(8))
    columns.insert(1, columns.pop(9))
    columns.insert(2, columns.pop(10))
    return df[columns]

xtrain = pd.read_csv('data/train.csv')
add_customer_id(xtrain)
xtrain = add_timestamps(xtrain)
xtrain.to_parquet('data/train.parquet')
print("-----xtrain-----")
print(xtrain.info())

print("Creating FeatureStore")
store = FeatureStore('./feature_repo')
print(f"offline_store is {store._provider.offline_store}")

train_source = FileSource(
    name="train_source",
    path="data/train.parquet",
    timestamp_field="ts",
    created_timestamp_column="created",
)
store.registry.apply_data_source(train_source, store.project)

customer = Entity(name="customer", join_keys=["customer_id"])
store.registry.apply_entity(customer, store.project)

training_fv = FeatureView(
    name="training_fv",
    entities=[customer],
    ttl=timedelta(days=1),
    schema=[
        Field(name="customer_id", dtype=Int64),
        Field(name="distance_from_last_transaction", dtype=Float64),
        Field(name="ratio_to_median_purchase_price", dtype=Float64),
        Field(name="used_chip", dtype=Float64),
        Field(name="used_pin_number", dtype=Float64),
        Field(name="online_order", dtype=Float64),
        Field(name="fraud", dtype=Float64),
    ],
    online=True,
    source=train_source,
    tags={"team": "training"},
)
store.registry.apply_feature_view(training_fv, store.project)

print("FeatureStore content:")
for ds in store.registry.list_data_sources(store.project):
    print(f"{type(ds).__name__}/{ds.name}")
for e in store.registry.list_entities(store.project):
    print(f"{type(e).__name__}/{e.name}")
for fv in store.registry.list_feature_views(store.project):
    print(f"{type(fv).__name__}/{fv.name}")

@profile
def fetch_historical_data(fv_name, df):
    # Fetch historical data
    # TODO: how to fetch real timestamps?
    datetimes = df['ts'].dt.to_pydatetime().tolist()
    entity_df = pd.DataFrame.from_dict(
        {
            "customer_id": [1] * len(datetimes),
            "event_timestamp": datetimes,
        }
    )
    print(f"Fetching {len(datetimes)} historical rows from {fv_name}")
    
    features=[
        f"{fv_name}:distance_from_last_transaction",
        f"{fv_name}:ratio_to_median_purchase_price",
        f"{fv_name}:used_chip",
        f"{fv_name}:used_pin_number",
        f"{fv_name}:online_order",
        f"{fv_name}:fraud",
    ]

    historical_df = pd.DataFrame()
    batch_size = 10000
    offset = 0
    while offset < len(entity_df):
        end_index = min(len(entity_df), offset + batch_size)
        print(f"Fetching rows from {offset} to {end_index}")
        batch_entity_df = pd.DataFrame.from_dict(
            {
                "customer_id": [1] * (end_index - offset),
                "event_timestamp": entity_df['event_timestamp'][offset: end_index],
            }
        )

        offset += batch_size
        batch_df = store.get_historical_features(
            entity_df=batch_entity_df,
            features=features,
        ).to_df()
        historical_df = pd.concat([historical_df, batch_df], ignore_index=True)
    
    return historical_df

train_df = fetch_historical_data('training_fv', xtrain)

print(f"Fetched {len(train_df)} historical records")

print(train_df.head())