## Context
* Given the sample files from the [OpenShift AI tutorial - Fraud detection example](https://docs.redhat.com/en/documentation/red_hat_openshift_ai_self-managed/2-latest/html/openshift_ai_tutorial_-_fraud_detection_example/index), [repo](https://github.com/rh-aiservices-bu/fraud-detection)
* I want to model the train dataset as a Feast offline store using a `FileSource` and fetch historical features

## Problem description
The fetch of historical features never completes.

## Procedure
### Prerequisites
* Feast installed (either latest `dev` or > `0.40`)
### Steps
* Validate feast state
```bash
FEAST_REPO=./feature_repo
feast -c $FEAST_REPO apply
feast -c $FEAST_REPO entities list
feast -c $FEAST_REPO data-sources list
feast -c $FEAST_REPO feature-views list
```
* Run test code to:
  * Read original `train.csv`, manipulate it as a time-series and convert to a parquet file
  * Create required `FileSource`, `Entity` and `FeatureView`
  * Fetch historical features (in batches of 10,000)

```bash
python test.py
```
Example output:
```text
-----xtrain-----
<class 'pandas.core.frame.DataFrame'>
RangeIndex: 600000 entries, 0 to 599999
Data columns (total 11 columns):
 #   Column                          Non-Null Count   Dtype         
---  ------                          --------------   -----         
 0   customer_id                     600000 non-null  int64         
 1   ts                              600000 non-null  datetime64[ns]
 2   created                         600000 non-null  datetime64[ns]
 3   distance_from_home              600000 non-null  float64       
 4   distance_from_last_transaction  600000 non-null  float64       
 5   ratio_to_median_purchase_price  600000 non-null  float64       
 6   repeat_retailer                 600000 non-null  float64       
 7   used_chip                       600000 non-null  float64       
 8   used_pin_number                 600000 non-null  float64       
 9   online_order                    600000 non-null  float64       
 10  fraud                           600000 non-null  float64       
dtypes: datetime64[ns](2), float64(8), int64(1)
memory usage: 50.4 MB
None
Creating FeatureStore
offline_store is <feast.infra.offline_stores.dask.DaskOfflineStore object at 0x14c83d6d0>
FeatureStore content:
INFO:feast.infra.registry.registry:Registry cache expired, so refreshing
FileSource/train_source
INFO:feast.infra.registry.registry:Registry cache expired, so refreshing
Entity/__dummy
Entity/customer
INFO:feast.infra.registry.registry:Registry cache expired, so refreshing
FeatureView/training_fv
/Users/dmartino/projects/AI/feast/feast_issue_600K/test.py:90: FutureWarning: The behavior of DatetimeProperties.to_pydatetime is deprecated, in a future version this will return a Series containing python datetime objects instead of an ndarray. To retain the old behavior, call `np.array` on the result
  datetimes = df['ts'].dt.to_pydatetime().tolist()
Fetching 600000 historical rows from training_fv
Fetching rows from 0 to 10000
INFO:feast.infra.registry.registry:Registry cache expired, so refreshing
INFO:feast.infra.registry.registry:Registry cache expired, so refreshing
INFO:feast.infra.registry.registry:Registry cache expired, so refreshing
INFO:feast.infra.registry.registry:Registry cache expired, so refreshing
INFO:feast.infra.registry.registry:Registry cache expired, so refreshing
INFO:feast.infra.registry.registry:Registry cache expired, so refreshing
INFO:feast.infra.registry.registry:Registry cache expired, so refreshing
DEBUG:fsspec.local:open file: /Users/dmartino/projects/AI/feast/feast_issue_600K/data/train.parquet
zsh: killed     python test.py
```

### Hhints
Debugger shows that the last method executed is `_merge` in `DaskRetrievalJob` class, at [line 605](https://github.com/feast-dev/feast/blob/23c6c862e1da4e9523530eb48c7ce79319dc442d/sdk/python/feast/infra/offline_stores/dask.py#L605)
