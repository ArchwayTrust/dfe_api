# DFE API Example Code

Example PySpark code for extracting data from DfE Data API.

It is recommended this notebook be loaded into Microsoft Fabric. It can however be run locally or in any other spark cluster with miniaml alteration.

The code is still in an early stage and currently has no error handling or logging.

Be careful with test set B. It will take a long while to run (20-30 mins). Would suggest a tighter query is required. Could however improve the code so multiple requests happen in parallel.

## Example Usage
All the code is in a class which abstracts the complexity of the API away.

- Create an instance of the class for a data-set:
```
data_set_id = "e1ae9201-2fff-d376-8fa3-bd3c3660d4c8"
SetA = DfE_API(data_set_id)
```

- Explore the dataset information:

```
print(json.dumps(SetA.data_set_summary_json, indent=4))
```
```
display(SetA.filters_df)
```

- Send a queyr:

```
data = {
    "criteria": {
        "and": [
            {
                "timePeriods": {
                    "in": [
                        {"period": "2023/2024","code": "AY"},
                        {"period": "2023/2024","code": "AY"}
                    ]
                }
            },
            {
                "locations": {
                    "eq": {"level": "NAT","code": "E92000001"}
                }
            }
        ]
    },
    "indicators": ["dPe0Z", "OBXCL", "7YFXo"]
}
SetA.post_data_set_query(data)
SetA.query_response_df.write.format("delta").mode("overwrite").saveAsTable("LH_DFE.raw.set_a")
```


