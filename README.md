# DFE API Example Code

Example PySpark code for extracting data from DfE Data API.

It is recommended this notebook be loaded into Microsoft Fabric. It can however be run locally or in any other spark cluster with minimal alteration.

The code is still in an early stage and currently has no error handling or logging.

Be careful with test set B. It will take a long while to run (20-30 mins). Would suggest a tighter query is required. Could however improve the code so multiple requests happen in parallel.

The output dataframe should be very much considered a raw/bronze layer in terms of a datalake. While it does join into the metadata so output is human readable, the code is written to handle any DfE endpoint so the resulting dataframe outputs values as strings in a key value normalised form. Along the lines of:

| Metadata Columns | Indicator | Value |
|--|--|--|
| Secondary  | Sessions Possible    | 50000  |
| Primary | Sessions Possible    | 10000  |
| Secondary  | Sessions Absent    | 1000  |
| Primary | Sessions Absent    | 500  |

As you merge into a base/silver layer you would likey further process and set it up to do a delta load for the bigger datasets.

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

- Send a query:
(https://dfe-analytical-services.github.io/explore-education-statistics-api-docs/getting-started/creating-advanced-data-set-queries/)
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

## DfE_API Class Overview

The `DfE_API` class is designed to facilitate interactions with the Department for Education (DfE) data sets. It streamlines the process of querying data sets, processing metadata, and generating PySpark DataFrames for data analysis.

### Public Variables
#### Input when object created:
- **`base_endpoint_url`**: The base URL for accessing the DfE API. It is constructed using the API version and data set ID provided during initialization.
- **`spark`**: The Spark session instance used for creating and manipulating PySpark DataFrames. It is passed during initialization or defaults to an existing `spark` session.

#### Automatically populated from API when object created:
- **`data_set_summary_json`**: A JSON object containing a summary of the data set fetched from the DfE API.
- **`data_set_meta_json`**: A JSON object with metadata about the data set, such as indicators, geographic levels, and time periods.
- **`indicators_df`**: A PySpark DataFrame representing the indicators available in the data set, including their IDs, columns, labels, and decimal places.
- **`geographic_levels_df`**: A PySpark DataFrame containing the different geographic levels within the data set and their corresponding labels.
- **`locations_df`**: A PySpark DataFrame that details the location levels and options present in the data set, flattened for ease of use.
- **`time_periods_df`**: A PySpark DataFrame listing the available time periods within the data set, including period codes and labels.
- **`filters_df`**: A PySpark DataFrame detailing the filters available in the data set, with flattened options for easy querying.
#### Populated from API when query query is posted against the object:
- **`query_response_json`**: Stores the JSON response from the data set query.
- **`raw_query_response_df`**: A PySpark DataFrame representing the raw, query response.
- **`query_response_df`**: A final PySpark DataFrame containing the processed results, joined with metadata such as indicators, geographic levels, and filters.

### Public Functions

1. **`__init__(self, data_set_id, spark_session=spark, api_version="1.0")`**:
   - **Description**: Initializes the `DfE_API` class with a given data set ID and an optional Spark session and API version.
   - **Parameters**:
     - `data_set_id` (str): The ID of the data set to interact with.
     - `spark_session` (SparkSession, optional): A Spark session to use for DataFrame operations. Defaults to `spark`.
     - `api_version` (str, optional): The version of the API to use. Defaults to "1.0".
   - **Functionality**: Sets up class variables and fetches data set summary and metadata, initializing DataFrames for indicators, geographic levels, locations, time periods, and filters.
   - **Usage**: `DataSetA = DfE_API("e1ae9201-2fff-d376-8fa3-bd3c3660d4c8", spark, "1.0")`

2. **`post_data_set_query(self, query_body, data_set_version=None)`**:
   - **Description**: Sends a POST request with a query body to retrieve data from the data set.
   - **Parameters**:
     - `query_body` (dict): The body of the query to send.
     - `data_set_version` (str, optional): The version of the data set to query. If not provided, the default version is used.
   - **Returns**: A success message if the query succeeds.
   - **Functionality**: Uses helper functions to fetch results, stores the response in `query_response_json`, and processes the response to populate `query_response_df`.
    - **Usage**: `DataSetA.post_data_set_query(query_json)`
