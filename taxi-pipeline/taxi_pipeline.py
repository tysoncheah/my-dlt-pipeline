"""dlt REST API pipeline for NYC taxi data from the Data Engineering Zoomcamp API."""

import dlt
from dlt.sources.rest_api import rest_api_resources
from dlt.sources.rest_api.typing import RESTAPIConfig


@dlt.source
def taxi_rest_api_source():
    """REST API source for NYC taxi trips (paginated JSON, 1,000 records per page)."""
    config: RESTAPIConfig = {
        "client": {
            "base_url": "https://us-central1-dlthub-analytics.cloudfunctions.net",
        },
        "resources": [
            {
                "name": "trips",
                "endpoint": {
                    "path": "data_engineering_zoomcamp_api",
                    "params": {
                        # Start from page 1; `page` is the query parameter used for pagination
                        "page": 1,
                    },
                    "paginator": {
                        # Page-number pagination; dlt will keep increasing `page`
                        # and will stop automatically once an empty page is returned.
                        # We explicitly set `total_path` to None so that the paginator
                        # does NOT expect a `total`/`pages` field in the API response
                        # and instead stops when it encounters an empty page.
                        "type": "page_number",
                        "base_page": 1,
                        "page_param": "page",
                        "total_path": None,
                        # `stop_after_empty_page` defaults to True in @dlt rest_api
                    },
                    # Response is a root-level JSON array of trip records
                    "data_selector": "$",
                },
            }
        ],
    }

    # Use the generic REST API helper from @dlt to materialize resources
    yield from rest_api_resources(config)


taxi_pipeline = dlt.pipeline(
    pipeline_name="taxi_pipeline",
    destination="duckdb",
    progress="log",
)


if __name__ == "__main__":
    load_info = taxi_pipeline.run(taxi_rest_api_source())
    print(load_info)
