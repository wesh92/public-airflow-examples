from __future__ import annotations

import pendulum
from airflow.decorators import dag, task
from openfga_sdk import OpenFgaApi
from openfga_sdk.api_client import ApiClient, Configuration
from openfga_sdk.models.create_store_request import CreateStoreRequest

@dag(
    dag_id="open_fga_create_store",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    schedule=None,
    tags=["openfga"],
)
def create_openfga_store_dag():
    @task
    def create_store():
        configuration = Configuration(
            api_scheme="http",
            api_host="openfga:8088", # Your OpenFGA service and port
        )
        api_client = ApiClient(configuration)
        api_instance = OpenFgaApi(api_client)

        try:
            response = api_instance.create_store(
                body=CreateStoreRequest(name="Kafka-Airflow-Store")
            )
            print(f"Store created with ID: {response.id}")
            return response.id
        except Exception as e:
            print(f"Error creating store: {e}")
            raise

    create_store()

create_openfga_store_dag()
