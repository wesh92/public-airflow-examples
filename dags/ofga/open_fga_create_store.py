from __future__ import annotations

import pendulum
from airflow.decorators import dag, task
from airflow.models.param import Param
from openfga_sdk import OpenFgaApi
from openfga_sdk.client import ClientConfiguration, OpenFgaClient
from openfga_sdk.models.create_store_request import CreateStoreRequest

@dag(
    dag_id="open_fga_create_store",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    schedule=None,
    tags=["openfga"],
    params={
        "store_name": Param(
            type="string",
            default="Kafka-Airflow-Store"
        ),
    },
)
def create_openfga_store_dag():
    @task
    async def create_store(**context):
        configuration = ClientConfiguration(
            api_host="openfga.default:8088", # Your OpenFGA service and port
        )
        async with OpenFgaClient(configuration) as fga:
            api_response = await fga.list_stores()
            print(str(api_response.content_length))
            await fga.close()

    create_store()

create_openfga_store_dag()
