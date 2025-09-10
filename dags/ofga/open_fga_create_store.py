from __future__ import annotations

import pendulum
from airflow.decorators import dag, task
from airflow.models.param import Param
from openfga_sdk import OpenFgaApi, ApiClient, Configuration
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
            default="Kafka-Airflow-Store",
            title="OpenFGA Store Name",
            description="The name for the new OpenFGA store.",
        ),
    },
)
def create_openfga_store_dag():
    """
    ### Create OpenFGA Store
    This DAG creates a new store in your OpenFGA instance using the
    provided 'store_name' parameter.
    """

    @task
    async def create_store(**context):
        """
        Asynchronously creates a new store in OpenFGA.
        """
        store_name = context["params"]["store_name"]

        configuration = Configuration(
            api_scheme="http",
            api_host="openfga:8088",  # Your OpenFGA service and port
        )

        try:
            # The ApiClient should be used with an async context manager
            async with ApiClient(configuration) as api_client:
                api_instance = OpenFgaApi(api_client)

                # Check if a store with the same name already exists
                existing_stores_response = await api_instance.list_stores()
                async for element in existing_stores_response.content:
                    print(element.decode("utf-8"))
                    return

                # If not, create a new one
                body = CreateStoreRequest(name=store_name)
                await api_instance.create_store(body)

                return

        except Exception as e:
            print(f"Error communicating with OpenFGA: {e}")
            raise

    create_store()

create_openfga_store_dag()
