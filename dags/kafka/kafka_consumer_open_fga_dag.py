from __future__ import annotations

import json
import pendulum

from airflow.decorators import dag, task
from airflow.models.taskinstance import TaskInstance
from airflow.providers.apache.kafka.operators.consume import ConsumeFromTopicOperator
from airflow.providers.standard.operators.empty import EmptyOperator

from openfga_sdk.models.tuple_key import TupleKey


def process_kafka_message(message):
    """
    Processes a message from Kafka, formats it as an OpenFGA tuple object,
    and prints the resulting object and its dictionary representation.
    """
    data = json.loads(message.value().decode("utf-8"))
    pid = data.get("pid")
    pid2 = data.get("pid2")
    internalId = data.get("data", {}).get("internalId")

    if pid and pid2 and internalId:
        if TupleKey:
            # 1. Define the components of the tuple
            user = f"pid:{pid}"
            relation = pid2
            obj = f"internalId:{internalId}"

            # 2. Create the actual OpenFGA TupleKey object
            openfga_tuple_key = TupleKey(user=user, relation=relation, object=obj)

            # 3. Print the results for verification
            print(f"Successfully created TupleKey object: {openfga_tuple_key}")
            print(f"TupleKey as dictionary: {openfga_tuple_key.to_dict()}")

        else:
            print("Skipping tuple creation because OpenFGA SDK is missing.")
    else:
        print(f"Skipping message due to missing data: {data}")


@dag(
    dag_id="kafka_consumer_dag_taskflow",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    schedule=None,
    tags=["kafka", "consumer", "example", "taskflow"],
)
def kafka_consumer_dag_taskflow():
    """
    ### Kafka Consumer DAG using TaskFlow API with production of a result.
    This DAG consumes messages from a Kafka topic and produces an OpenFGA Tuple for export.
    """

    consume_from_topic = ConsumeFromTopicOperator(
        task_id="consume_from_topic",
        kafka_config_id="kafka_default",
        topics=["my-topic"],
        apply_function=process_kafka_message,
        poll_timeout=60,
        max_messages=10,
    )

    # This task will run after either 'process_record' or 'stop_processing' completes.
    end = EmptyOperator(task_id="end", trigger_rule="none_failed_min_one_success")

    consume_from_topic >> end

# Instantiate the DAG
kafka_consumer_dag_instance = kafka_consumer_dag_taskflow()
