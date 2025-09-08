from __future__ import annotations

import json
import pendulum

from airflow.decorators import dag, task
from airflow.models.taskinstance import TaskInstance
from airflow.providers.apache.kafka.operators.consume import ConsumeFromTopicOperator
from airflow.providers.standard.operators.empty import EmptyOperator


def process_kafka_message(message):
    """
    Processes a message from Kafka. This function is called for each message
    consumed from the topic and its return value is pushed to XComs.
    """
    data = json.loads(message.value().decode("utf-8"))
    pid = data.get("pid")
    pid2 = data.get("pid2")
    # Nested .get() to safely access providerId
    internalId = data.get("data", {}).get("internalId")

    # If any required field is missing, we just return the string for the stop task.
    if not all([pid, pid2, internalId]):
        return "stop_processing"

    # If all fields are present, return a tuple with the target task_id and the data.
    return (
        "process_record",
        {
            "pid": pid,
            "pid2": pid2,
            "internalId": internalId,
        },
    )


@dag(
    dag_id="kafka_consumer_dag_taskflow",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    schedule=None,
    tags=["kafka", "consumer", "example", "taskflow"],
)
def kafka_consumer_dag_taskflow():
    """
    ### Kafka Consumer DAG using TaskFlow API
    This DAG consumes messages from a Kafka topic, uses the `@task.branch`
    decorator to check for required fields, and processes the message only if
    all fields are present.
    """

    consume_from_topic = ConsumeFromTopicOperator(
        task_id="consume_from_topic",
        kafka_config_id="kafka_default",
        topics=["my-topic"],
        apply_function="kafka_consumer_dag.process_kafka_message",
        poll_timeout=60,
        max_messages=10,
    )

    @task.branch
    def check_required_fields(ti: TaskInstance):
        """
        Pulls the result from the consumer task and decides which path to take.
        """
        xcom_result = ti.xcom_pull(task_ids="consume_from_topic", key="return_value")
        if isinstance(xcom_result, tuple) and xcom_result[0] == "process_record":
            return "process_record"
        return "stop_processing"

    @task
    def process_record(ti: TaskInstance):
        """
        Logs the extracted values from the Kafka message.
        """
        # Pull the data payload from the tuple returned by the consumer
        _, extracted_data = ti.xcom_pull(task_ids="consume_from_topic", key="return_value")
        print("Processing record with the following details:")
        print(f"  PID: {extracted_data['pid']}")
        print(f"  PID2: {extracted_data['pid2']}")
        print(f"  Internal ID: {extracted_data['internalId']}")

    stop_processing = EmptyOperator(task_id="stop_processing")

    # This task will run after either 'process_record' or 'stop_processing' completes.
    end = EmptyOperator(task_id="end", trigger_rule="none_failed_min_one_success")

    # Define the task dependencies
    branch_task = check_required_fields()
    process_task = process_record()

    consume_from_topic >> branch_task
    branch_task >> process_task >> end
    branch_task >> stop_processing >> end

# Instantiate the DAG
kafka_consumer_dag_instance = kafka_consumer_dag_taskflow()
