from airflow import DAG
from datetime import timedelta, datetime
from airflow.operators.dummy_operator import DummyOperator
import boto3
from airflow.providers.amazon.aws.operators.emr import EmrCreateJobFlowOperator, EmrAddStepsOperator, EmrTerminateJobFlowOperator
from airflow.providers.amazon.aws.sensors.emr import EmrJobFlowSensor, EmrStepSensor

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 27), 
    'retries': 2,
    'retry_delay': timedelta(seconds=10)
}

job_flow_overrides = {
    "Name": "redfin_emr_cluster",
    "ReleaseLabel": "emr-6.13.0",
    "Applications": [{"Name": "Spark"}, {"Name": "JupyterEnterpriseGateway"}],
    "LogUri": "s3://redfin-project-om/emr-logs/",
    "VisibleToAllUsers":False,
    "Instances": {
        "InstanceGroups": [
            {
                "Name": "Master node",
                "Market": "ON_DEMAND",
                "InstanceRole": "MASTER",
                "InstanceType": "m5.xlarge",
                "InstanceCount": 1,
            },
            {
                "Name": "Core node",
                "Market": "ON_DEMAND",
                "InstanceRole": "CORE",
                "InstanceType": "m5.xlarge",
                "InstanceCount": 2,
            },
        ],
         
        "Ec2SubnetId": "subnet-07c75362e002ac34e",
        "Ec2KeyName" : 'emr-keypair-airflow',
        "KeepJobFlowAliveWhenNoSteps": True,
        "TerminationProtected": False,
    },
    "JobFlowRole": "EMR_EC2_DefaultRole",
    "ServiceRole": "EMR_DefaultRole",
   
}

# Run the ingest.sh script using script-runner.jar file
SPARK_STEPS_EXTRACTION = [
    {
        "Name": "Extract Redfin data",
        "ActionOnFailure": "CANCEL_AND_WAIT",
        "HadoopJarStep": {
            "Jar": "s3://us-east-1.elasticmapreduce/libs/script-runner/script-runner.jar",
            "Args": [
                "s3://redfin-project-om/scripts/ingest.sh",
            ],
        },
    },
   ]

SPARK_STEPS_TRANSFORMATION = [
    {
        "Name": "Transform Redfin data",
        "ActionOnFailure": "CANCEL_AND_WAIT",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": ["spark-submit",
            "s3://redfin-project-om/scripts/transform_redfin_data.py",
            ],
        },
    },
   ]

with DAG('redfin_analytic_dag', default_args=default_args, catchup=False) as dag:
    
    start_pipeline = DummyOperator(task_id = 'task_start_pipeline')

    # Task for creating EMR cluster
    create_emr_cluster = EmrCreateJobFlowOperator(
        task_id = 'task_create_emr_cluster',
        job_flow_overrides = job_flow_overrides,
    )

    # Task for checking if EMR cluster is created
    is_emr_cluster_created = EmrJobFlowSensor(
        task_id = "task_is_emr_cluster_created", 
        job_flow_id = "{{ task_instance.xcom_pull(task_ids='task_create_emr_cluster', key='return_value') }}",
        target_states = {"WAITING"},  # The desired state that cluster needs to achieve for the sensor to notice its existence
        timeout = 3600,
        poke_interval = 5,
        mode = 'poke',
    )

    extract_redfin_data = EmrAddStepsOperator(
        task_id = 'task_extract_redfin_data',
        job_flow_id="{{ task_instance.xcom_pull(task_ids='task_create_emr_cluster', key='return_value') }}",
        steps = SPARK_STEPS_EXTRACTION,
    )

    is_extraction_completed = EmrStepSensor(
        task_id = "task_is_extraction_completed",
        job_flow_id = "{{ task_instance.xcom_pull(task_ids='task_create_emr_cluster', key='return_value') }}",
        step_id = "{{ task_instance.xcom_pull(task_ids='task_extract_redfin_data')[0] }}",
        target_states = {"COMPLETED"},
        timeout = 3600,
        poke_interval = 5,
    )

    transformation_step = EmrAddStepsOperator(
        task_id="task_transformation_step",
        job_flow_id="{{ task_instance.xcom_pull(task_ids='task_create_emr_cluster', key='return_value') }}",
        steps=SPARK_STEPS_TRANSFORMATION,
    )

    is_transformation_completed = EmrStepSensor(
        task_id="task_is_transformation_completed",
        job_flow_id="{{ task_instance.xcom_pull(task_ids='task_create_emr_cluster', key='return_value') }}",
        step_id="{{ task_instance.xcom_pull(task_ids='task_transformation_step')[0] }}",
        target_states={"COMPLETED"},
        timeout=3600,
        poke_interval=10,
    )

    terminate_cluster = EmrTerminateJobFlowOperator(
        task_id="task_terminate_cluster",
        job_flow_id="{{ task_instance.xcom_pull(task_ids='task_create_emr_cluster', key='return_value') }}",
    )

    is_emr_cluster_terminated = EmrJobFlowSensor(
        task_id="task_is_emr_cluster_terminated", 
        job_flow_id="{{ task_instance.xcom_pull(task_ids='task_create_emr_cluster', key='return_value') }}",
        target_states={"TERMINATED"},
        timeout=3600,
        poke_interval=5,
        mode='poke',
    )

    end_pipeline = DummyOperator(task_id="task_end_pipeline")

    start_pipeline >> create_emr_cluster >> is_emr_cluster_created >> extract_redfin_data >> is_extraction_completed
    is_extraction_completed >> transformation_step >> is_transformation_completed >> terminate_cluster
    terminate_cluster >> is_emr_cluster_terminated >> end_pipeline