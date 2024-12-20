from dagster import (op, Field, List, String, Int,  # type: ignore
                     DependencyDefinition, 
                     GraphDefinition,  
                     In, Out, Output, 
                     graph, job
                     )

from slack_sdk import *  # type: ignore
import boto3  # type: ignore
import json 
import pandas as pd  # type: ignore
import os 
from slack_sdk.web import WebClient # type: ignore
from ..utils import *

import os 
import json
import shutil
from botocore.exceptions import NoCredentialsError, PartialCredentialsError, ClientError
from slack_sdk.web import WebClient
import tempfile
from evidently.report import Report # type: ignore
from evidently.test_suite import TestSuite # type: ignore
from evidently.tests import * # type: ignore
from evidently import ColumnMapping # type: ignore
from evidently.ui.workspace import Workspace # type: ignore
from evidently.ui.dashboards import CounterAgg # type: ignore
from evidently.ui.dashboards import DashboardPanelCounter
from evidently.ui.dashboards import DashboardPanelPlot
from evidently.ui.dashboards import PanelValue # type: ignore
from evidently.ui.dashboards import PlotType
from evidently.ui.dashboards import ReportFilter
from evidently.renderers.html_widgets import WidgetSize
from evidently.ui.dashboards import DashboardPanelTestSuite
from evidently.ui.dashboards import TestSuitePanelType
from evidently import metrics
from confluent_kafka import Producer, Consumer, KafkaException, KafkaError
from dotenv import load_dotenv


#load_dotenv()


@op(
    required_resource_keys={"s3_resource"},
    tags={"kind": "aws"}
)
def process_s3_ref(context):
    bucket_name = context.resources.s3_resource[1]
    files_to_download = []
    all_dataframes = []

    try:
        ACCESS_KEY = os.getenv('AWS_ACCESS_KEY_ID')
        SECRET_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
        bucket_name = os.getenv('S3_BUCKET_NAME')
        
        context.log.info(f"vars values : {ACCESS_KEY} , {SECRET_KEY}, {bucket_name}")

        s3 = boto3.client('s3', aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_KEY)
        
        # List .json files with 'pending' tag
        response = s3.list_objects_v2(Bucket=bucket_name)

        files_to_download = []

        if 'Contents' in response:
            for obj in response['Contents']:
                key = obj['Key']
                if key.endswith('.json'):
                    try:
                        # Check if the file has the 'pending' tag
                        tagging = s3.get_object_tagging(Bucket=bucket_name, Key=key)
                        tags = {tag['Key']: tag['Value'] for tag in tagging['TagSet']}
                        if tags.get('status') == 'In Progress' or tags.get('status') == 'ref':
                            files_to_download.append(key)
                        else:
                            context.log.info(f"Skipping file {key} with status: {tags.get('STATUS')}")
                    except ClientError as e:
                        context.log.error(f"Error retrieving tags for {key}: {str(e)}")
                        raise
        else:
            context.log.info(f"No files found in bucket: {bucket_name}")
            return Output("no data in bucket","no data in bucket")

        # Download and process the files
        for file_name in files_to_download:
            try:
                    # Get the object from S3
                obj = s3.get_object(Bucket=bucket_name, Key=file_name)
                file_content = obj['Body'].read().decode('utf-8')
                # Load JSON data
                data = json.loads(file_content)
                df = pd.DataFrame(data)
                # Select and rename columns
                df = df[columns_to_select].rename(columns=columns_rename_map)
                all_dataframes.append(df)
            except ClientError as e:
                context.log.error(f"Error downloading {file_name}: {str(e)}")
                raise
            except FileNotFoundError as e:
                context.log.error(f"Error: The file {file_name} was not found.")
                raise
            except json.JSONDecodeError as e:
                context.log.error(f"Error: The file {file_name} could not be decoded.")
                raise
            except Exception as e:
                context.log.error(f"An unexpected error occurred while processing {file_name}: {e}")
                raise
        
        if all_dataframes:
            try:
                # Concatenate all DataFrames
                concatenated_df = pd.concat(all_dataframes, ignore_index=True)
                ref_crdt_df = concatenated_df[concatenated_df['type'] == 'CRDT']
                ref_dbit_df = concatenated_df[concatenated_df['type'] == 'DBIT']

                context.log.info(f"Credit Df columns : {ref_crdt_df.columns}")
                context.log.info(f"Debit Df columns  : {ref_dbit_df.columns}")


                ref_crdt_json = ref_crdt_df.to_json(orient='records')
                ref_dbit_json = ref_dbit_df.to_json(orient='records')
                
                # Initialize Kafka Producer
                producer = Producer({'bootstrap.servers': f'{os.environ.get("KAFKA_BOOTSTRAP_URL")}:9092'})
                
                # Produce message to Kafka topics
                producer.produce('credit_topic', value=ref_crdt_json.encode('utf-8'), headers=[('header_key', 'ref'.encode('utf-8'))])
                producer.produce('debit_topic', value=ref_dbit_json.encode('utf-8'),  headers=[('header_key', 'ref'.encode('utf-8'))])
                
                # Flush producer buffer
                producer.flush()
                context.log.info("Data sent Successfully !")

            except Exception as e:
                context.log.error(f"An unexpected error occurred while concatenating DataFrames: {e}")

        else:
            raise ValueError("No dataframes were created from the downloaded files.")
    
    # except NoSuchBucket as e:
    #     context.log.error(f"The specified bucket does not exist: {str(e)}")
    #     raise
    except (NoCredentialsError, PartialCredentialsError) as e:
        context.log.error(f"Credentials not available or incomplete: {str(e)}")
        raise
    except Exception as e:
        context.log.error(f"Error processing files: {str(e)}")
        raise




@op(
    required_resource_keys={"s3_resource"},
    out={

        "files_to_download": Out(),
    },
    tags={"kind": "aws"}
)



def process_s3_current(context):
    bucket_name = context.resources.s3_resource[1]
    files_to_download = []
    all_dataframes = []

    try:
        # s3 = context.resources.s3_resource[0]
        # # List .json files with 'pending' tag
        # response = s3.list_objects_v2(Bucket=bucket_name)
        ACCESS_KEY = os.getenv('AWS_ACCESS_KEY_ID')
        SECRET_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
        bucket_name = os.getenv('S3_BUCKET_NAME')
        
        context.log.info(f"vars values : {ACCESS_KEY} , {SECRET_KEY}, {bucket_name}")

        s3 = boto3.client('s3', aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_KEY)
        
        # List .json files with 'pending' tag
        response = s3.list_objects_v2(Bucket=bucket_name)

        files_to_download = []

        if 'Contents' in response:
            for obj in response['Contents']:
                key = obj['Key']
                if key.endswith('.json'):
                    try:
                        # Check if the file has the 'pending' tag
                        tagging = s3.get_object_tagging(Bucket=bucket_name, Key=key)
                        tags = {tag['Key']: tag['Value'] for tag in tagging['TagSet']}
                        if tags.get('status') == 'In Progress' or tags.get('status') == 'In Progress':
                            files_to_download.append(key)
                        else:
                            context.log.info(f"Skipping file {key} with status: {tags.get('STATUS')}")
                    except ClientError as e:
                        context.log.error(f"Error retrieving tags for {key}: {str(e)}")
                        update_tag_to_fail(s3, bucket_name, key)
                        raise
        else:
            context.log.info(f"No files found in bucket: {bucket_name}")
            return Output("no data in bucket","no data in bucket")

        # Download and process the files
        for file_name in files_to_download:
            try:
                    # Get the object from S3
                obj = s3.get_object(Bucket=bucket_name, Key=file_name)
                file_content = obj['Body'].read().decode('utf-8')
                # Load JSON data
                data = json.loads(file_content)
                df = pd.DataFrame(data)
                # Select and rename columns
                df = df[columns_to_select].rename(columns=columns_rename_map)
                all_dataframes.append(df)
                update_tag_to_done(s3, bucket_name, file_name)
            except ClientError as e:
                context.log.error(f"Error downloading {file_name}: {str(e)}")
                update_tag_to_fail(s3, bucket_name, file_name)
                raise
            except FileNotFoundError as e:
                context.log.error(f"Error: The file {file_name} was not found.")
                update_tag_to_fail(s3, bucket_name, file_name)
                raise
            except json.JSONDecodeError as e:
                context.log.error(f"Error: The file {file_name} could not be decoded.")
                update_tag_to_fail(s3, bucket_name, file_name)
                raise
            except Exception as e:
                context.log.error(f"An unexpected error occurred while processing {file_name}: {e}")
                update_tag_to_fail(s3, bucket_name, file_name)
                raise
        
        if all_dataframes:
            try:
                # Concatenate all DataFrames
                concatenated_df = pd.concat(all_dataframes, ignore_index=True)
                current_crdt_df = concatenated_df[concatenated_df['type'] == 'CRDT']
                current_dbit_df = concatenated_df[concatenated_df['type'] == 'DBIT']

                current_crdt_json = current_crdt_df.to_json(orient='records')
                current_dbit_json = current_dbit_df.to_json(orient='records')
                
                # Initialize Kafka Producer
                producer = Producer({'bootstrap.servers': f'{os.environ.get("KAFKA_BOOTSTRAP_URL")}:9092'})
                
                # Produce message to Kafka topics
                producer.produce('credit_topic', value=current_crdt_json.encode('utf-8'),headers=[('header_key', 'current'.encode('utf-8'))])
                producer.produce('debit_topic', value=current_dbit_json.encode('utf-8'), headers=[('header_key', 'current'.encode('utf-8'))])
                
                # Flush producer buffer
                producer.flush()
                context.log.info("Data sent Successfully !")



                context.log.info(f"Credit Df columns : {current_crdt_df.columns}")
                context.log.info(f"Debit Df columns  : {current_dbit_df.columns}")
            except Exception as e:
                context.log.error(f"An unexpected error occurred while concatenating DataFrames: {e}")
                for file_name in files_to_download:
                    update_tag_to_fail(s3, bucket_name, file_name)
                raise
        else:
            raise ValueError("No dataframes were created from the downloaded files.")
    
    except (NoCredentialsError, PartialCredentialsError) as e:
        context.log.error(f"Credentials not available or incomplete: {str(e)}")
        raise
    except Exception as e:
        context.log.error(f"Error processing files: {str(e)}")
        raise


    yield Output(files_to_download, "files_to_download")


@op(
    required_resource_keys={
        "s3_resource",
        "slack_resource"
    },
    ins={
       "files_to_process" : In(list)
    },
    tags={"kind": "python"}, 
)

def process_credit(context, files_to_process):
    slack_token = os.environ.get('SLACK_API_TOKEN')
    channel_id = os.environ.get('SLACK_CHANNEL_ID')

    if not files_to_process :
        return Output("no data in bucket","no data in bucket")

    context.log.info(f"Processing debits for project: {PROJECTS_NAMES[0]}")

    #deviding current into multiple categories
    consumer_config = {
        'bootstrap.servers': f'{os.environ.get("KAFKA_BOOTSTRAP_URL")}:9092',
        'group.id': os.environ.get('KAFKA_GROUP_ID'),
        'auto.offset.reset': 'earliest'
    }

    # Initialize Kafka consumer
    consumer = Consumer(consumer_config)

    # Subscribe to the 'debit_topic'
    consumer.subscribe([str(os.environ.get('CREDIT_TOPIC'))])

    try:
        ref_crdt_df = pd.DataFrame()
        current_crdt_df = pd.DataFrame()
        while ref_crdt_df.empty or current_crdt_df.empty :
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event
                    continue
                else:
                    raise KafkaException(msg.error())

            # Log the message
            context.log.info(f"Received message: {msg.value()}")

            # Extract headers
            headers = msg.headers()
            context.log.info(f"Received headers: {headers}")
            header_value = None
            if headers:
                for header_key, header_val in headers:
                    if header_key == 'header_key':
                        header_value = header_val.decode('utf-8')
                        break
            
            if header_value is None:
                # Log and skip processing if no relevant header found
                context.log.warning("No relevant header found, skipping message.")
                continue
            
            # Decode JSON message
            try:
                data = json.loads(msg.value().decode('utf-8'))
            except json.JSONDecodeError as e:
                context.log.error(f"JSON decode error: {e}")
                continue
            
            # Transform JSON data into a DataFrame
            df = pd.DataFrame(data)
            
            # Process DataFrame based on header value
            if header_value == 'ref':
                ref_crdt_df = df
                context.log.info(f"Received 'ref' data: {ref_crdt_df}")
                # Process or use ref_df as needed
            elif header_value == 'current':
                current_crdt_df = df
                context.log.info(f"Received 'current' data: {current_crdt_df}")
                # Process or use current_df as needed
            else:
                context.log.warning(f"Unknown header value: {header_value}")

    except KeyboardInterrupt:
        pass
    except KafkaException as e:
        context.log.error(f"KafkaException: {e}")
    except Exception as e:
        context.log.error(f"Exception occurred: {e}")
    finally:
        # Close the consumer
        consumer.close()

    # if current_crdt_df is None:
    #     raise ValueError("current_crdt_df is not initialized.")

    tag = "full"
    add_report_to_project(PROJECTS_NAMES[0],ref_crdt_df, current_crdt_df, tag, column_mapping, files_to_process)
    add_test_to_project(PROJECTS_NAMES[0], ref_crdt_df, current_crdt_df, tag, column_mapping, files_to_process)



@op(
    required_resource_keys={
        "s3_resource",
        "slack_resource"
    },
    ins={
        "files_to_process" : In(list)
    },
    tags={"kind": "python"}, 
)
def process_debits(context,files_to_process):
    slack_token = os.environ.get('SLACK_API_TOKEN')
    channel_id = os.environ.get('SLACK_CHANNEL_ID')

    if not files_to_process :
        return Output("no data in bucket","no data in bucket")

        
    context.log.info(f"Processing debits for project: {PROJECTS_NAMES[1]}")

    consumer_config = {
        'bootstrap.servers': f'{os.environ.get("KAFKA_BOOTSTRAP_URL")}:9092',
        'group.id': os.environ.get('KAFKA_GROUP_ID'),
        'auto.offset.reset': 'earliest'
    }

    # Initialize Kafka consumer
    consumer = Consumer(consumer_config)

    # Subscribe to the 'debit_topic'
    consumer.subscribe([str(os.environ.get('DBIT_TOPIC'))])

    try:
        ref_dbit_df = pd.DataFrame()
        current_dbit_df = pd.DataFrame()

        while ref_dbit_df.empty or current_dbit_df.empty :
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event
                    continue
                else:
                    raise KafkaException(msg.error())

            # Log the message
            context.log.info(f"Received message: {msg.value()}")

            # Extract headers
            headers = msg.headers()
            context.log.info(f"Received headers: {headers}")
            header_value = None
            if headers:
                for header_key, header_val in headers:
                    if header_key == 'header_key':
                        header_value = header_val.decode('utf-8')
                        break
            
            if header_value is None:
                # Log and skip processing if no relevant header found
                context.log.warning("No relevant header found, skipping message.")
                continue
            
            # Decode JSON message
            try:
                data = json.loads(msg.value().decode('utf-8'))
            except json.JSONDecodeError as e:
                context.log.error(f"JSON decode error: {e}")
                continue
            
            # Transform JSON data into a DataFrame
            df = pd.DataFrame(data)
            
            # Process DataFrame based on header value
            if header_value == 'ref':
                ref_dbit_df = df
                context.log.info(f"Received 'ref' data: {ref_dbit_df}")
                a=1
                # Process or use ref_df as needed
            elif header_value == 'current':
                current_dbit_df = df
                context.log.info(f"Received 'current' data: {current_dbit_df}")
                b=1
                # Process or use current_df as needed
            else:
                context.log.warning(f"Unknown header value: {header_value}")

    except KeyboardInterrupt:
        pass
    except KafkaException as e:
        context.log.error(f"KafkaException: {e}")
    except Exception as e:
        context.log.error(f"Exception occurred: {e}")
    finally:
        # Close the consumer
        consumer.close()

    context.log.info("while is out")

    # ref_dbit = ref_dbit_df
    # current_dbit = current_dbit_df

    current_dbit_df['main_category'] = current_dbit_df['target'].apply(split_category)
    grouped = current_dbit_df.groupby('main_category')
    current_dbit_s = {main_category: grouped.get_group(main_category) for main_category in grouped.groups}
    
    #deviding reference into multiple categories
    ref_dbit_df['main_category'] = ref_dbit_df['target'].apply(split_category)
    grouped_ref = ref_dbit_df.groupby('main_category')
    refs_s = {main_category: grouped.get_group(main_category) for main_category in grouped_ref.groups}

    # column_mapping = column_mapping

    present_categories = []
    for main_category, current_df in current_dbit_s.items():
        present_categories.append(main_category)
        ref_cat = refs_s[main_category]
        tag = main_category
        
        print("working on category: ", main_category)
        add_report_to_project(PROJECTS_NAMES[1], ref_cat, current_df, tag, column_mapping, files_to_process)
        add_test_to_project(PROJECTS_NAMES[1], ref_cat, current_df, tag, column_mapping, files_to_process)
    
    for category in payments_names:
        if category not in present_categories:
            message = f"this category: {category} is not present in the current batch !!"
            send_alert_to_slack(message, slack_token, channel_id)

    current_dbit_df = current_dbit_df.drop(columns=['main_category'])
    ref_dbit_df = ref_dbit_df.drop(columns = ["main_category"])

    tag = "full"
    add_report_to_project(PROJECTS_NAMES[1], ref_dbit_df, current_dbit_df, tag, column_mapping, files_to_process)
    add_test_to_project(PROJECTS_NAMES[1], ref_dbit_df, current_dbit_df, tag, column_mapping, files_to_process)



evedently_dag_dependencies = {
    "process_debits": {
        "files_to_process":DependencyDefinition("process_s3_current","files_to_download")
    },
    "process_credit":{
        "files_to_process" : DependencyDefinition("process_s3_current","files_to_download")
    }
}


GraphDefinition(
    name="evedently_graph", 
    node_defs=[
        process_s3_ref, 
        process_s3_current, 
        process_debits,
        process_credit
    ],
    dependencies=evedently_dag_dependencies
)

@graph()

def evedently_graph():

    process_s3_ref()

    files_to_process = process_s3_current()

    process_credit(files_to_process)
    process_debits(files_to_process)


@job 
def evedently_job():
    evedently_graph() 
