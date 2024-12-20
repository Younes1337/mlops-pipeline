import os 
import shutil
import tempfile
from botocore.exceptions import NoCredentialsError, PartialCredentialsError, ClientError # type: ignore
from evidently import ColumnMapping # type: ignore
import pandas as pd  # type: ignore
import json 
from slack_sdk import WebClient # type: ignore
from evidently.test_suite import TestSuite # type: ignore
from evidently.tests import * # type: ignore

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
import boto3
from dotenv import load_dotenv
##########################################################################################################################
from evidently.ui.remote import RemoteWorkspace


load_dotenv()

# Slack credentials &é"
slack_token = os.getenv('SLACK_API_TOKEN')
channel_id = os.getenv('SLACK_CHANNEL_ID')

# AWS credentials and S3 bucket name
ACCESS_KEY = os.getenv('AWS_ACCESS_KEY_ID')
SECRET_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
bucket_name = os.getenv('S3_BUCKET_NAME')

workspace = os.getenv("workspace")

# Evidently variables
PROJECT_DESCRIPTION = os.getenv("PROJECT_DESCRIPTION")
PROJECTS_NAMES_str = os.getenv("PROJECTS_NAMES")

if PROJECTS_NAMES_str:
    PROJECTS_NAMES = PROJECTS_NAMES_str.split(',')
else:
    PROJECTS_NAMES = []

#static variables
payments_names = [
                'Housing','Food & Drinks',
                'Leisure & Entertainment','Transportation',
                'Recurrent Payments','Investment',
                'Shopping','Healthy & Beauty',
                'Bank services','Other'
                ]

columns_to_select = ['REMITTANCE_INFORMATION',
                    'CATEGORY',
                    'category',
                    'CREDIT_DEBIT_INDICATOR'] 


columns_rename_map = {
    'REMITTANCE_INFORMATION': 'text',
    'CATEGORY': 'target',
    'category': 'prediction',
    'CREDIT_DEBIT_INDICATOR': 'type'
}
column_mapping = ColumnMapping(
    #target= 'target',
    categorical_features=['prediction', 'target'],
    text_features=['text'],
    #task='classification',
    #embeddings={'set':data_tot.columns[7:]}
    )



#############################################  Utils Functions ###########################################################
def split_category(row):
    return row.split(' / ')[0]


def update_tag_to_fail(s3, bucket_name, key):
    """
    Update the tag of a specific file or files to 'fail'.

    Parameters:
    s3 (boto3.client): The S3 client.
    bucket_name (str): The name of the S3 bucket.
    key (str or list): The key(s) of the file(s).
    """
    try:
        if isinstance(key, str):
            keys_to_update = [key]
        elif isinstance(key, list):
            keys_to_update = key
        else:
            raise ValueError("key must be a string or a list of strings")

        for k in keys_to_update:
            s3.put_object_tagging(
                Bucket=bucket_name,
                Key=k,
                Tagging={
                    'TagSet': [
                        {
                            'Key': 'status_monitoring',
                            'Value': 'fail'
                        }
                    ]
                }
            )
            print(f"Updated tag to 'fail' for {k}")
    except ClientError as e:
        print(f"Error updating tag for {key}: {str(e)}")


def update_tag_to_done(s3, bucket_name, key):
    """
    Update the tag of a specific file or files to 'done'.

    Parameters:
    s3 (boto3.client): The S3 client.
    bucket_name (str): The name of the S3 bucket.
    key (str or list): The key(s) of the file(s).
    """
    try:
        if isinstance(key, str):
            keys_to_update = [key]
        elif isinstance(key, list):
            keys_to_update = key
        else:
            raise ValueError("key must be a string or a list of strings")

        for k in keys_to_update:
            s3.put_object_tagging(
                Bucket=bucket_name,
                Key=k,
                Tagging={
                    'TagSet': [
                        {
                            'Key': 'status_monitoring',
                            'Value': 'done'
                        }
                    ]
                }
            )
            print(f"Updated tag to 'done' for {k}")
    except ClientError as e:
        print(f"Error updating tag for {key}: {str(e)}")



def send_alert_to_slack(message, slack_token, channel_id): 
    client = WebClient(token=slack_token)
    response = client.chat_postMessage(channel=channel_id, text=message)
    assert response["ok"]


def data_quality_tests(ref, current,tag, column_mapping):
    print("data_quality_tests")
    failed_tests = ""
    data_quality_tests = TestSuite(
        tests=[
            TestConflictPrediction(),
            # TestTargetPredictionCorrelation(),
        ],
        metadata= {"data quality tests":1},
        tags=["data quality tests",tag,"data"]
        )
    data_quality_tests.run(reference_data= ref ,
                        current_data= current, 
                        column_mapping=column_mapping)
    print("data_quality_tests done")
    test_summary = data_quality_tests.as_dict()
    for test in test_summary["tests"]:
        if test["status"].lower() == "fail":
            failed_tests = failed_tests + (str("  --"+test["name"]+":"+"\n"+"    "+test["description"])+"\n"+"\n")
    return data_quality_tests, failed_tests


def data_drift_tests(ref, current,tag, column_mapping):
    print("data_drift_tests")
    failed_tests = ""
    data_drift_tests = TestSuite(
        tests=[
            TestColumnDrift(column_name='target',stattest = "TVD", stattest_threshold = 0.6),
            TestColumnDrift(column_name='prediction',stattest = "TVD", stattest_threshold = 0.6),
            #TestEmbeddingsDrift(embeddings_name='set')
        ],
        metadata= {"data drift tests":1},
        tags=["data drift tests",tag,"data"]
        )
    data_drift_tests.run(reference_data= ref ,
                        current_data= current, 
                        column_mapping=column_mapping)
    print("data_drift_tests done")
    test_summary = data_drift_tests.as_dict()
    for test in test_summary["tests"]:
        if test["status"].lower() == "fail":
            failed_tests = failed_tests + (str("  --"+test["name"]+":"+"\n"+"    "+test["description"])+"\n"+"\n")
    return data_drift_tests, failed_tests


def model_performance_tests(ref, current,tag, column_mapping):
    print("model_performance_tests")
    failed_tests = ""
    model_performance_tests = TestSuite(
        tests=[
            TestAccuracyScore(),
            TestF1Score(),
            TestPrecisionScore(),
            TestRecallScore(),
            #TestPrecisionByClass(label='labeeeeeeeel')
        ],
        metadata= {"model tests":1},
        tags=["classification",tag,"model tests"]
        )
    model_performance_tests.run(reference_data= ref ,
                        current_data= current, 
                        column_mapping=column_mapping)
    print("model_performance_tests done")
    test_summary = model_performance_tests.as_dict()
    for test in test_summary["tests"]:
        if test["status"].lower() == "fail":
            failed_tests = failed_tests + (str("  --"+test["name"]+":"+"\n"+"    "+test["description"])+"\n"+"\n")
    # if any(failed_tests):
    #     return model_performance_tests, failed_tests, 
    return model_performance_tests, failed_tests

def send_alert_to_slack(message, slack_token, channel_id): 
    client = WebClient(token=slack_token)
    response = client.chat_postMessage(channel=channel_id, text=message)
    assert response["ok"]


def get_evidently_html_and_send_to_slack(evidently_object, slack_token, channel_id, filename,comment) -> None:
    """Generates HTML content from EvidentlyAI and sends it to Slack channel without saving it locally"""

    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        evidently_object.save_html(tmp.name)
        with open(tmp.name, 'r', encoding='utf-8') as fh:
            html_content = fh.read()

    client = WebClient(token=slack_token)
    response = client.files_upload(
        channels=channel_id,
        file=html_content.encode("utf-8"),
        filename=filename,
        filetype="html",
        initial_comment= comment,
    )



def create_report(ref, current, tag, column_mapping):
    print("classification_report")
    classification_report = Report(metrics=[
    metrics.ClassificationQualityMetric(),
    metrics.ConflictTargetMetric(),
    metrics.ConflictPredictionMetric(),
    metrics.DataDriftTable(columns=['target','prediction','text'],cat_stattest = "TVD",cat_stattest_threshold = 0.6,text_stattest= "perc_text_content_drift",text_stattest_threshold=0.5),
    metrics.ClassificationConfusionMatrix(),
    metrics.ClassificationQualityByClass(),
    metrics.ColumnDriftMetric(column_name="target", stattest ="TVD", stattest_threshold = 0.6),
    metrics.DatasetSummaryMetric(),
    ],
    metadata= {"classification report":4},
    tags=["classification",tag,"data"]
    )

    classification_report.run(reference_data= ref[["target","prediction","text"]],
                            current_data= current[['target','prediction','text']], 
                            column_mapping=column_mapping)
    print('classification_report done')
    return classification_report





def add_report_to_project(project_name, ref, current, tag, column_mapping, files_to_process):


    
    # Initialize the S3 client
    s3 = boto3.client(
        's3',
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY
    )
    
    # Initialize workspace
    ws = RemoteWorkspace(workspace)
    # ws = Workspace.create(WORKSPACE)
    project = ws.search_project(project_name)[0]
    
    try:
        # Create and add reports
        report = create_report(ref, current, tag, column_mapping)
        ws.add_report(project.id, report)
        print("classification_report added")


    
    except Exception as e:
        print(f"Error adding report to project: {str(e)}")
        for file in files_to_process:
            send_alert_to_slack(f"With {file} we had the error of:\nError adding reports to project: {str(e)}", slack_token, channel_id)
            update_tag_to_fail(s3, bucket_name, file)


def add_test_to_project(project_name, ref, current, tag, column_mapping, files_to_process):

    
    # Initialize the S3 client
    s3 = boto3.client(
        's3',
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY
    )

    # Initialize workspace
    ws = RemoteWorkspace(workspace)
    # ws = Workspace.create(WORKSPACE)
    project = ws.search_project(project_name)[0]

    slack_token = os.environ.get('SLACK_API_TOKEN')
    channel_id = os.environ.get('SLACK_CHANNEL_ID')

    try:
        # Data quality test
        data_quality, data_quality_failed = data_quality_tests(ref, current, tag, column_mapping)
        ws.add_test_suite(project.id, data_quality)
        print("data quality test added")
        if any(data_quality_failed):
            get_evidently_html_and_send_to_slack(
                data_quality, slack_token, channel_id, filename="data_quality_test.html",
                comment="ALERT " + tag + "\nFailure in Data Quality test: " + str(data_quality_failed)
            )

        # Data drift test
        data_drift, data_drift_failed = data_drift_tests(ref, current, tag, column_mapping)
        ws.add_test_suite(project.id, data_drift)
        print("data drift test added")
        if any(data_drift_failed):
            get_evidently_html_and_send_to_slack(
                data_drift, slack_token, channel_id, filename="data_drift_test.html",
                comment="ALERT " + tag + "\nFailure in Data Drift test: " + str(data_drift_failed)
            )

        # Model performance test
        model_performance, model_performance_failed = model_performance_tests(ref, current, tag, column_mapping)
        ws.add_test_suite(project.id, model_performance)
        print("model performance test added")
        if any(model_performance_failed):
            get_evidently_html_and_send_to_slack(
                model_performance, slack_token, channel_id, filename="model_performance_test.html",
                comment="ALERT " + tag + "\nFailure in Model Performance test: " + str(model_performance_failed)
            )
        

    
    except Exception as e:
        print(f"Error adding tests to project: {str(e)}")
        for file in files_to_process:
            send_alert_to_slack(
                f"With {file} we had the error of:\nError adding tests to project: {str(e)}",
                slack_token, channel_id
            )
            update_tag_to_fail(s3, bucket_name, file)
