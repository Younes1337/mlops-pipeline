
from dagster import (  # type: ignore
                resource, 
                Field, 
                String, 
                EnvVar
    ) 
from confluent_kafka import Consumer # type: ignore
from slack_sdk import WebClient  # type: ignore
import boto3  # type: ignore


########################################################################### ENV Vars ######################################################################


############################################################################################################################################################

# @resource(config_schema={
#     "bootstrap_servers": Field(String, description="Kafka broker addresses", default_value="kafka:9092"),
#     "group_id": Field(String, description="Consumer group ID", default_value="my_consumer_group"),
#     "topic_name": Field(String, description="Kafka topic name", default_value="test_topic")
# })
# def kafka_consumer_resource(context):
#     bootstrap_servers = os.getenv("KAFKA_BROKER_HOST")
#     group_id = context.resource_config["group_id"]
#     topic_name = context.resource_config["topic_name"]

#     consumer = Consumer({
#       'bootstrap.servers': bootstrap_servers,
#       'group.id': group_id,
#       'topic_name': topic_name,
#       'auto.offset.reset': 'earliest'  
#   })

#     return consumer


# from dagster import resource, Field, String



@resource(config_schema={
    "bootstrap_servers": Field(String, description="Kafka broker addresses", default_value="kafka:9092"),
    "group_id": Field(String, description="Consumer group ID", default_value="my_consumer_group"),
    "topic_name": Field(String, description="Kafka topic name", default_value="test_topic")
})

def kafka_consumer_resource(context):
    bootstrap_servers = context.resource_config["bootstrap_servers"]
    group_id = context.resource_config["group_id"]
    topic_name = context.resource_config["topic_name"]

    consumer = Consumer({
      'bootstrap.servers': bootstrap_servers,
      'group.id': group_id,
      'auto.offset.reset': 'earliest'  
    })

    consumer.subscribe([topic_name])


    return consumer


@resource(
    config_schema={
        "slack_token": Field(String, description="Slack Connection Token", default_value=str(SLACK_API_TOKEN)),
        "channel_id": Field(String, description="Channel ID", default_value=str(SLACK_CHANNEL_ID)),
    }
)

def slack_resource(context):
     """This Dagster resource initializes a Slack client."""
     slack_token = context.resource_config['slack_token']
     channel_id = context.resource_config['channel_id']
    #  slack_client = WebClient(toekn=slack_token)

     return slack_token, channel_id



@resource(
    config_schema={
        "AWS_ACCESS_KEY_ID": Field(String, description="AWS Access Key ID", default_value=str(AWS_ACCESS_KEY_ID)),
        "AWS_SECRET_ACCESS_KEY": Field(String, description="AWS Secret Access Key", default_value=str(AWS_SECRET_ACCESS_KEY)),
        "BUCKET_NAME": Field(String, description="S3 Bucket Name", default_value=str(S3_BUCKET_NAME)),
    }
)
def s3_resource(context):
    """This is a resource that initializes an S3 client."""
    aws_access_key_id = context.resource_config["AWS_ACCESS_KEY_ID"]
    aws_secret_access_key = context.resource_config["AWS_SECRET_ACCESS_KEY"]
    bucket_name = context.resource_config["BUCKET_NAME"]

    s3_client = boto3.client(
        "s3",
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key
    )

    return s3_client, bucket_name
