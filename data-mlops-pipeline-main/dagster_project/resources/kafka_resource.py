from typing import List
from dagster import ConfigurableResource, DagsterInvariantViolationError
from confluent_kafka import Consumer, Producer, KafkaException
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

class KafkaResource(ConfigurableResource):

    bootstrap_server: str = os.getenv('KAFKA_BOOTSTRAP_SERVER')
    topic_name: str = os.getenv('KAFKA_TOPIC_NAME')
    group_id: str = os.getenv('KAFKA_GROUP_ID')
    auto_offset_reset: str = "earliest"
    fetch_min_bytes: int = 4000

    def get_consumer(self):
        consumer_config = {
            'bootstrap.servers': self.bootstrap_server,
            'group.id': self.group_id,
            'auto.offset.reset': self.auto_offset_reset,
            'fetch.min.bytes': self.fetch_min_bytes
        }
        try:
            consumer = Consumer(consumer_config)
            consumer.subscribe([self.topic_name])
            return consumer
        except KafkaException as e:
            raise DagsterInvariantViolationError(f"Failed to create Kafka consumer: {e}")
        except Exception as e:
            raise DagsterInvariantViolationError(f"An unexpected error occurred: {e}")

    def get_producer(self):
        producer_config = {
            'bootstrap.servers': self.bootstrap_server
        }
        try:
            producer = Producer(producer_config)
            return producer
        except KafkaException as e:
            raise DagsterInvariantViolationError(f"Failed to create Kafka producer: {e}")
        except Exception as e:
            raise DagsterInvariantViolationError(f"An unexpected error occurred: {e}")
