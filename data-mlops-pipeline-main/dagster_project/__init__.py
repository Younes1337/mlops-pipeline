from dagster import (
    AssetSelection,
    Definitions,
    load_assets_from_modules,
    RepositoryDefinition,
    ScheduleDefinition, 
    define_asset_job
    
)
from .resources.resources import kafka_consumer_resource, slack_resource, s3_resource
from .ops.evedently_dag import evedently_job


evedently_scheduler = ScheduleDefinition(
    job=evedently_job, 
    cron_schedule="*/2 * * * * "
)

defs = Definitions(  
    jobs=[
        evedently_job, 
        ],
    resources={
        "kafka_consumer": kafka_consumer_resource,
        "slack_resource": slack_resource,
        "s3_resource": s3_resource
        }, 
    schedules=[evedently_scheduler]
)


