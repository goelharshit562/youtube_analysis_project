import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

# Get job arguments
args = getResolvedOptions(sys.argv, ["JOB_NAME"])

# Initialize Spark context
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Initialize Glue job
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Define a SQL predicate for filtering data
predicate_pushdown = "region in ('ca','gb','us')"

# Create a dynamic frame from a Glue catalog table with a push-down predicate
S3Sourcebucket_node1 = glueContext.create_dynamic_frame.from_catalog(
    database="de_youtube_raw",
    table_name="raw_statistics",
    transformation_ctx="S3Sourcebucket_node1",
    push_down_predicate=predicate_pushdown,
)

# Apply mapping to change the schema of the dynamic frame
ChangeSchema_node1696397402623 = ApplyMapping.apply(
    frame=S3Sourcebucket_node1,
    mappings=[
        ("video_id", "string", "video_id", "string"),
        ("trending_date", "string", "trending_date", "string"),
        ("title", "string", "title", "string"),
        ("channel_title", "string", "channel_title", "string"),
        ("category_id", "long", "category_id", "bigint"),
        ("publish_time", "string", "publish_time", "string"),
        ("tags", "string", "tags", "string"),
        ("views", "long", "views", "bigint"),
        ("likes", "long", "likes", "bigint"),
        ("dislikes", "long", "dislikes", "bigint"),
        ("comment_count", "long", "comment_count", "bigint"),
        ("thumbnail_link", "string", "thumbnail_link", "string"),
        ("comments_disabled", "boolean", "comments_disabled", "boolean"),
        ("ratings_disabled", "boolean", "ratings_disabled", "boolean"),
        ("video_error_or_removed", "boolean", "video_error_or_removed", "boolean"),
        ("description", "string", "description", "string"),
        ("region", "string", "region", "string"),
    ],
    transformation_ctx="ChangeSchema_node1696397402623",
)

# Coalesce the resulting data frame into a single partition
datasink1 = ChangeSchema_node1696397402623.toDF().coalesce(1)

# Convert the data frame back to a dynamic frame
df_final_output = DynamicFrame.fromDF(datasink1, glueContext, "df_final_output")

# Write the dynamic frame to an S3 bucket in Parquet format
S3Targetbucket_node2 = glueContext.write_dynamic_frame.from_options(
    frame=df_final_output,
    connection_type="s3",
    format="parquet",
    connection_options={
        "path": "s3://vedant-de-on-youtube-cleaned-dev/Youtube/raw_statistics/",
        "partitionKeys": ["region"],
    },
    format_options={"compression": "snappy"},
    transformation_ctx="S3Targetbucket_node2",
)

# Commit the Glue job
job.commit()
