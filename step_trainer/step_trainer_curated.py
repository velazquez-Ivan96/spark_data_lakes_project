import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Amazon S3
AmazonS3_node1678560689521 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi_process",
    table_name="accelerometer_trusted",
    transformation_ctx="AmazonS3_node1678560689521",
)

# Script generated for node S3 bucket
S3bucket_node1 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi_process",
    table_name="step_trainer_landing",
    transformation_ctx="S3bucket_node1",
)

# Script generated for node Amazon S3
AmazonS3_node1678560687018 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi_process",
    table_name="customer_curated",
    transformation_ctx="AmazonS3_node1678560687018",
)

# Script generated for node Join 1
Join1_node2 = Join.apply(
    frame1=S3bucket_node1,
    frame2=AmazonS3_node1678560687018,
    keys1=["serialnumber"],
    keys2=["serialnumber"],
    transformation_ctx="Join1_node2",
)

# Script generated for node Join 2
Join2_node1678560886926 = Join.apply(
    frame1=Join1_node2,
    frame2=AmazonS3_node1678560689521,
    keys1=["email"],
    keys2=["user"],
    transformation_ctx="Join2_node1678560886926",
)

# Script generated for node S3 bucket
S3bucket_node3 = glueContext.write_dynamic_frame.from_options(
    frame=Join2_node1678560886926,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://stedi-lake-house-4512/step_trainer/curated/",
        "partitionKeys": [],
    },
    transformation_ctx="S3bucket_node3",
)

job.commit()
