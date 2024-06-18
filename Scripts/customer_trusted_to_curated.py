import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as SqlFuncs

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Accelerometer
Accelerometer_node1718614319067 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_landing", transformation_ctx="Accelerometer_node1718614319067")

# Script generated for node Customer Trusted
CustomerTrusted_node1718614320397 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_trusted", transformation_ctx="CustomerTrusted_node1718614320397")

# Script generated for node Join
Join_node1718614477330 = Join.apply(frame1=CustomerTrusted_node1718614320397, frame2=Accelerometer_node1718614319067, keys1=["email"], keys2=["user"], transformation_ctx="Join_node1718614477330")

# Script generated for node Drop Fields
DropFields_node1718677088729 = DropFields.apply(frame=Join_node1718614477330, paths=["z", "y", "x", "timestamp", "user"], transformation_ctx="DropFields_node1718677088729")

# Script generated for node Drop Duplicates
DropDuplicates_node1718677220682 =  DynamicFrame.fromDF(DropFields_node1718677088729.toDF().dropDuplicates(), glueContext, "DropDuplicates_node1718677220682")

# Script generated for node Customer Curated
CustomerCurated_node1718614522741 = glueContext.write_dynamic_frame.from_options(frame=DropDuplicates_node1718677220682, connection_type="s3", format="json", connection_options={"path": "s3://stedi-bucket2024/customer/curated/", "partitionKeys": []}, transformation_ctx="CustomerCurated_node1718614522741")

job.commit()