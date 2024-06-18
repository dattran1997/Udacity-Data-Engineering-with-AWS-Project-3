import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import re

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Customer Landing
CustomerLanding_node1718594729182 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": True}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-bucket2024/customer/landing/"], "recurse": True}, transformation_ctx="CustomerLanding_node1718594729182")

# Script generated for node PrivacyFilter
PrivacyFilter_node1718594736325 = Filter.apply(frame=CustomerLanding_node1718594729182, f=lambda row: (not(row["shareWithResearchAsOfDate"] == 0)), transformation_ctx="PrivacyFilter_node1718594736325")

# Script generated for node Amazon S3
AmazonS3_node1718594851431 = glueContext.getSink(path="s3://stedi-bucket2024/customer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1718594851431")
AmazonS3_node1718594851431.setCatalogInfo(catalogDatabase="stedi",catalogTableName="customer_trusted")
AmazonS3_node1718594851431.setFormat("json")
AmazonS3_node1718594851431.writeFrame(PrivacyFilter_node1718594736325)
job.commit()