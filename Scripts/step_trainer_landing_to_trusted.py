import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Step Trainer Landing
StepTrainerLanding_node1718607143662 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-bucket2024/step_trainer/landing/"], "recurse": True}, transformation_ctx="StepTrainerLanding_node1718607143662")

# Script generated for node Customer Trusted
CustomerTrusted_node1718607141884 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_trusted", transformation_ctx="CustomerTrusted_node1718607141884")

# Script generated for node SQL Query
SqlQuery0 = '''
SELECT STL.*, CT.*
FROM STL INNER JOIN CT
ON STL.serialnumber = CT.serialnumber
'''
SQLQuery_node1718641365439 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"STL":StepTrainerLanding_node1718607143662, "CT":CustomerTrusted_node1718607141884}, transformation_ctx = "SQLQuery_node1718641365439")

# Script generated for node Select Fields
SelectFields_node1718612994805 = SelectFields.apply(frame=SQLQuery_node1718641365439, paths=["serialnumber", "sensorreadingtime", "distancefromobject", "sensorReadingTime", "serialNumber", "distanceFromObject"], transformation_ctx="SelectFields_node1718612994805")

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node1718610839029 = glueContext.write_dynamic_frame.from_options(frame=SelectFields_node1718612994805, connection_type="s3", format="json", connection_options={"path": "s3://stedi-bucket2024/step_trainer/trusted/", "partitionKeys": []}, transformation_ctx="StepTrainerTrusted_node1718610839029")

job.commit()