import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from awsglue import DynamicFrame
from pyspark.sql import functions as SqlFuncs

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

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1718614319067 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_trusted", transformation_ctx="AccelerometerTrusted_node1718614319067")

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node1718678812991 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="step_trainer_trusted", transformation_ctx="StepTrainerTrusted_node1718678812991")

# Script generated for node SQL Query
SqlQuery0 = '''
SELECT ACC.*, ST.*
FROM ACC INNER JOIN ST
ON ACC.timestamp = ST.sensorReadingTime;
'''
SQLQuery_node1718679290110 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"ACC":AccelerometerTrusted_node1718614319067, "ST":StepTrainerTrusted_node1718678812991}, transformation_ctx = "SQLQuery_node1718679290110")

# Script generated for node Drop Duplicates
DropDuplicates_node1718677220682 =  DynamicFrame.fromDF(SQLQuery_node1718679290110.toDF().dropDuplicates(), glueContext, "DropDuplicates_node1718677220682")

# Script generated for node Machine Learning Curated
MachineLearningCurated_node1718614522741 = glueContext.write_dynamic_frame.from_options(frame=DropDuplicates_node1718677220682, connection_type="s3", format="json", connection_options={"path": "s3://stedi-bucket2024/step_trainer/curated/", "partitionKeys": []}, transformation_ctx="MachineLearningCurated_node1718614522741")

job.commit()