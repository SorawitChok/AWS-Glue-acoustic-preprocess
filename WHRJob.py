import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrameCollection
from awsglue.dynamicframe import DynamicFrame


# Script generated for node Custom Transform WHR
def MyTransform(glueContext, dfc) -> DynamicFrameCollection:
    worldHappinessDF = dfc.select(list(dfc.keys())[0]).toDF()

    ori_columns_name = worldHappinessDF.columns
    new_columns_name = list()
    for col in ori_columns_name:
        name = col.lower()
        new_col = name.replace(" ", "_")
        new_col = new_col.replace("(", "")
        new_col = new_col.replace(")", "")
        new_col = new_col.replace(",", "")
        new_columns_name.append(new_col)

    worldHappinessDF = worldHappinessDF.toDF(*new_columns_name)
    worldHappinessDF = worldHappinessDF.filter(worldHappinessDF.year == 2016)
    dyf0 = DynamicFrame.fromDF(worldHappinessDF, glueContext, "worldHappinessDF")
    return DynamicFrameCollection({"worldHappinessDF": dyf0}, glueContext)


args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node WHR
WHR_node1699776520707 = glueContext.create_dynamic_frame.from_options(
    format_options={
        "quoteChar": '"',
        "withHeader": True,
        "separator": "\t",
        "optimizePerformance": False,
    },
    connection_type="s3",
    format="csv",
    connection_options={
        "paths": ["s3://spotify-user-profile-input/world_happiness_report_2018.tsv"],
        "recurse": True,
    },
    transformation_ctx="WHR_node1699776520707",
)

# Script generated for node Custom Transform WHR
CustomTransformWHR_node1699776562734 = MyTransform(
    glueContext,
    DynamicFrameCollection(
        {"WHR_node1699776520707": WHR_node1699776520707}, glueContext
    ),
)

# Script generated for node Select From Collection
SelectFromCollection_node1699776617270 = SelectFromCollection.apply(
    dfc=CustomTransformWHR_node1699776562734,
    key=list(CustomTransformWHR_node1699776562734.keys())[0],
    transformation_ctx="SelectFromCollection_node1699776617270",
)

# Script generated for node Amazon S3
AmazonS3_node1699776628634 = glueContext.write_dynamic_frame.from_options(
    frame=SelectFromCollection_node1699776617270,
    connection_type="s3",
    format="glueparquet",
    connection_options={
        "path": "s3://spotify-user-profile-intermediate/data/WHR-preprocessed/",
        "partitionKeys": [],
    },
    format_options={"compression": "snappy"},
    transformation_ctx="AmazonS3_node1699776628634",
)

job.commit()
