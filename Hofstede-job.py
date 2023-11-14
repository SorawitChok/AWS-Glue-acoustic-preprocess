import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrameCollection
from awsglue.dynamicframe import DynamicFrame


# Script generated for node Custom Transform hof country name
def MyTransform(glueContext, dfc) -> DynamicFrameCollection:
    from pyspark.sql.functions import when

    hofstedeDF = dfc.select(list(dfc.keys())[0]).toDF()

    hofstedeDF = hofstedeDF.withColumn(
        "country",
        when(hofstedeDF.country == "Czech Rep", "Czech Republic")
        .when(hofstedeDF.country == "U.S.A.", "United States")
        .when(hofstedeDF.country == "Great Britain", "United Kingdom")
        .otherwise(hofstedeDF.country),
    )
    dyf0 = DynamicFrame.fromDF(hofstedeDF, glueContext, "hofstedeDF")
    return DynamicFrameCollection({"hofstedeDF": dyf0}, glueContext)


args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Hofstede
Hofstede_node1699696711654 = glueContext.create_dynamic_frame.from_options(
    format_options={
        "quoteChar": '"',
        "withHeader": True,
        "separator": "\t",
        "optimizePerformance": False,
    },
    connection_type="s3",
    format="csv",
    connection_options={
        "paths": ["s3://spotify-user-profile-input/hofstede.tsv"],
        "recurse": True,
    },
    transformation_ctx="Hofstede_node1699696711654",
)

# Script generated for node Custom Transform hof country name
CustomTransformhofcountryname_node1699696858571 = MyTransform(
    glueContext,
    DynamicFrameCollection(
        {"Hofstede_node1699696711654": Hofstede_node1699696711654}, glueContext
    ),
)

# Script generated for node Select From Collection
SelectFromCollection_node1699776330237 = SelectFromCollection.apply(
    dfc=CustomTransformhofcountryname_node1699696858571,
    key=list(CustomTransformhofcountryname_node1699696858571.keys())[0],
    transformation_ctx="SelectFromCollection_node1699776330237",
)

# Script generated for node Hofstede preprocessed
Hofstedepreprocessed_node1699776338666 = glueContext.write_dynamic_frame.from_options(
    frame=SelectFromCollection_node1699776330237,
    connection_type="s3",
    format="glueparquet",
    connection_options={
        "path": "s3://spotify-user-profile-intermediate/data/hofstede-preprocessed/",
        "partitionKeys": [],
    },
    format_options={"compression": "snappy"},
    transformation_ctx="Hofstedepreprocessed_node1699776338666",
)

job.commit()
