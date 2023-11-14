import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrameCollection
from awsglue.dynamicframe import DynamicFrame


# Script generated for node Custom Transform
def MyTransform(glueContext, dfc) -> DynamicFrameCollection:
    from pyspark.ml.linalg import Vectors, VectorUDT
    from pyspark.sql.functions import col, lit, array, udf
    from pyspark.sql.types import ArrayType, FloatType

    convert_to_array = udf(
        lambda vector: vector.toArray().tolist(), ArrayType(FloatType())
    )

    user_profileDF = dfc.select(list(dfc.keys())[0]).toDF()

    concat = udf(lambda v, e: Vectors.dense(list(v) + list(e)), VectorUDT())
    user_profileDF = user_profileDF.select(
        "user_id",
        "ctr",
        concat(
            user_profileDF.user_features_scaled, user_profileDF.country_features_scaled
        ).alias("user_profile"),
    )
    user_profileDF = user_profileDF.withColumn(
        "user_profile", convert_to_array(col("user_profile"))
    )

    dyf0 = DynamicFrame.fromDF(user_profileDF, glueContext, "user_profileDF")
    return DynamicFrameCollection({"user_profileDF": dyf0}, glueContext)


args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Hofstede WHR
HofstedeWHR_node1699966249112 = glueContext.create_dynamic_frame.from_options(
    format_options={},
    connection_type="s3",
    format="parquet",
    connection_options={
        "paths": ["s3://spotify-user-profile-intermediate/data/Hofstede-WHR/"],
        "recurse": True,
    },
    transformation_ctx="HofstedeWHR_node1699966249112",
)

# Script generated for node User-EventAcoustic
UserEventAcoustic_node1699966247194 = glueContext.create_dynamic_frame.from_options(
    format_options={},
    connection_type="s3",
    format="parquet",
    connection_options={
        "paths": ["s3://spotify-user-profile-intermediate/data/Users-EventAcoustic/"],
        "recurse": True,
    },
    transformation_ctx="UserEventAcoustic_node1699966247194",
)

# Script generated for node Join
Join_node1699966336585 = Join.apply(
    frame1=UserEventAcoustic_node1699966247194,
    frame2=HofstedeWHR_node1699966249112,
    keys1=["country"],
    keys2=["ctr"],
    transformation_ctx="Join_node1699966336585",
)

# Script generated for node Custom Transform
CustomTransform_node1699966458596 = MyTransform(
    glueContext,
    DynamicFrameCollection(
        {"Join_node1699966336585": Join_node1699966336585}, glueContext
    ),
)

# Script generated for node Select From Collection
SelectFromCollection_node1699966636291 = SelectFromCollection.apply(
    dfc=CustomTransform_node1699966458596,
    key=list(CustomTransform_node1699966458596.keys())[0],
    transformation_ctx="SelectFromCollection_node1699966636291",
)

# Script generated for node Amazon S3
AmazonS3_node1699966642568 = glueContext.write_dynamic_frame.from_options(
    frame=SelectFromCollection_node1699966636291,
    connection_type="s3",
    format="glueparquet",
    connection_options={
        "path": "s3://spotify-user-profile-output/user-profile/",
        "partitionKeys": [],
    },
    format_options={"compression": "snappy"},
    transformation_ctx="AmazonS3_node1699966642568",
)

job.commit()
