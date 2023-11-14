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
    from pyspark.sql.functions import desc
    from pyspark.sql.types import FloatType
    from pyspark.sql.functions import col
    from pyspark.sql.functions import when

    userDF = dfc.select(list(dfc.keys())[0]).toDF()

    userCountry = userDF.groupBy("country").count().orderBy(desc("count"))
    userCountry.createOrReplaceTempView("user_country_tb")
    userCountry_new = spark.sql("select * from user_country_tb where count >= 200")

    userCountry_new.createOrReplaceTempView("user_country_new_tb")
    country_gt_200 = spark.sql("select country from user_country_new_tb")
    country_gt_200_list = country_gt_200.rdd.flatMap(lambda x: x).collect()

    userDF_new = userDF.filter(userDF.country.isin(country_gt_200_list)).filter(
        userDF.country != "ID"
    )

    userDF_drop_neg_age = userDF_new.where("age >= 0")
    userDF_drop_neg_age = userDF_drop_neg_age.withColumn(
        "age", col("age").cast(FloatType())
    )
    userDF_mean = userDF_drop_neg_age.groupBy("country").mean("age")

    filled_user_df = userDF_new.join(userDF_mean, "country", "left")
    filled_user_df = filled_user_df.withColumn(
        "age", when(col("age") == -1, col("avg(age)")).otherwise(col("age"))
    )
    filled_user_df = filled_user_df.drop("avg(age)")
    userDF_final = filled_user_df.fillna("n", subset=["gender"])

    dyf0 = DynamicFrame.fromDF(userDF_final, glueContext, "userDF_final")
    return DynamicFrameCollection({"userDF_final": dyf0}, glueContext)


args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Users
Users_node1699776791266 = glueContext.create_dynamic_frame.from_options(
    format_options={
        "quoteChar": '"',
        "withHeader": True,
        "separator": "\t",
        "optimizePerformance": False,
    },
    connection_type="s3",
    format="csv",
    connection_options={
        "paths": ["s3://spotify-user-profile-input/users.tsv"],
        "recurse": True,
    },
    transformation_ctx="Users_node1699776791266",
)

# Script generated for node Custom Transform
CustomTransform_node1699776832036 = MyTransform(
    glueContext,
    DynamicFrameCollection(
        {"Users_node1699776791266": Users_node1699776791266}, glueContext
    ),
)

# Script generated for node Select From Collection
SelectFromCollection_node1699776864485 = SelectFromCollection.apply(
    dfc=CustomTransform_node1699776832036,
    key=list(CustomTransform_node1699776832036.keys())[0],
    transformation_ctx="SelectFromCollection_node1699776864485",
)

# Script generated for node Amazon S3
AmazonS3_node1699776870066 = glueContext.write_dynamic_frame.from_options(
    frame=SelectFromCollection_node1699776864485,
    connection_type="s3",
    format="glueparquet",
    connection_options={
        "path": "s3://spotify-user-profile-intermediate/data/Users-preprocessed/",
        "partitionKeys": [],
    },
    format_options={"compression": "snappy"},
    transformation_ctx="AmazonS3_node1699776870066",
)

job.commit()
